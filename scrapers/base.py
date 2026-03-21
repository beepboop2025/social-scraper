"""Base scraper class — all scrapers inherit from this."""

import asyncio
import hashlib
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from models import Platform, ScrapedContent, ScrapedItem
from core.http_pool import get_http_client

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token-bucket rate limiter for scraper requests."""

    def __init__(self, max_per_minute: int = 30, burst: int = 5):
        self.max_per_minute = max_per_minute
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            wait = 0.0
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.burst,
                    self._tokens + elapsed * (self.max_per_minute / 60.0),
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                else:
                    wait = (1.0 - self._tokens) / (self.max_per_minute / 60.0)

            # Sleep outside the lock, then retry
            await asyncio.sleep(wait)


class BaseScraper(ABC):
    """Abstract base class for all scrapers.

    Provides:
    - Consistent ScrapedContent output
    - Rate limiting
    - Error handling with retry
    - Deterministic content IDs
    - Metrics tracking
    - Self-healing: detects schema changes, auto-disables after repeated failures
    """

    platform: Platform
    name: str = "base"

    # Schema change detection
    SCHEMA_ERROR_THRESHOLD = 10  # Auto-disable after N consecutive schema errors

    def __init__(
        self,
        rate_limit: int = 30,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.rate_limiter = RateLimiter(max_per_minute=rate_limit)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._stats = {
            "total_scraped": 0,
            "total_errors": 0,
            "schema_errors": 0,
            "last_scrape_at": None,
            "uptime_start": datetime.now(timezone.utc),
        }
        self._consecutive_schema_errors = 0
        self._disabled = False
        self._parser_version = self._compute_parser_version()

    async def get_client(self):
        """Get a shared HTTP client from the connection pool.

        Scrapers should use this instead of creating their own httpx.AsyncClient.
        """
        return await get_http_client()

    @abstractmethod
    async def scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape content matching the query. Must be implemented by subclasses."""

    @abstractmethod
    async def scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape a specific channel/feed/subreddit. Must be implemented by subclasses."""

    async def safe_scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape with retry logic, rate limiting, and self-healing."""
        if self._disabled:
            logger.info(f"[{self.name}] Scraper disabled due to repeated schema errors")
            return []

        for attempt in range(1, self.max_retries + 1):
            try:
                await self.rate_limiter.acquire()
                items = await self.scrape(query, limit)

                # Schema change detection: if we got 0 items but expected some,
                # try fallback extraction
                if not items and attempt == 1:
                    items = await self._try_fallback_extraction(query, limit)

                self._stats["total_scraped"] += len(items)
                self._stats["last_scrape_at"] = datetime.now(timezone.utc).isoformat()
                self._consecutive_schema_errors = 0  # Reset on success
                return items
            except (KeyError, AttributeError, TypeError) as e:
                # These suggest schema/structure changes
                self._handle_schema_error(e, query)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
            except Exception as e:
                self._stats["total_errors"] += 1
                logger.warning(
                    "Scrape failed",
                    extra={
                        "source": self.name,
                        "query": query,
                        "error_type": type(e).__name__,
                        "error": str(e),
                        "retry": attempt,
                        "max_retries": self.max_retries,
                    },
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
        return []

    async def safe_scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape channel with retry logic and self-healing."""
        if self._disabled:
            logger.info(f"[{self.name}] Scraper disabled due to repeated schema errors")
            return []

        for attempt in range(1, self.max_retries + 1):
            try:
                await self.rate_limiter.acquire()
                items = await self.scrape_channel(channel_id, limit)
                self._stats["total_scraped"] += len(items)
                self._stats["last_scrape_at"] = datetime.now(timezone.utc).isoformat()
                self._consecutive_schema_errors = 0
                return items
            except (KeyError, AttributeError, TypeError) as e:
                self._handle_schema_error(e, channel_id)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
            except Exception as e:
                self._stats["total_errors"] += 1
                logger.warning(
                    "Channel scrape failed",
                    extra={
                        "source": self.name,
                        "channel_id": channel_id,
                        "error_type": type(e).__name__,
                        "error": str(e),
                        "retry": attempt,
                        "max_retries": self.max_retries,
                    },
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
        return []

    # ── Self-healing methods ────────────────────────────────

    def _compute_parser_version(self) -> str:
        """Hash the scraper's parsing logic for version tracking."""
        import inspect
        try:
            source = inspect.getsource(self.__class__)
            return hashlib.sha256(source.encode()).hexdigest()[:12]
        except Exception:
            return "unknown"

    def _handle_schema_error(self, error: Exception, context: str):
        """Handle a schema/structure change detection."""
        self._consecutive_schema_errors += 1
        self._stats["schema_errors"] = self._stats.get("schema_errors", 0) + 1
        self._stats["total_errors"] += 1

        logger.warning(
            "SCHEMA_CHANGED",
            extra={
                "source": self.name,
                "context": context,
                "error_type": type(error).__name__,
                "error": str(error),
                "consecutive_schema_errors": self._consecutive_schema_errors,
                "parser_version": self._parser_version,
            },
        )

        # Auto-disable after threshold
        if self._consecutive_schema_errors >= self.SCHEMA_ERROR_THRESHOLD:
            self._disabled = True
            logger.error(
                f"[{self.name}] AUTO-DISABLED after {self._consecutive_schema_errors} "
                f"consecutive schema errors. Manual intervention required."
            )
            # Fire webhook notification
            try:
                from core.webhooks import fire_event
                fire_event("source_down", {
                    "source": self.name,
                    "reason": "schema_change_auto_disabled",
                    "consecutive_errors": self._consecutive_schema_errors,
                    "last_error": str(error),
                    "parser_version": self._parser_version,
                })
            except Exception:
                pass

    async def _try_fallback_extraction(self, query: str, limit: int) -> list[ScrapedItem]:
        """Attempt generic article extraction using trafilatura as fallback."""
        try:
            import trafilatura
            logger.info(f"[{self.name}] Attempting trafilatura fallback extraction")
            # Subclasses can override this for platform-specific fallback
            return []
        except ImportError:
            return []

    def reset_disabled(self):
        """Manually re-enable a disabled scraper."""
        self._disabled = False
        self._consecutive_schema_errors = 0
        logger.info(f"[{self.name}] Scraper manually re-enabled")

    @staticmethod
    def make_id(platform: str, *parts: str) -> str:
        """Generate a deterministic content ID."""
        raw = f"{platform}:" + ":".join(str(p) for p in parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @property
    def stats(self) -> dict:
        return {
            **self._stats,
            "platform": self.platform.value,
            "name": self.name,
            "parser_version": self._parser_version,
            "disabled": self._disabled,
            "consecutive_schema_errors": self._consecutive_schema_errors,
        }

    async def health_check(self) -> dict:
        """Check if the scraper is operational."""
        return {
            "platform": self.platform.value,
            "name": self.name,
            "status": "disabled" if self._disabled else "ok",
            "stats": self.stats,
        }
