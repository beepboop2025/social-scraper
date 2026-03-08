"""Base scraper class — all scrapers inherit from this."""

import asyncio
import hashlib
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from models import Platform, ScrapedContent, ScrapedItem

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
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self.burst,
                self._tokens + elapsed * (self.max_per_minute / 60.0),
            )
            self._last_refill = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / (self.max_per_minute / 60.0)
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class BaseScraper(ABC):
    """Abstract base class for all scrapers.

    Provides:
    - Consistent ScrapedContent output
    - Rate limiting
    - Error handling with retry
    - Deterministic content IDs
    - Metrics tracking
    """

    platform: Platform
    name: str = "base"

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
            "last_scrape_at": None,
            "uptime_start": datetime.now(timezone.utc),
        }

    @abstractmethod
    async def scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape content matching the query. Must be implemented by subclasses."""

    @abstractmethod
    async def scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape a specific channel/feed/subreddit. Must be implemented by subclasses."""

    async def safe_scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape with retry logic and rate limiting."""
        for attempt in range(1, self.max_retries + 1):
            try:
                await self.rate_limiter.acquire()
                items = await self.scrape(query, limit)
                self._stats["total_scraped"] += len(items)
                self._stats["last_scrape_at"] = datetime.now(timezone.utc).isoformat()
                return items
            except Exception as e:
                self._stats["total_errors"] += 1
                logger.warning(
                    f"[{self.name}] Attempt {attempt}/{self.max_retries} failed: {e}"
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
        return []

    async def safe_scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape channel with retry logic."""
        for attempt in range(1, self.max_retries + 1):
            try:
                await self.rate_limiter.acquire()
                items = await self.scrape_channel(channel_id, limit)
                self._stats["total_scraped"] += len(items)
                self._stats["last_scrape_at"] = datetime.now(timezone.utc).isoformat()
                return items
            except Exception as e:
                self._stats["total_errors"] += 1
                logger.warning(
                    f"[{self.name}] Channel scrape attempt {attempt}/{self.max_retries} failed: {e}"
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
        return []

    @staticmethod
    def make_id(platform: str, *parts: str) -> str:
        """Generate a deterministic content ID."""
        raw = f"{platform}:" + ":".join(str(p) for p in parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @property
    def stats(self) -> dict:
        return {**self._stats, "platform": self.platform.value, "name": self.name}

    async def health_check(self) -> dict:
        """Check if the scraper is operational."""
        return {
            "platform": self.platform.value,
            "name": self.name,
            "status": "ok",
            "stats": self.stats,
        }
