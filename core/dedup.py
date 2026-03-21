"""URL deduplication using Redis sets with sliding TTL window.

Prevents the same content from being processed multiple times when it
appears across different sources (e.g., the same article from RSS + Twitter + Reddit).

Usage:
    from core.dedup import URLDeduplicator

    dedup = URLDeduplicator()
    if await dedup.is_seen(url):
        skip  # Already processed
    else:
        await dedup.mark_seen(url)
        process(url)
"""

import hashlib
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 86400  # 24 hours


class URLDeduplicator:
    """Redis-backed URL deduplication with sliding TTL window.

    Each URL is hashed (SHA-256) and stored as a Redis key with TTL.
    After TTL expires, the URL can be re-scraped (useful for updated content).
    Falls back to an in-memory set if Redis is unavailable.
    """

    REDIS_PREFIX = "dedup:url:"

    def __init__(
        self,
        redis_url: Optional[str] = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.ttl_seconds = ttl_seconds
        self._redis = None
        self._fallback_set: set[str] = set()
        self._fallback_max = 50000  # Cap in-memory fallback

    @staticmethod
    def _hash_url(url: str) -> str:
        """Create a compact hash of the URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]

    async def _get_redis(self):
        """Lazy async Redis connection."""
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
                await self._redis.ping()
            except Exception as e:
                logger.debug(f"[Dedup] Redis unavailable, using in-memory fallback: {e}")
                self._redis = None
        return self._redis

    async def is_seen(self, url: str) -> bool:
        """Check if a URL has been seen within the TTL window.

        Returns True if the URL was already processed recently.
        """
        if not url:
            return False

        url_hash = self._hash_url(url)
        r = await self._get_redis()

        if r is not None:
            try:
                return await r.exists(f"{self.REDIS_PREFIX}{url_hash}") > 0
            except Exception as e:
                logger.debug(f"[Dedup] Redis check failed: {e}")

        # In-memory fallback
        return url_hash in self._fallback_set

    async def mark_seen(self, url: str):
        """Mark a URL as seen with TTL expiry."""
        if not url:
            return

        url_hash = self._hash_url(url)
        r = await self._get_redis()

        if r is not None:
            try:
                await r.set(
                    f"{self.REDIS_PREFIX}{url_hash}",
                    "1",
                    ex=self.ttl_seconds,
                )
                return
            except Exception as e:
                logger.debug(f"[Dedup] Redis mark failed: {e}")

        # In-memory fallback
        if len(self._fallback_set) >= self._fallback_max:
            # Evict half the set to prevent unbounded growth
            to_keep = list(self._fallback_set)[self._fallback_max // 2:]
            self._fallback_set = set(to_keep)
        self._fallback_set.add(url_hash)

    async def is_seen_batch(self, urls: list[str]) -> dict[str, bool]:
        """Check multiple URLs at once. Returns {url: is_seen}."""
        results = {}
        r = await self._get_redis()

        if r is not None:
            try:
                pipe = r.pipeline()
                hashes = {}
                for url in urls:
                    if url:
                        h = self._hash_url(url)
                        hashes[url] = h
                        pipe.exists(f"{self.REDIS_PREFIX}{h}")
                    else:
                        results[url] = False

                pipe_results = await pipe.execute()
                for url, exists in zip(hashes.keys(), pipe_results):
                    results[url] = bool(exists)
                return results
            except Exception as e:
                logger.debug(f"[Dedup] Redis batch check failed: {e}")

        # In-memory fallback
        for url in urls:
            if url:
                results[url] = self._hash_url(url) in self._fallback_set
            else:
                results[url] = False
        return results

    async def close(self):
        """Close Redis connection."""
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None
