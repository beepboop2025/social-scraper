"""DragonScope connector — pushes social intelligence into DragonScope's data pipeline.

DragonScope uses:
- Redis cache (market:{category} keys) for real-time data
- Redis pub/sub (market:updates channel) for WebSocket broadcasting
- TimescaleDB (market_ticks, snapshot_logs) for persistence
- Kafka (optional) for async processing

This connector maps scraped social data into DragonScope's existing categories:
- reddit_posts → sentiment workspace
- news → cross-market workspace
- github_repos → research workspace
- Social sentiment signals → ML analytics
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import Platform, ScrapedItem

logger = logging.getLogger(__name__)

# Map scraper platforms to DragonScope categories
PLATFORM_TO_CATEGORY = {
    Platform.REDDIT: "reddit_posts",
    Platform.TWITTER: "reddit_posts",  # Social sentiment same category
    Platform.TELEGRAM: "news",
    Platform.YOUTUBE: "news",
    Platform.HACKERNEWS: "news",
    Platform.RSS: "news",
    Platform.WEB: "news",
    Platform.MASTODON: "reddit_posts",
    Platform.GITHUB: "github_repos",
    Platform.SEC_EDGAR: "sec_filings",
    Platform.CENTRAL_BANK: "news",
    Platform.DARKWEB: "news",
    Platform.DISCORD: "reddit_posts",
}


class DragonScopeConnector:
    """Push scraped data into DragonScope's pipeline.

    Supports two modes:
    1. Direct Redis: Write to DragonScope's Redis cache + publish to market:updates
    2. API proxy: POST to DragonScope's FastAPI backend at /api/data/{category}

    Mode 1 is preferred for real-time data (bypasses HTTP overhead).
    Mode 2 is the fallback when Redis isn't directly accessible.
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.redis_url = redis_url or "redis://localhost:6379"
        self.api_url = api_url or "http://localhost:3456"
        self.api_key = api_key
        self._redis = None
        self._http = httpx.AsyncClient(timeout=15)

    async def _get_redis(self):
        """Lazy Redis connection."""
        if self._redis is None:
            conn = None
            try:
                import redis.asyncio as aioredis
                conn = aioredis.from_url(self.redis_url, decode_responses=True)
                await conn.ping()
                self._redis = conn
            except Exception as e:
                logger.warning(f"[DragonScope] Redis connection failed: {e}")
                if conn is not None:
                    try:
                        await conn.close()
                    except Exception:
                        pass
                self._redis = None
        return self._redis

    def _transform_for_dragonscope(self, items: list[ScrapedItem], category: str) -> dict:
        """Transform scraped items into DragonScope's expected format."""
        if category == "reddit_posts":
            return {
                "posts": [
                    {
                        "title": item.unified.raw_metadata.get("title", item.unified.text[:100]),
                        "body": item.unified.text,
                        "author": item.unified.author.username or item.unified.author.display_name or "unknown",
                        "subreddit": item.unified.source_channel or item.unified.platform.value,
                        "score": item.unified.engagement.likes,
                        "num_comments": item.unified.engagement.replies,
                        "url": item.unified.source_url,
                        "created_utc": item.unified.created_at.timestamp(),
                        "platform": item.unified.platform.value,
                        "sentiment": item.unified.raw_metadata.get("sentiment"),
                    }
                    for item in items
                ],
                "scrape_source": "social_scraper",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        elif category == "news":
            return {
                "articles": [
                    {
                        "title": item.unified.raw_metadata.get("title", item.unified.text[:100]),
                        "description": item.unified.text[:500],
                        "content": item.unified.text,
                        "author": item.unified.author.display_name or item.unified.author.username or "unknown",
                        "source": item.unified.source_channel or item.unified.platform.value,
                        "url": item.unified.source_url,
                        "publishedAt": item.unified.created_at.isoformat(),
                        "platform": item.unified.platform.value,
                        "category": item.unified.category or "general",
                    }
                    for item in items
                ],
                "scrape_source": "social_scraper",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        elif category == "github_repos":
            return {
                "repos": [
                    {
                        "name": item.unified.source_channel or item.unified.author.username or item.unified.author.display_name,
                        "full_name": item.unified.source_channel,
                        "description": item.unified.text[:300],
                        "stars": item.unified.engagement.likes,
                        "forks": item.unified.engagement.reposts,
                        "watchers": item.unified.engagement.views or 0,
                        "url": item.unified.source_url,
                        "language": item.unified.raw_metadata.get("language"),
                        "topics": item.unified.hashtags,
                        "content_type": item.unified.content_type.value,
                    }
                    for item in items
                ],
                "scrape_source": "social_scraper",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        elif category == "sec_filings":
            return {
                "filings": [
                    {
                        "form_type": item.unified.raw_metadata.get("form_type", ""),
                        "company": item.unified.author.display_name,
                        "cik": item.unified.author.username,
                        "tickers": item.unified.hashtags,
                        "title": item.unified.raw_metadata.get("title", item.unified.text[:100]),
                        "description": item.unified.text,
                        "url": item.unified.source_url,
                        "filed_at": item.unified.created_at.isoformat(),
                    }
                    for item in items
                ],
                "scrape_source": "social_scraper",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            return {
                "items": [item.unified.model_dump() for item in items],
                "category": category,
                "scrape_source": "social_scraper",
            }

    async def push_via_redis(
        self, items: list[ScrapedItem], category: str, payload: dict = None
    ) -> bool:
        """Push data directly into DragonScope's Redis cache."""
        r = await self._get_redis()
        if not r:
            return False

        try:
            if payload is None:
                payload = self._transform_for_dragonscope(items, category)
            payload_json = json.dumps(payload, default=str)

            # Write to DragonScope's cache key
            cache_key = f"market:{category}"
            await r.set(cache_key, payload_json, ex=300)  # 5min TTL

            # Publish update notification — best-effort; data is already cached
            try:
                await r.publish("market:updates", json.dumps({
                    "category": category,
                    "source": "social_scraper",
                    "count": len(items),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))
            except Exception as pub_err:
                logger.warning(f"[DragonScope] Redis publish notification failed (data cached OK): {pub_err}")

            logger.info(f"[DragonScope] Pushed {len(items)} items to Redis:{category}")
            return True
        except Exception as e:
            logger.error(f"[DragonScope] Redis push failed: {e}")
            # Reset stale connection so _get_redis() will reconnect next call
            if self._redis is not None:
                try:
                    await self._redis.close()
                except Exception:
                    pass
                self._redis = None
            return False

    async def push_via_api(
        self, items: list[ScrapedItem], category: str, payload: dict = None
    ) -> bool:
        """Push data via DragonScope's API (fallback)."""
        try:
            if payload is None:
                payload = self._transform_for_dragonscope(items, category)
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            resp = await self._http.post(
                f"{self.api_url}/api/ingest/{category}",
                json=payload,
                headers=headers,
            )

            if resp.status_code in (200, 201):
                logger.info(f"[DragonScope] API push {len(items)} items to {category}")
                return True
            else:
                body = resp.text[:200] if resp.text else "(empty)"
                logger.warning(
                    f"[DragonScope] API push failed: {resp.status_code} — {body}"
                )
                return False
        except Exception as e:
            logger.error(f"[DragonScope] API push error: {e}")
            return False

    # Cap items per category to prevent oversized Redis payloads
    MAX_ITEMS_PER_CATEGORY = 500

    async def push(self, items: list[ScrapedItem]) -> dict:
        """Push items to DragonScope, grouped by category.

        Returns dict of {category: success_bool}.
        """
        from collections import defaultdict

        by_category = defaultdict(list)
        for item in items:
            category = PLATFORM_TO_CATEGORY.get(item.unified.platform, "news")
            by_category[category].append(item)

        results = {}
        for category, cat_items in by_category.items():
            if len(cat_items) > self.MAX_ITEMS_PER_CATEGORY:
                logger.warning(
                    f"[DragonScope] Truncating {category} batch from "
                    f"{len(cat_items)} to {self.MAX_ITEMS_PER_CATEGORY} items"
                )
                cat_items = cat_items[:self.MAX_ITEMS_PER_CATEGORY]
            # Transform once, reuse payload for both Redis and API paths
            payload = self._transform_for_dragonscope(cat_items, category)
            success = await self.push_via_redis(cat_items, category, payload)
            if not success:
                success = await self.push_via_api(cat_items, category, payload)
            results[category] = {"success": success, "count": len(cat_items)}

        return results

    async def close(self):
        """Close HTTP and Redis connections."""
        try:
            await self._http.aclose()
        except Exception as e:
            logger.debug(f"[DragonScope] HTTP close error: {e}")
        if self._redis:
            try:
                await self._redis.close()
            except Exception as e:
                logger.debug(f"[DragonScope] Redis close error: {e}")
            self._redis = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
