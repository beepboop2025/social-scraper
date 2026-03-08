"""Health monitoring — tracks scraper uptime, error rates, and data freshness."""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Monitors the health of all scrapers and the pipeline.

    Tracks:
    - Scraper uptime and last successful run
    - Error rates per scraper
    - Data freshness (time since last data from each source)
    - Pipeline throughput (items/minute)
    - Destination connectivity (DragonScope, LiquiFi)
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self._redis = None

    async def _get_redis(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def record_scrape(self, scraper_name: str, items_count: int, duration_ms: float, error: Optional[str] = None):
        """Record a scrape result for monitoring."""
        r = await self._get_redis()
        now = datetime.now(timezone.utc).isoformat()

        stats_key = f"scraper:stats:{scraper_name}"
        existing = await r.get(stats_key)
        stats = json.loads(existing) if existing else {
            "total_scrapes": 0,
            "total_items": 0,
            "total_errors": 0,
            "first_seen": now,
        }

        stats["total_scrapes"] += 1
        stats["total_items"] += items_count
        stats["last_scrape_at"] = now
        stats["last_items_count"] = items_count
        stats["last_duration_ms"] = duration_ms

        if error:
            stats["total_errors"] += 1
            stats["last_error"] = error
            stats["last_error_at"] = now
        else:
            stats["last_success_at"] = now

        await r.set(stats_key, json.dumps(stats), ex=86400)  # 24hr TTL

        # Update global counter
        await r.incrby("scraper:total_items", items_count)

    async def get_dashboard(self) -> dict:
        """Get full health dashboard."""
        r = await self._get_redis()

        # Collect all scraper stats
        keys = await r.keys("scraper:stats:*")
        scrapers = {}
        for key in keys:
            data = await r.get(key)
            if data:
                name = key.replace("scraper:stats:", "")
                scrapers[name] = json.loads(data)

        # Get health check result
        health_data = await r.get("scraper:health")
        health = json.loads(health_data) if health_data else {}

        # Get total items
        total = await r.get("scraper:total_items") or "0"

        return {
            "status": "operational",
            "total_items_scraped": int(total),
            "active_scrapers": len(scrapers),
            "scrapers": scrapers,
            "infrastructure": health,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def get_alerts(self) -> list[dict]:
        """Check for scraper issues that need attention."""
        dashboard = await self.get_dashboard()
        alerts = []

        for name, stats in dashboard.get("scrapers", {}).items():
            # Alert if scraper hasn't run in expected time
            last_success = stats.get("last_success_at")
            if last_success:
                from datetime import datetime as dt
                last = dt.fromisoformat(last_success)
                age_minutes = (datetime.now(timezone.utc) - last).total_seconds() / 60

                # Different thresholds per scraper type
                thresholds = {
                    "central_bank": 10,  # Should run every 2min
                    "rss": 10,
                    "reddit": 15,
                    "twitter": 15,
                    "hackernews": 30,
                    "youtube": 30,
                    "github": 60,
                    "darkweb": 120,
                }
                threshold = thresholds.get(name, 60)
                if age_minutes > threshold:
                    alerts.append({
                        "scraper": name,
                        "type": "stale_data",
                        "message": f"{name} hasn't scraped in {age_minutes:.0f}min (threshold: {threshold}min)",
                        "severity": "high" if age_minutes > threshold * 2 else "medium",
                    })

            # Alert on high error rate
            total = stats.get("total_scrapes", 0)
            errors = stats.get("total_errors", 0)
            if total > 10 and errors / total > 0.3:
                alerts.append({
                    "scraper": name,
                    "type": "high_error_rate",
                    "message": f"{name} error rate: {errors}/{total} ({errors/total*100:.0f}%)",
                    "severity": "high",
                })

        return alerts
