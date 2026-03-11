"""Health tracking — reads/writes collector health to Redis."""

import json
import logging
import os
from datetime import datetime, timezone

import redis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def get_all_health() -> dict:
    """Read health status of all collectors from Redis."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        keys = list(r.scan_iter("health:*"))
        result = {}
        for key in keys:
            data = r.get(key)
            if data:
                name = key.replace("health:", "")
                result[name] = json.loads(data)
        r.close()
        return result
    except Exception as e:
        logger.error(f"Health read failed: {e}")
        return {}


def get_source_health(source: str) -> dict:
    """Get health of a single source."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        data = r.get(f"health:{source}")
        r.close()
        return json.loads(data) if data else {"status": "unknown"}
    except Exception:
        return {"status": "unknown"}


def get_alerts(limit: int = 20) -> list[dict]:
    """Get recent collector failure alerts."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        raw = r.lrange("alerts:collector_failures", 0, limit - 1)
        r.close()
        return [json.loads(a) for a in raw]
    except Exception:
        return []


def system_status() -> dict:
    """Get overall system health summary."""
    all_health = get_all_health()
    alerts = get_alerts(10)

    total = len(all_health)
    healthy = sum(1 for h in all_health.values() if h.get("status") == "success")
    running = sum(1 for h in all_health.values() if h.get("status") == "running")
    failed = sum(1 for h in all_health.values() if h.get("status") == "failed")

    if failed > total * 0.5:
        overall = "degraded"
    elif failed > 0:
        overall = "partial"
    else:
        overall = "healthy"

    return {
        "status": overall,
        "total_sources": total,
        "healthy": healthy,
        "running": running,
        "failed": failed,
        "recent_alerts": alerts[:5],
        "sources": all_health,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
