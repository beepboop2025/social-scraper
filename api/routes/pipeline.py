"""Pipeline API routes — manage scraper orchestration, routing, and health."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])


@router.get("/health")
async def pipeline_health():
    """Get health status of all scrapers and destinations."""
    try:
        from scheduler.health import HealthMonitor
        monitor = HealthMonitor()
        dashboard = await monitor.get_dashboard()
        return dashboard
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@router.get("/alerts")
async def pipeline_alerts():
    """Get active alerts for scrapers needing attention."""
    try:
        from scheduler.health import HealthMonitor
        monitor = HealthMonitor()
        alerts = await monitor.get_alerts()
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as e:
        return {"alerts": [], "error": str(e)}


@router.post("/trigger/{scraper_name}")
async def trigger_scraper(scraper_name: str):
    """Manually trigger a specific scraper task."""
    task_map = {
        "reddit": "scheduler.tasks.scrape_reddit",
        "twitter": "scheduler.tasks.scrape_twitter",
        "telegram": "scheduler.tasks.scrape_telegram",
        "hackernews": "scheduler.tasks.scrape_hackernews",
        "youtube": "scheduler.tasks.scrape_youtube",
        "rss": "scheduler.tasks.scrape_rss_financial",
        "rss_all": "scheduler.tasks.scrape_rss_all",
        "central_banks": "scheduler.tasks.scrape_central_banks",
        "sec": "scheduler.tasks.scrape_sec",
        "github": "scheduler.tasks.scrape_github",
        "discord": "scheduler.tasks.scrape_discord",
        "mastodon": "scheduler.tasks.scrape_mastodon",
        "web": "scheduler.tasks.scrape_web",
        "darkweb": "scheduler.tasks.scrape_darkweb",
    }

    if scraper_name not in task_map:
        raise HTTPException(status_code=404, detail=f"Unknown scraper: {scraper_name}. Available: {list(task_map.keys())}")

    try:
        from scheduler.celery_app import app
        result = app.send_task(task_map[scraper_name])
        return {
            "status": "triggered",
            "scraper": scraper_name,
            "task_id": result.id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger: {e}")


@router.get("/stats")
async def pipeline_stats():
    """Get pipeline statistics — items scraped, routing, throughput."""
    try:
        import redis.asyncio as aioredis
        import json
        import os

        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

        # Aggregate stats
        stats_keys = await r.keys("scraper:stats:*")
        scrapers = {}
        total_items = 0
        total_errors = 0
        for key in stats_keys:
            data = await r.get(key)
            if data:
                name = key.replace("scraper:stats:", "")
                parsed = json.loads(data)
                scrapers[name] = parsed
                total_items += parsed.get("total_items", 0)
                total_errors += parsed.get("total_errors", 0)

        global_total = await r.get("scraper:total_items") or "0"
        await r.close()

        return {
            "total_items_all_time": int(global_total),
            "total_items_24h": total_items,
            "total_errors_24h": total_errors,
            "error_rate": round(total_errors / max(total_items, 1) * 100, 2),
            "active_scrapers": len(scrapers),
            "scrapers": scrapers,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/destinations")
async def destination_status():
    """Check connectivity to DragonScope and LiquiFi."""
    import os
    import httpx

    results = {}

    # DragonScope
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{os.getenv('DRAGONSCOPE_API_URL', 'http://localhost:3456')}/api/health")
            results["dragonscope"] = {
                "status": "connected" if resp.status_code == 200 else "error",
                "response_code": resp.status_code,
            }
    except Exception as e:
        results["dragonscope"] = {"status": "unreachable", "error": str(e)}

    # LiquiFi
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{os.getenv('LIQUIFI_API_URL', 'http://localhost:8001')}/api/health")
            results["liquifi"] = {
                "status": "connected" if resp.status_code == 200 else "error",
                "response_code": resp.status_code,
            }
    except Exception as e:
        results["liquifi"] = {"status": "unreachable", "error": str(e)}

    return results
