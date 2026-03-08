"""Social Scraper Platform v3.0 — 15-source intelligence pipeline.

Feeds data to:
- DragonScope (financial analytics dashboard)
- LiquiFi (Indian treasury management)

Runs 24/7 via Celery Beat scheduler with health monitoring.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from api.database import init_db
from api.routes.scrape import router as scrape_router
from api.routes.analysis import router as analysis_router
from api.routes.search import router as search_router
from api.routes.pipeline import router as pipeline_router
from api.routes.financial import router as financial_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[API] Initializing database tables...")
    init_db()
    print("[API] Social Scraper Intelligence Platform v3.0 ready")
    print("[API] Scrapers: 15 sources | Destinations: DragonScope + LiquiFi")
    yield
    print("[API] Shutting down")


app = FastAPI(
    title="Social Scraper Intelligence Platform",
    version="3.0.0",
    description=(
        "15-source social intelligence pipeline with dark web monitoring. "
        "Feeds DragonScope (financial analytics) and LiquiFi (treasury management). "
        "24/7 automated collection via Celery Beat."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Existing routes
app.include_router(scrape_router, prefix="/api")
app.include_router(analysis_router, prefix="/api")
app.include_router(search_router, prefix="/api")

# New v3.0 routes
app.include_router(pipeline_router, prefix="/api")
app.include_router(financial_router, prefix="/api")


@app.get("/health")
async def health():
    """Basic health check with scraper status summary."""
    import os
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        await r.ping()
        redis_ok = True
        total_items = int(await r.get("scraper:total_items") or "0")
        await r.close()
    except Exception:
        redis_ok = False
        total_items = 0

    return {
        "status": "ok",
        "service": "social-scraper-platform",
        "version": "3.0.0",
        "redis": "connected" if redis_ok else "disconnected",
        "total_items_scraped": total_items,
        "scrapers": [
            "twitter", "telegram", "reddit", "discord", "youtube",
            "hackernews", "rss", "web", "darkweb", "mastodon",
            "github", "sec_edgar", "central_bank",
        ],
        "destinations": ["dragonscope", "liquifi"],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
