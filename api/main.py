"""EconScraper v4.0 — Modular Economic Data Collection & AI Analysis Platform.

Plugin-based collectors, NLP/ML pipeline, RAG-powered API.
TimescaleDB + pgvector + MinIO storage. 24/7 operation.
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.database import init_db

# v4.0 routes
from api.routes.semantic_search import router as semantic_router
from api.routes.ask import router as ask_router
from api.routes.trends import router as trends_router
from api.routes.data import router as data_router
from api.routes.digest import router as digest_router
from api.routes.health_v4 import router as monitoring_router

# API Key Manager
from apikeys.routes import router as keys_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[EconScraper] Initializing database tables...")
    init_db()
    # Auto-inject vault keys into runtime
    try:
        from apikeys.injector import KeyInjector
        injector = KeyInjector()
        result = injector.sync_from_vault()
        print(f"[EconScraper] Injected {result.get('injected', 0)} API keys from vault")
    except Exception:
        pass
    print("[EconScraper] v4.1.0 ready — modular economic data platform")
    print("[EconScraper] 14 collectors | 8 processors | RAG-powered API | Key Manager")
    yield
    print("[EconScraper] Shutting down")


app = FastAPI(
    title="EconScraper",
    version="4.1.0",
    description=(
        "Modular economic data collection and AI analysis platform. "
        "Plugin-based collectors for FRED, RBI, SEBI, NSE, World Bank, IMF, RSS feeds, "
        "Telegram, Twitter, and more. NLP/ML pipeline with FinBERT sentiment, "
        "spaCy NER, topic classification, and embedding-based RAG search. "
        "24/7 automated via Celery Beat."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

# v4.0 routes
app.include_router(semantic_router, prefix="/api/v4")
app.include_router(ask_router, prefix="/api/v4")
app.include_router(trends_router, prefix="/api/v4")
app.include_router(data_router, prefix="/api/v4")
app.include_router(digest_router, prefix="/api/v4")
app.include_router(monitoring_router, prefix="/api/v4")

# API Key Manager
app.include_router(keys_router, prefix="/api/v4")


@app.get("/health")
async def health():
    import os
    try:
        import redis as _redis
        r = _redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        r.ping()
        redis_ok = True
        r.close()
    except Exception:
        redis_ok = False

    return {
        "status": "ok",
        "service": "econscraper",
        "version": "4.1.0",
        "redis": "connected" if redis_ok else "disconnected",
        "docs": "/docs",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
