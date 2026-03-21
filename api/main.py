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

# Enterprise features
from api.routes.dashboard import router as dashboard_router
from api.routes.auth import router as auth_router
from api.routes.webhooks import router as webhooks_router


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
    print("[EconScraper] v5.0.0 ready — enterprise economic data platform")
    print("[EconScraper] 14 collectors | 8 processors | RAG | Backpressure | Webhooks | Metrics")
    yield
    print("[EconScraper] Shutting down")


app = FastAPI(
    title="EconScraper",
    version="5.0.0",
    description=(
        "Enterprise economic data collection and AI analysis platform. "
        "Plugin-based collectors for FRED, RBI, SEBI, NSE, World Bank, IMF, RSS feeds, "
        "Telegram, Twitter, and more. NLP/ML pipeline with FinBERT sentiment, "
        "multi-language support, spaCy NER, topic classification, and embedding-based RAG. "
        "Enterprise features: backpressure management, data quality scoring, "
        "API key auth, webhooks, data retention, Prometheus metrics, Kafka DLQ. "
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

# Enterprise features
app.include_router(dashboard_router, prefix="/api/v4")
app.include_router(auth_router, prefix="/api/v4")
app.include_router(webhooks_router, prefix="/api/v4")


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
        "version": "5.0.0",
        "redis": "connected" if redis_ok else "disconnected",
        "docs": "/docs",
    }


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    from fastapi.responses import PlainTextResponse
    from monitoring.metrics import get_metrics_registry

    registry = get_metrics_registry()
    registry.update_queue_depths()
    return PlainTextResponse(registry.collect_all(), media_type="text/plain")


@app.get("/api/v4/backpressure")
async def backpressure_status():
    """Current backpressure state — queue depths and throttle level."""
    from core.backpressure import get_backpressure_manager
    bp = get_backpressure_manager()
    return bp.check()


@app.get("/api/v4/kafka/health")
async def kafka_health():
    """Kafka consumer group health — lag, DLQ depth."""
    try:
        from pipeline.consumer_groups import ConsumerGroupManager
        mgr = ConsumerGroupManager()
        result = mgr.health()
        mgr.close()
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/v4/kafka/dlq")
async def kafka_dlq(limit: int = 50):
    """Read messages from the dead letter queue."""
    try:
        from pipeline.consumer_groups import ConsumerGroupManager
        mgr = ConsumerGroupManager()
        messages = mgr.get_dlq_messages(limit=limit)
        mgr.close()
        return {"messages": messages, "count": len(messages)}
    except Exception as e:
        return {"messages": [], "error": str(e)}


@app.get("/api/v4/quality/stats")
async def quality_stats():
    """Get quality scoring statistics."""
    try:
        from processors.quality_scorer import QualityScorer
        scorer = QualityScorer()
        return scorer.stats
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/language/stats")
async def language_stats():
    """Get language distribution statistics."""
    try:
        from processors.language_detector import get_language_stats
        return get_language_stats().to_dict()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/retention/policies")
async def retention_policies():
    """Get current data retention policies."""
    from core.retention import RETENTION_POLICIES
    return {"policies": RETENTION_POLICIES}


@app.get("/api/v4/grafana/dashboard")
async def grafana_dashboard():
    """Get Grafana dashboard JSON template."""
    from monitoring.metrics import GRAFANA_DASHBOARD_JSON
    return GRAFANA_DASHBOARD_JSON


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
