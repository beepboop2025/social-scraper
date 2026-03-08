"""Enhanced health & monitoring API for econscraper v4."""

import logging
import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/health")
async def detailed_health(db: Session = Depends(get_db)):
    """Comprehensive health check — DB, Redis, sources, processors."""
    from storage.models import CollectionLog, Article, EconomicData

    # DB health
    try:
        article_count = db.query(func.count(Article.id)).scalar()
        econ_count = db.query(func.count(EconomicData.id)).scalar()
        db_ok = True
    except Exception as e:
        article_count = econ_count = 0
        db_ok = False

    # Redis health
    redis_ok = False
    try:
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        r.ping()
        redis_ok = True
        r.close()
    except Exception:
        pass

    # Recent collection status
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_logs = (
        db.query(
            CollectionLog.source,
            CollectionLog.status,
            func.max(CollectionLog.run_at).label("last_run"),
            func.sum(CollectionLog.records_collected).label("total"),
        )
        .filter(CollectionLog.run_at >= cutoff)
        .group_by(CollectionLog.source, CollectionLog.status)
        .all()
    )

    sources = {}
    for source, status, last_run, total in recent_logs:
        if source not in sources:
            sources[source] = {"statuses": {}, "last_run": None, "total_records": 0}
        sources[source]["statuses"][status] = sources[source]["statuses"].get(status, 0) + 1
        sources[source]["total_records"] += int(total or 0)
        if last_run:
            sources[source]["last_run"] = last_run.isoformat()

    return {
        "status": "healthy" if db_ok and redis_ok else "degraded",
        "service": "econscraper",
        "version": "4.0.0",
        "database": {"connected": db_ok, "articles": article_count, "economic_data_points": econ_count},
        "redis": {"connected": redis_ok},
        "sources_24h": sources,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/sources")
async def source_status(db: Session = Depends(get_db)):
    """Per-source collection stats."""
    from storage.models import CollectionLog

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    logs = (
        db.query(CollectionLog)
        .filter(CollectionLog.run_at >= cutoff)
        .order_by(CollectionLog.run_at.desc())
        .all()
    )

    by_source: dict[str, list] = {}
    for log in logs:
        if log.source not in by_source:
            by_source[log.source] = []
        by_source[log.source].append({
            "status": log.status,
            "records": log.records_collected,
            "duration_s": log.duration_seconds,
            "error": log.error_message,
            "run_at": log.run_at.isoformat(),
        })

    return {"sources": by_source}


@router.get("/alerts")
async def active_alerts():
    """Get active alerts from Redis health monitor."""
    try:
        from core.health import get_alerts
        alerts = get_alerts()
        return {"alerts": alerts}
    except Exception as e:
        return {"alerts": [], "error": str(e)}


@router.get("/stats")
async def platform_stats(db: Session = Depends(get_db)):
    """Aggregate platform statistics."""
    from storage.models import (
        Article, ArticleEmbedding, ArticleTopic,
        DailyDigest, EconomicData, Entity, SentimentScore,
    )

    stats = {
        "articles": db.query(func.count(Article.id)).scalar(),
        "economic_data_points": db.query(func.count(EconomicData.id)).scalar(),
        "embeddings": db.query(func.count(ArticleEmbedding.id)).scalar(),
        "sentiment_scores": db.query(func.count(SentimentScore.id)).scalar(),
        "entities": db.query(func.count(Entity.id)).scalar(),
        "topic_tags": db.query(func.count(ArticleTopic.id)).scalar(),
        "daily_digests": db.query(func.count(DailyDigest.id)).scalar(),
        "unprocessed_articles": db.query(func.count(Article.id)).filter(Article.is_processed == False).scalar(),
    }

    # Articles by source
    source_counts = (
        db.query(Article.source, func.count().label("count"))
        .group_by(Article.source)
        .order_by(func.count().desc())
        .all()
    )
    stats["articles_by_source"] = {s: c for s, c in source_counts}

    return stats
