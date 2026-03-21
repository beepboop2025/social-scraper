"""Real-time dashboard API — live stats, trends, and source health."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, case, desc
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/live")
async def dashboard_live(db: Session = Depends(get_db)):
    """Real-time dashboard stats: articles, scrapers, queues, sentiment, topics, source health."""
    from storage.models import Article, SentimentScore, CollectionLog

    now = datetime.now(timezone.utc)
    hour_ago = now - timedelta(hours=1)
    day_ago = now - timedelta(hours=24)

    # Articles collected
    articles_last_hour = db.query(func.count(Article.id)).filter(
        Article.collected_at >= hour_ago
    ).scalar()
    articles_last_day = db.query(func.count(Article.id)).filter(
        Article.collected_at >= day_ago
    ).scalar()

    # Active scrapers and status
    scraper_status = (
        db.query(
            CollectionLog.source,
            CollectionLog.status,
            func.max(CollectionLog.run_at).label("last_run"),
        )
        .filter(CollectionLog.run_at >= day_ago)
        .group_by(CollectionLog.source, CollectionLog.status)
        .all()
    )

    scrapers = {}
    for source, status, last_run in scraper_status:
        if source not in scrapers:
            scrapers[source] = {"statuses": {}, "last_run": None}
        scrapers[source]["statuses"][status] = scrapers[source]["statuses"].get(status, 0) + 1
        if last_run:
            scrapers[source]["last_run"] = last_run.isoformat()

    active_scrapers = len(scrapers)

    # Queue depths from Redis
    queue_depths = {}
    try:
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        for q in ["collectors", "processors", "routing", "health", "celery"]:
            try:
                queue_depths[q] = r.llen(q)
            except Exception:
                queue_depths[q] = 0
        r.close()
    except Exception:
        pass

    # Sentiment distribution (last 24h)
    sentiment_rows = (
        db.query(
            case(
                (SentimentScore.overall > 0.2, "positive"),
                (SentimentScore.overall < -0.2, "negative"),
                else_="neutral",
            ).label("category"),
            func.count().label("count"),
        )
        .filter(SentimentScore.created_at >= day_ago)
        .group_by("category")
        .all()
    )
    sentiment_dist = {row.category: row.count for row in sentiment_rows}

    # Top trending topics (articles by category, last 24h)
    topic_rows = (
        db.query(Article.category, func.count().label("count"))
        .filter(Article.collected_at >= day_ago, Article.category.isnot(None))
        .group_by(Article.category)
        .order_by(desc("count"))
        .limit(10)
        .all()
    )
    trending_topics = [{"topic": t.category, "count": t.count} for t in topic_rows]

    # Source health matrix
    source_health = {}
    for source, info in scrapers.items():
        statuses = info["statuses"]
        total = sum(statuses.values())
        success = statuses.get("success", 0)
        health_pct = round(success / total * 100, 1) if total > 0 else 0
        source_health[source] = {
            "health_pct": health_pct,
            "total_runs_24h": total,
            "last_run": info["last_run"],
        }

    # Backpressure state
    backpressure = {}
    try:
        import redis as _redis
        r = _redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        bp_data = r.get("backpressure:state")
        if bp_data:
            backpressure = json.loads(bp_data)
        r.close()
    except Exception:
        pass

    return {
        "articles": {
            "last_hour": articles_last_hour,
            "last_day": articles_last_day,
        },
        "active_scrapers": active_scrapers,
        "scraper_details": scrapers,
        "queue_depths": queue_depths,
        "sentiment_distribution": sentiment_dist,
        "trending_topics": trending_topics,
        "source_health": source_health,
        "backpressure": backpressure,
        "timestamp": now.isoformat(),
    }


@router.get("/trends")
async def dashboard_trends(db: Session = Depends(get_db)):
    """Trending entities, topics, and sentiment shifts over time."""
    from storage.models import Article, Entity, SentimentScore, ArticleTopic

    now = datetime.now(timezone.utc)
    day_ago = now - timedelta(hours=24)
    week_ago = now - timedelta(days=7)

    # Trending entities (last 24h)
    entity_rows = (
        db.query(Entity.entity_type, Entity.entity_value, func.count().label("mentions"))
        .join(Article, Entity.article_id == Article.id)
        .filter(Article.collected_at >= day_ago)
        .group_by(Entity.entity_type, Entity.entity_value)
        .order_by(desc("mentions"))
        .limit(20)
        .all()
    )
    trending_entities = [
        {"type": e.entity_type, "value": e.entity_value, "mentions": e.mentions}
        for e in entity_rows
    ]

    # Topic trends (compare 24h vs previous 24h)
    prev_day = day_ago - timedelta(hours=24)
    current_topics = dict(
        db.query(ArticleTopic.topic, func.count().label("count"))
        .join(Article, ArticleTopic.article_id == Article.id)
        .filter(Article.collected_at >= day_ago)
        .group_by(ArticleTopic.topic)
        .all()
    )
    previous_topics = dict(
        db.query(ArticleTopic.topic, func.count().label("count"))
        .join(Article, ArticleTopic.article_id == Article.id)
        .filter(Article.collected_at >= prev_day, Article.collected_at < day_ago)
        .group_by(ArticleTopic.topic)
        .all()
    )

    topic_changes = []
    all_topics = set(current_topics.keys()) | set(previous_topics.keys())
    for topic in all_topics:
        curr = current_topics.get(topic, 0)
        prev = previous_topics.get(topic, 0)
        change = curr - prev
        pct_change = round(change / max(prev, 1) * 100, 1)
        topic_changes.append({
            "topic": topic,
            "current_count": curr,
            "previous_count": prev,
            "change": change,
            "pct_change": pct_change,
        })
    topic_changes.sort(key=lambda x: abs(x["change"]), reverse=True)

    # Sentiment shifts (hourly buckets, last 24h)
    sentiment_hourly = (
        db.query(
            func.date_trunc("hour", SentimentScore.created_at).label("hour"),
            func.avg(SentimentScore.overall).label("avg_sentiment"),
            func.count().label("count"),
        )
        .filter(SentimentScore.created_at >= day_ago)
        .group_by("hour")
        .order_by("hour")
        .all()
    )
    sentiment_timeline = [
        {
            "hour": row.hour.isoformat() if row.hour else "",
            "avg_sentiment": round(float(row.avg_sentiment or 0), 4),
            "count": row.count,
        }
        for row in sentiment_hourly
    ]

    return {
        "trending_entities": trending_entities,
        "topic_changes": topic_changes[:15],
        "sentiment_timeline": sentiment_timeline,
        "timestamp": now.isoformat(),
    }


@router.get("/sources")
async def dashboard_sources(db: Session = Depends(get_db)):
    """Per-source collection stats, error rates, and latency."""
    from storage.models import CollectionLog

    now = datetime.now(timezone.utc)
    day_ago = now - timedelta(hours=24)

    # Aggregate per-source stats
    source_rows = (
        db.query(
            CollectionLog.source,
            func.count().label("total_runs"),
            func.sum(case((CollectionLog.status == "success", 1), else_=0)).label("successes"),
            func.sum(case((CollectionLog.status == "failed", 1), else_=0)).label("failures"),
            func.sum(CollectionLog.records_collected).label("total_records"),
            func.avg(CollectionLog.duration_seconds).label("avg_duration"),
            func.max(CollectionLog.duration_seconds).label("max_duration"),
            func.max(CollectionLog.run_at).label("last_run"),
        )
        .filter(CollectionLog.run_at >= day_ago)
        .group_by(CollectionLog.source)
        .all()
    )

    sources = {}
    for row in source_rows:
        total = row.total_runs or 0
        successes = int(row.successes or 0)
        failures = int(row.failures or 0)
        error_rate = round(failures / max(total, 1) * 100, 1)

        sources[row.source] = {
            "total_runs": total,
            "successes": successes,
            "failures": failures,
            "error_rate_pct": error_rate,
            "total_records": int(row.total_records or 0),
            "avg_duration_s": round(float(row.avg_duration or 0), 2),
            "max_duration_s": round(float(row.max_duration or 0), 2),
            "last_run": row.last_run.isoformat() if row.last_run else None,
        }

    # Recent errors (last 10 failures)
    recent_errors = (
        db.query(CollectionLog)
        .filter(CollectionLog.status == "failed", CollectionLog.run_at >= day_ago)
        .order_by(CollectionLog.run_at.desc())
        .limit(10)
        .all()
    )
    errors = [
        {
            "source": e.source,
            "error": e.error_message,
            "run_at": e.run_at.isoformat(),
        }
        for e in recent_errors
    ]

    return {
        "sources": sources,
        "recent_errors": errors,
        "timestamp": now.isoformat(),
    }
