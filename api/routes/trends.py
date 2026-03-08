"""Trends API — topic trends, sentiment trends, entity frequency."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/trends", tags=["trends"])


@router.get("/topics")
async def topic_trends(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """Topic distribution over time."""
    from storage.models import ArticleTopic, Article

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    results = (
        db.query(
            func.date(Article.published_at).label("date"),
            ArticleTopic.topic,
            func.count().label("count"),
        )
        .join(Article, Article.id == ArticleTopic.article_id)
        .filter(Article.published_at >= cutoff)
        .group_by(func.date(Article.published_at), ArticleTopic.topic)
        .order_by(func.date(Article.published_at))
        .all()
    )

    data: dict[str, list] = {}
    for date_val, topic, count in results:
        if topic not in data:
            data[topic] = []
        data[topic].append({"date": str(date_val), "count": count})

    return {"period_days": days, "topics": data}


@router.get("/sentiment")
async def sentiment_trends(
    days: int = Query(7, ge=1, le=90),
    source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Average sentiment over time, optionally filtered by source."""
    from storage.models import SentimentScore, Article

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    query = (
        db.query(
            func.date(Article.published_at).label("date"),
            func.avg(SentimentScore.overall).label("avg_sentiment"),
            func.count().label("count"),
        )
        .join(Article, Article.id == SentimentScore.article_id)
        .filter(Article.published_at >= cutoff)
    )

    if source:
        query = query.filter(Article.source == source)

    results = (
        query
        .group_by(func.date(Article.published_at))
        .order_by(func.date(Article.published_at))
        .all()
    )

    return {
        "period_days": days,
        "source_filter": source,
        "data": [
            {
                "date": str(date_val),
                "avg_sentiment": round(float(avg), 4),
                "article_count": count,
            }
            for date_val, avg, count in results
        ],
    }


@router.get("/entities")
async def entity_trends(
    days: int = Query(7, ge=1, le=90),
    entity_type: Optional[str] = Query(None, description="ORG, PERSON, FIN_ORG, POLICY, TICKER"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Most frequently mentioned entities."""
    from storage.models import Entity, Article

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    query = (
        db.query(
            Entity.entity_type,
            Entity.entity_value,
            func.count().label("mentions"),
        )
        .join(Article, Article.id == Entity.article_id)
        .filter(Article.published_at >= cutoff)
    )

    if entity_type:
        query = query.filter(Entity.entity_type == entity_type)

    results = (
        query
        .group_by(Entity.entity_type, Entity.entity_value)
        .order_by(func.count().desc())
        .limit(limit)
        .all()
    )

    return {
        "period_days": days,
        "type_filter": entity_type,
        "entities": [
            {"type": etype, "value": evalue, "mentions": count}
            for etype, evalue, count in results
        ],
    }


@router.get("/policy-direction")
async def policy_direction_trend(
    days: int = Query(30, ge=1, le=180),
    db: Session = Depends(get_db),
):
    """Hawkish vs dovish sentiment trend over time."""
    from storage.models import SentimentScore, Article

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    results = (
        db.query(
            func.date(Article.published_at).label("date"),
            SentimentScore.policy_direction,
            func.count().label("count"),
        )
        .join(Article, Article.id == SentimentScore.article_id)
        .filter(Article.published_at >= cutoff)
        .group_by(func.date(Article.published_at), SentimentScore.policy_direction)
        .order_by(func.date(Article.published_at))
        .all()
    )

    data: dict[str, dict[str, int]] = {}
    for date_val, direction, count in results:
        d = str(date_val)
        if d not in data:
            data[d] = {"hawkish": 0, "dovish": 0, "neutral": 0}
        data[d][direction] = count

    return {
        "period_days": days,
        "data": [
            {"date": d, **counts}
            for d, counts in sorted(data.items())
        ],
    }
