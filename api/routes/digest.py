"""Daily digest API — access LLM-generated economic briefings."""

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/digest", tags=["digest"])


@router.get("/today")
async def today_digest(db: Session = Depends(get_db)):
    """Get today's digest."""
    from storage.models import DailyDigest

    digest = db.query(DailyDigest).filter(DailyDigest.date == date.today()).first()
    if not digest:
        return {"status": "not_ready", "message": "Today's digest has not been generated yet."}

    return _format_digest(digest)


@router.get("/date/{target_date}")
async def get_digest_by_date(target_date: date, db: Session = Depends(get_db)):
    """Get digest for a specific date."""
    from storage.models import DailyDigest

    digest = db.query(DailyDigest).filter(DailyDigest.date == target_date).first()
    if not digest:
        return {"status": "not_found", "message": f"No digest for {target_date}"}

    return _format_digest(digest)


@router.get("/recent")
async def recent_digests(
    days: int = Query(7, ge=1, le=30),
    db: Session = Depends(get_db),
):
    """Get recent digests."""
    from storage.models import DailyDigest

    cutoff = date.today() - timedelta(days=days)
    digests = (
        db.query(DailyDigest)
        .filter(DailyDigest.date >= cutoff)
        .order_by(DailyDigest.date.desc())
        .all()
    )

    return {
        "count": len(digests),
        "digests": [_format_digest(d) for d in digests],
    }


@router.post("/generate")
async def trigger_digest(db: Session = Depends(get_db)):
    """Manually trigger digest generation for today."""
    from processors.daily_digest import DailyDigestGenerator

    generator = DailyDigestGenerator()
    result = generator.run()
    return result


def _format_digest(digest) -> dict:
    return {
        "date": str(digest.date),
        "summary": digest.summary,
        "top_themes": digest.top_themes,
        "sentiment_summary": digest.sentiment_summary,
        "key_data_releases": digest.key_data_releases,
        "new_circulars": digest.new_circulars,
        "created_at": digest.created_at.isoformat() if digest.created_at else None,
    }
