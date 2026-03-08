"""Structured economic data API — time-series from FRED, RBI, World Bank, etc."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["economic_data"])


@router.get("/series")
async def get_series(
    indicator: str = Query(..., description="e.g., fred_FEDFUNDS, rbi_forex_reserves"),
    days: int = Query(365, ge=1, le=3650),
    db: Session = Depends(get_db),
):
    """Get time-series for a specific economic indicator."""
    from storage.models import EconomicData

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    results = (
        db.query(EconomicData)
        .filter(
            EconomicData.indicator == indicator,
            EconomicData.date >= cutoff,
        )
        .order_by(EconomicData.date)
        .all()
    )

    return {
        "indicator": indicator,
        "count": len(results),
        "data": [
            {
                "date": r.date.isoformat(),
                "value": float(r.value) if r.value is not None else None,
                "source": r.source,
            }
            for r in results
        ],
    }


@router.get("/indicators")
async def list_indicators(
    source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """List all available economic indicators."""
    from storage.models import EconomicData

    query = db.query(
        EconomicData.source,
        EconomicData.indicator,
        func.count().label("data_points"),
        func.min(EconomicData.date).label("earliest"),
        func.max(EconomicData.date).label("latest"),
    )

    if source:
        query = query.filter(EconomicData.source == source)

    results = (
        query
        .group_by(EconomicData.source, EconomicData.indicator)
        .order_by(EconomicData.source, EconomicData.indicator)
        .all()
    )

    return {
        "count": len(results),
        "indicators": [
            {
                "source": r[0],
                "indicator": r[1],
                "data_points": r[2],
                "earliest": r[3].isoformat() if r[3] else None,
                "latest": r[4].isoformat() if r[4] else None,
            }
            for r in results
        ],
    }


@router.get("/latest")
async def latest_values(
    source: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Get the latest value for each indicator."""
    from storage.models import EconomicData

    # Subquery to get max date per indicator
    subq = (
        db.query(
            EconomicData.indicator,
            func.max(EconomicData.date).label("max_date"),
        )
        .group_by(EconomicData.indicator)
    )

    if source:
        subq = subq.filter(EconomicData.source == source)

    subq = subq.subquery()

    results = (
        db.query(EconomicData)
        .join(
            subq,
            (EconomicData.indicator == subq.c.indicator)
            & (EconomicData.date == subq.c.max_date),
        )
        .order_by(EconomicData.source, EconomicData.indicator)
        .limit(limit)
        .all()
    )

    return {
        "count": len(results),
        "data": [
            {
                "source": r.source,
                "indicator": r.indicator,
                "date": r.date.isoformat(),
                "value": float(r.value) if r.value is not None else None,
                "unit": r.unit,
            }
            for r in results
        ],
    }


@router.get("/compare")
async def compare_indicators(
    indicators: str = Query(..., description="Comma-separated indicators, e.g. fred_FEDFUNDS,fred_DGS10"),
    days: int = Query(365, ge=1, le=3650),
    db: Session = Depends(get_db),
):
    """Compare multiple indicators over time."""
    from storage.models import EconomicData

    indicator_list = [i.strip() for i in indicators.split(",")]
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    results = (
        db.query(EconomicData)
        .filter(
            EconomicData.indicator.in_(indicator_list),
            EconomicData.date >= cutoff,
        )
        .order_by(EconomicData.date)
        .all()
    )

    series: dict[str, list] = {ind: [] for ind in indicator_list}
    for r in results:
        if r.indicator in series:
            series[r.indicator].append({
                "date": r.date.isoformat(),
                "value": float(r.value) if r.value is not None else None,
            })

    return {"indicators": indicator_list, "period_days": days, "series": series}
