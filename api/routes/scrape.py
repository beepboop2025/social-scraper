"""Scraping API routes — trigger and manage scrape jobs."""

import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from api.database import get_db
from api.models import ScrapeJob, ScrapedPost

router = APIRouter(prefix="/scrape", tags=["scraping"])


class ScrapeRequest(BaseModel):
    platform: str  # twitter | telegram
    query: Optional[str] = None
    channel: Optional[str] = None
    count: int = 20


class ScrapeJobResponse(BaseModel):
    batch_id: str
    status: str
    platform: str
    query: Optional[str]
    channel: Optional[str]


@router.post("/trigger", response_model=ScrapeJobResponse)
async def trigger_scrape(
    req: ScrapeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Trigger a new scrape job. Runs in the background."""
    if req.platform not in ("twitter", "telegram"):
        raise HTTPException(400, "platform must be 'twitter' or 'telegram'")
    if not req.query and not req.channel:
        raise HTTPException(400, "Either query or channel is required")

    batch_id = uuid.uuid4().hex[:12]
    job = ScrapeJob(
        batch_id=batch_id,
        platform=req.platform,
        query=req.query,
        channel=req.channel,
        status="pending",
        created_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Run scrape in background
    background_tasks.add_task(_run_scrape, batch_id, req)

    return ScrapeJobResponse(
        batch_id=batch_id,
        status="pending",
        platform=req.platform,
        query=req.query,
        channel=req.channel,
    )


@router.get("/jobs")
def list_jobs(status: Optional[str] = None, limit: int = 20, db: Session = Depends(get_db)):
    """List scrape jobs, optionally filtered by status."""
    q = db.query(ScrapeJob).order_by(ScrapeJob.created_at.desc())
    if status:
        q = q.filter(ScrapeJob.status == status)
    jobs = q.limit(min(limit, 100)).all()
    return [
        {
            "batch_id": j.batch_id,
            "platform": j.platform,
            "query": j.query,
            "channel": j.channel,
            "status": j.status,
            "items_scraped": j.items_scraped,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
        }
        for j in jobs
    ]


@router.get("/jobs/{batch_id}")
def get_job(batch_id: str, db: Session = Depends(get_db)):
    """Get details of a specific scrape job."""
    job = db.query(ScrapeJob).filter(ScrapeJob.batch_id == batch_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    return {
        "batch_id": job.batch_id,
        "platform": job.platform,
        "query": job.query,
        "channel": job.channel,
        "status": job.status,
        "items_scraped": job.items_scraped,
        "items_failed": job.items_failed,
        "error_message": job.error_message,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


async def _run_scrape(batch_id: str, req: ScrapeRequest):
    """Background task to execute a scrape job."""
    from api.database import SessionLocal
    db = SessionLocal()
    try:
        job = db.query(ScrapeJob).filter(ScrapeJob.batch_id == batch_id).first()
        if not job:
            return

        job.status = "running"
        job.started_at = datetime.utcnow()
        db.commit()

        # Import scrapers lazily to avoid circular imports
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        items = []
        if req.platform == "twitter":
            from main import quick_scrape_twitter
            result = await quick_scrape_twitter(
                queries=[req.query] if req.query else [],
                cookies_path="cookies.json",
                count=req.count,
            )
            items = result if result else []
        elif req.platform == "telegram" and req.channel:
            from main import quick_scrape_telegram
            result = await quick_scrape_telegram(
                channels=[req.channel],
                limit=req.count,
            )
            items = result if result else []

        # Store results in DB
        stored = 0
        for item in items:
            unified = item.unified if hasattr(item, "unified") else item
            post = ScrapedPost(
                platform=req.platform,
                platform_id=str(unified.id),
                content_type=unified.content_type.value if hasattr(unified.content_type, "value") else str(unified.content_type),
                text=unified.text,
                author_username=unified.author.username if unified.author else None,
                author_display_name=unified.author.display_name if unified.author else "Unknown",
                likes=unified.engagement.likes if unified.engagement else 0,
                replies=unified.engagement.replies if unified.engagement else 0,
                reposts=unified.engagement.reposts if unified.engagement else 0,
                views=unified.engagement.views if unified.engagement else None,
                hashtags=unified.hashtags,
                mentions=unified.mentions,
                urls=unified.urls,
                search_query=req.query,
                batch_id=batch_id,
                created_at=unified.created_at,
                scraped_at=datetime.utcnow(),
            )
            db.merge(post)
            stored += 1

        job.items_scraped = stored
        job.status = "completed"
        job.completed_at = datetime.utcnow()
        db.commit()

    except Exception as e:
        job = db.query(ScrapeJob).filter(ScrapeJob.batch_id == batch_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)[:1000]
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()
