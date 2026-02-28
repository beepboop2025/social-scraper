"""Search API routes — full-text search across scraped data."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import or_, func
from typing import Optional

from api.database import get_db
from api.models import ScrapedPost

router = APIRouter(prefix="/search", tags=["search"])


@router.get("/posts")
def search_posts(
    q: str = Query(..., min_length=1, description="Search query"),
    platform: Optional[str] = None,
    author: Optional[str] = None,
    hashtag: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort_by: str = "relevance",
    limit: int = 20,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    """Full-text search across scraped posts."""
    query = db.query(ScrapedPost)

    # Text search (ILIKE for PostgreSQL)
    query = query.filter(
        or_(
            ScrapedPost.text.ilike(f"%{q}%"),
            ScrapedPost.author_username.ilike(f"%{q}%"),
            ScrapedPost.source_channel.ilike(f"%{q}%"),
        )
    )

    # Filters
    if platform:
        query = query.filter(ScrapedPost.platform == platform)
    if author:
        query = query.filter(ScrapedPost.author_username.ilike(f"%{author}%"))
    if hashtag:
        # Search within JSON array
        query = query.filter(ScrapedPost.hashtags.contains([hashtag]))
    if date_from:
        query = query.filter(ScrapedPost.created_at >= date_from)
    if date_to:
        query = query.filter(ScrapedPost.created_at <= date_to)

    # Count total matches
    total = query.count()

    # Sorting
    if sort_by == "date":
        query = query.order_by(ScrapedPost.created_at.desc())
    elif sort_by == "engagement":
        query = query.order_by((ScrapedPost.likes + ScrapedPost.reposts + ScrapedPost.replies).desc())
    else:
        # Relevance: prioritize by engagement + recency
        query = query.order_by(ScrapedPost.created_at.desc())

    posts = query.offset(offset).limit(min(limit, 100)).all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": [
            {
                "id": p.id,
                "platform": p.platform,
                "text": p.text[:500] if p.text else "",
                "author_username": p.author_username,
                "author_display_name": p.author_display_name,
                "likes": p.likes,
                "reposts": p.reposts,
                "replies": p.replies,
                "views": p.views,
                "hashtags": p.hashtags,
                "source_url": p.source_url,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in posts
        ],
    }


@router.get("/trending")
def trending_hashtags(hours: int = 24, limit: int = 20, db: Session = Depends(get_db)):
    """Get trending hashtags from recent posts."""
    # Use a raw query for JSON unnesting in PostgreSQL
    from sqlalchemy import text
    result = db.execute(
        text("""
            SELECT tag, COUNT(*) AS count
            FROM scraped_posts,
                 jsonb_array_elements_text(hashtags::jsonb) AS tag
            WHERE scraped_at >= NOW() - make_interval(hours => :hours)
            GROUP BY tag
            ORDER BY count DESC
            LIMIT :limit
        """),
        {"hours": hours, "limit": limit},
    )
    return [{"hashtag": row.tag, "count": row.count} for row in result]


@router.get("/stats")
def search_stats(db: Session = Depends(get_db)):
    """Get overall scraping statistics."""
    total_posts = db.query(ScrapedPost).count()
    by_platform = (
        db.query(ScrapedPost.platform, func.count())
        .group_by(ScrapedPost.platform)
        .all()
    )
    return {
        "total_posts": total_posts,
        "by_platform": {p: c for p, c in by_platform},
    }
