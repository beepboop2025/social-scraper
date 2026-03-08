"""Semantic search API — pgvector cosine similarity on article embeddings."""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])


@router.get("/semantic")
async def semantic_search(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    source: Optional[str] = None,
    category: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Semantic search using vector embeddings.

    Embeds the query, then finds the most similar articles
    via pgvector cosine distance.
    """
    from api.routes.deps import get_embedder, get_vector_store

    embedder = get_embedder()
    vector_store = get_vector_store()

    query_embedding = embedder._embed_text(q)
    if not query_embedding:
        return {"results": [], "error": "Failed to embed query"}

    filters = {}
    if source:
        filters["source"] = [source]
    if category:
        filters["category"] = [category]

    results = vector_store.search_similar(
        query_embedding=query_embedding,
        limit=limit,
        filters=filters if filters else None,
    )

    return {"query": q, "count": len(results), "results": results}


@router.get("/keyword")
async def keyword_search(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    source: Optional[str] = None,
    category: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Full-text keyword search across articles."""
    from storage.models import Article

    query = db.query(Article).filter(
        Article.full_text.ilike(f"%{q}%") | Article.title.ilike(f"%{q}%")
    )

    if source:
        query = query.filter(Article.source == source)
    if category:
        query = query.filter(Article.category == category)

    articles = query.order_by(Article.published_at.desc()).limit(limit).all()

    return {
        "query": q,
        "count": len(articles),
        "results": [
            {
                "id": a.id,
                "title": a.title,
                "url": a.url,
                "source": a.source,
                "category": a.category,
                "published_at": a.published_at.isoformat() if a.published_at else None,
                "snippet": (a.full_text or "")[:300],
            }
            for a in articles
        ],
    }
