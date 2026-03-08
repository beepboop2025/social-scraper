"""pgvector operations — store and search embeddings for RAG."""

import json
import logging
import os
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


class VectorStore:
    """Manage pgvector embeddings for semantic search."""

    def __init__(self, dimension: int = 384):
        self.dimension = dimension

    def init_pgvector(self):
        """Initialize pgvector extension and create vector column + index."""
        from api.database import engine

        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            # Add vector column if it doesn't exist
            conn.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'article_embeddings'
                        AND column_name = 'embedding'
                    ) THEN
                        ALTER TABLE article_embeddings
                        ADD COLUMN embedding vector({self.dimension});
                    END IF;
                END $$;
            """))
            # Create IVFFlat index for fast search
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_embedding_cosine
                ON article_embeddings
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """))
            conn.commit()

    def store_embedding(self, article_id: int, embedding: list[float], model_name: str = "all-MiniLM-L6-v2"):
        """Store an embedding for an article."""
        from api.database import SessionLocal
        from storage.models import ArticleEmbedding

        db = SessionLocal()
        try:
            record = ArticleEmbedding(
                article_id=article_id,
                embedding_json=embedding,
                model_name=model_name,
            )
            db.add(record)
            db.commit()

            # Also update the pgvector column if available
            try:
                from api.database import engine
                with engine.connect() as conn:
                    vec_str = "[" + ",".join(str(x) for x in embedding) + "]"
                    conn.execute(text(
                        "UPDATE article_embeddings SET embedding = :vec WHERE id = :id"
                    ), {"vec": vec_str, "id": record.id})
                    conn.commit()
            except Exception:
                pass  # pgvector not available, JSON fallback works

        finally:
            db.close()

    def search_similar(self, query_embedding: list[float], limit: int = 10, filters: Optional[dict] = None) -> list[dict]:
        """Semantic search using pgvector cosine similarity."""
        from api.database import engine

        vec_str = "[" + ",".join(str(x) for x in query_embedding) + "]"

        filter_clause = ""
        params = {"vec": vec_str, "limit": limit}

        if filters:
            if filters.get("source"):
                filter_clause += " AND a.source = ANY(:sources)"
                params["sources"] = filters["source"]
            if filters.get("category"):
                filter_clause += " AND a.category = ANY(:categories)"
                params["categories"] = filters["category"]

        query = f"""
            SELECT
                a.id, a.title, a.url, a.source, a.category,
                a.published_at, a.full_text,
                1 - (ae.embedding <=> :vec::vector) AS similarity
            FROM article_embeddings ae
            JOIN articles a ON a.id = ae.article_id
            WHERE ae.embedding IS NOT NULL
            {filter_clause}
            ORDER BY ae.embedding <=> :vec::vector
            LIMIT :limit
        """

        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), params)
                rows = result.fetchall()
                return [
                    {
                        "id": r[0],
                        "title": r[1],
                        "url": r[2],
                        "source": r[3],
                        "category": r[4],
                        "published_at": r[5].isoformat() if r[5] else None,
                        "snippet": (r[6] or "")[:300],
                        "similarity": round(float(r[7]), 4),
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.warning(f"pgvector search failed, using JSON fallback: {e}")
            return self._json_fallback_search(query_embedding, limit)

    def _json_fallback_search(self, query_embedding: list[float], limit: int = 10) -> list[dict]:
        """Fallback search using JSON embeddings (slower, no pgvector needed)."""
        import numpy as np
        from api.database import SessionLocal
        from storage.models import ArticleEmbedding, Article

        db = SessionLocal()
        try:
            records = db.query(ArticleEmbedding).filter(ArticleEmbedding.embedding_json.isnot(None)).limit(1000).all()
            if not records:
                return []

            query_vec = np.array(query_embedding)
            scored = []
            for r in records:
                emb = np.array(r.embedding_json)
                sim = float(np.dot(query_vec, emb) / (np.linalg.norm(query_vec) * np.linalg.norm(emb) + 1e-8))
                scored.append((r.article_id, sim))

            scored.sort(key=lambda x: x[1], reverse=True)
            top_ids = [s[0] for s in scored[:limit]]

            articles = db.query(Article).filter(Article.id.in_(top_ids)).all()
            article_map = {a.id: a for a in articles}

            results = []
            for aid, sim in scored[:limit]:
                a = article_map.get(aid)
                if a:
                    results.append({
                        "id": a.id,
                        "title": a.title,
                        "url": a.url,
                        "source": a.source,
                        "category": a.category,
                        "published_at": a.published_at.isoformat() if a.published_at else None,
                        "snippet": (a.full_text or "")[:300],
                        "similarity": round(sim, 4),
                    })
            return results
        finally:
            db.close()
