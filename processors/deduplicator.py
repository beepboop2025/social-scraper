"""Content deduplication using URL hashing and text similarity.

Two-stage dedup:
1. Exact match via url_hash (SHA-256, fast)
2. Fuzzy match via text similarity (SequenceMatcher, for near-duplicates)

Marks duplicate articles with is_processed=True to skip downstream.
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class Deduplicator(BaseProcessor):
    name = "deduplicator"
    batch_size = 100

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.similarity_threshold = self.config.get("similarity_threshold", 0.85)
        self.window_hours = self.config.get("window_hours", 72)

    def process_one(self, article: dict) -> dict:
        return {"article_id": article.get("id"), "status": "use_batch"}

    def process_batch(self, articles: list[dict]) -> list[dict]:
        """Batch dedup is more efficient — compare all pairs within window."""
        results = []
        seen_hashes: set[str] = set()
        seen_texts: list[tuple[int, str]] = []

        for article in articles:
            article_id = article.get("id")
            url = article.get("url", "")
            text = article.get("full_text", "") or article.get("title", "")

            # Stage 1: URL hash dedup
            if url:
                url_hash = hashlib.sha256(url.encode()).hexdigest()[:32]
                if url_hash in seen_hashes:
                    results.append({
                        "article_id": article_id,
                        "status": "duplicate",
                        "reason": "url_hash",
                    })
                    continue
                seen_hashes.add(url_hash)

            # Stage 2: Text similarity dedup
            if text and len(text) > 50:
                is_dup = False
                text_snippet = text[:500]
                for existing_id, existing_text in seen_texts:
                    ratio = SequenceMatcher(None, text_snippet, existing_text).ratio()
                    if ratio >= self.similarity_threshold:
                        results.append({
                            "article_id": article_id,
                            "status": "duplicate",
                            "reason": "text_similarity",
                            "similarity": round(ratio, 3),
                            "duplicate_of": existing_id,
                        })
                        is_dup = True
                        break

                if is_dup:
                    continue
                seen_texts.append((article_id, text_snippet))

            results.append({"article_id": article_id, "status": "unique"})

        dup_count = sum(1 for r in results if r["status"] == "duplicate")
        logger.info(f"[Deduplicator] {dup_count}/{len(articles)} duplicates found")
        return results

    def run(self) -> dict:
        """Override run() to also check against DB for cross-batch dedup."""
        from datetime import datetime, timedelta, timezone

        from api.database import SessionLocal
        from storage.models import Article

        db = SessionLocal()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=self.window_hours)

            new_articles = (
                db.query(Article)
                .filter(Article.is_processed == False)
                .order_by(Article.collected_at.desc())
                .limit(self.batch_size)
                .all()
            )

            if not new_articles:
                return {"status": "no_work", "processed": 0}

            # Get existing url_hashes for cross-batch dedup
            existing_hashes = set()
            existing = (
                db.query(Article.url_hash)
                .filter(
                    Article.is_processed == True,
                    Article.collected_at >= cutoff,
                    Article.url_hash.isnot(None),
                )
                .all()
            )
            existing_hashes = {row[0] for row in existing}

            article_dicts = [
                {
                    "id": a.id,
                    "url": a.url,
                    "url_hash": a.url_hash,
                    "full_text": a.full_text or "",
                    "title": a.title or "",
                }
                for a in new_articles
            ]

            # Mark articles whose url_hash already exists in DB
            results = []
            remaining = []
            for ad in article_dicts:
                if ad.get("url_hash") and ad["url_hash"] in existing_hashes:
                    results.append({
                        "article_id": ad["id"],
                        "status": "duplicate",
                        "reason": "db_url_hash",
                    })
                else:
                    remaining.append(ad)

            # Run batch dedup on remaining
            batch_results = self.process_batch(remaining)
            results.extend(batch_results)

            self._store_results(results, db)
            return {
                "status": "success",
                "processed": len(results),
                "duplicates": sum(1 for r in results if r["status"] == "duplicate"),
            }
        finally:
            db.close()

    def _store_results(self, results: list[dict], db):
        from storage.models import Article

        dup_ids = [r["article_id"] for r in results if r["status"] == "duplicate"]
        if dup_ids:
            db.query(Article).filter(Article.id.in_(dup_ids)).update(
                {"is_processed": True}, synchronize_session=False
            )
            db.commit()
