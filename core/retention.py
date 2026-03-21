"""Data retention and archival — age-based cleanup with optional S3/MinIO export.

Retention policies:
- Articles: 90 days in hot storage (PostgreSQL), then archive
- Embeddings: 30 days (expensive storage)
- Sentiment scores: 180 days
- Collection logs: 30 days
- Digests: 365 days

Runs as a daily Celery task. Optionally exports to MinIO before deletion.
"""

import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, delete

logger = logging.getLogger(__name__)

# Retention periods in days
RETENTION_POLICIES = {
    "articles": 90,
    "article_embeddings": 30,
    "sentiment_scores": 180,
    "collection_logs": 30,
    "daily_digests": 365,
    "entities": 90,
    "article_topics": 90,
    "scraped_posts": 60,
    "analysis_results": 60,
    "scrape_jobs": 30,
}


class RetentionManager:
    """Manages data retention, archival, and cleanup."""

    def __init__(self, archive_to_s3: bool = False, s3_bucket: Optional[str] = None):
        self.archive_to_s3 = archive_to_s3 and bool(os.getenv("MINIO_ENDPOINT"))
        self.s3_bucket = s3_bucket or os.getenv("MINIO_ARCHIVE_BUCKET", "econscraper-archive")
        self._minio_client = None

    def _get_minio(self):
        """Lazy MinIO client init."""
        if self._minio_client is None and self.archive_to_s3:
            try:
                from minio import Minio
                self._minio_client = Minio(
                    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
                    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
                )
                # Ensure bucket exists
                if not self._minio_client.bucket_exists(self.s3_bucket):
                    self._minio_client.make_bucket(self.s3_bucket)
            except Exception as e:
                logger.warning(f"[Retention] MinIO init failed: {e}")
                self.archive_to_s3 = False
        return self._minio_client

    def _archive_to_minio(self, table_name: str, rows: list[dict], cutoff_date: str):
        """Export rows to MinIO as JSON before deletion."""
        if not self.archive_to_s3 or not rows:
            return False

        client = self._get_minio()
        if not client:
            return False

        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            key = f"archive/{table_name}/{cutoff_date}/{timestamp}.json"
            data = json.dumps(rows, default=str).encode("utf-8")
            client.put_object(
                self.s3_bucket,
                key,
                io.BytesIO(data),
                length=len(data),
                content_type="application/json",
            )
            logger.info(f"[Retention] Archived {len(rows)} {table_name} rows to {key}")
            return True
        except Exception as e:
            logger.error(f"[Retention] MinIO archive failed for {table_name}: {e}")
            return False

    def cleanup_articles(self, db) -> dict:
        """Clean up articles older than retention period."""
        from storage.models import Article

        days = RETENTION_POLICIES["articles"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Count
        count = db.query(func.count(Article.id)).filter(
            Article.collected_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "articles", "deleted": 0}

        # Archive if configured
        if self.archive_to_s3:
            old_articles = db.query(Article).filter(Article.collected_at < cutoff).limit(5000).all()
            rows = [
                {
                    "id": a.id, "source": a.source, "title": a.title,
                    "url": a.url, "published_at": str(a.published_at),
                    "collected_at": str(a.collected_at), "category": a.category,
                }
                for a in old_articles
            ]
            self._archive_to_minio("articles", rows, cutoff.strftime("%Y-%m-%d"))

        # Delete (cascade will handle embeddings, sentiments, entities, topics)
        db.query(Article).filter(Article.collected_at < cutoff).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} articles older than {days} days")
        return {"table": "articles", "deleted": count, "cutoff": cutoff.isoformat()}

    def cleanup_embeddings(self, db) -> dict:
        """Clean up orphaned or old embeddings."""
        from storage.models import ArticleEmbedding

        days = RETENTION_POLICIES["article_embeddings"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        count = db.query(func.count(ArticleEmbedding.id)).filter(
            ArticleEmbedding.created_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "article_embeddings", "deleted": 0}

        db.query(ArticleEmbedding).filter(
            ArticleEmbedding.created_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} embeddings older than {days} days")
        return {"table": "article_embeddings", "deleted": count}

    def cleanup_sentiments(self, db) -> dict:
        """Clean up old sentiment scores."""
        from storage.models import SentimentScore

        days = RETENTION_POLICIES["sentiment_scores"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        count = db.query(func.count(SentimentScore.id)).filter(
            SentimentScore.created_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "sentiment_scores", "deleted": 0}

        db.query(SentimentScore).filter(
            SentimentScore.created_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} sentiment scores older than {days} days")
        return {"table": "sentiment_scores", "deleted": count}

    def cleanup_collection_logs(self, db) -> dict:
        """Clean up old collection logs."""
        from storage.models import CollectionLog

        days = RETENTION_POLICIES["collection_logs"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        count = db.query(func.count(CollectionLog.id)).filter(
            CollectionLog.run_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "collection_logs", "deleted": 0}

        db.query(CollectionLog).filter(
            CollectionLog.run_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} collection logs older than {days} days")
        return {"table": "collection_logs", "deleted": count}

    def cleanup_digests(self, db) -> dict:
        """Clean up old daily digests."""
        from storage.models import DailyDigest

        days = RETENTION_POLICIES["daily_digests"]
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()

        count = db.query(func.count(DailyDigest.id)).filter(
            DailyDigest.date < cutoff_date
        ).scalar()

        if count == 0:
            return {"table": "daily_digests", "deleted": 0}

        db.query(DailyDigest).filter(
            DailyDigest.date < cutoff_date
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} digests older than {days} days")
        return {"table": "daily_digests", "deleted": count}

    def cleanup_scraped_posts(self, db) -> dict:
        """Clean up old scraped posts from the social pipeline."""
        from api.models import ScrapedPost

        days = RETENTION_POLICIES["scraped_posts"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        count = db.query(func.count(ScrapedPost.id)).filter(
            ScrapedPost.scraped_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "scraped_posts", "deleted": 0}

        db.query(ScrapedPost).filter(
            ScrapedPost.scraped_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} scraped posts older than {days} days")
        return {"table": "scraped_posts", "deleted": count}

    def cleanup_scrape_jobs(self, db) -> dict:
        """Clean up old scrape job records."""
        from api.models import ScrapeJob

        days = RETENTION_POLICIES["scrape_jobs"]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        count = db.query(func.count(ScrapeJob.id)).filter(
            ScrapeJob.created_at < cutoff
        ).scalar()

        if count == 0:
            return {"table": "scrape_jobs", "deleted": 0}

        db.query(ScrapeJob).filter(
            ScrapeJob.created_at < cutoff
        ).delete(synchronize_session=False)
        db.commit()

        logger.info(f"[Retention] Deleted {count} scrape jobs older than {days} days")
        return {"table": "scrape_jobs", "deleted": count}

    def run_all(self) -> dict:
        """Run all retention cleanup tasks. Returns summary."""
        from api.database import SessionLocal

        db = SessionLocal()
        results = {"timestamp": datetime.now(timezone.utc).isoformat(), "tables": {}}

        try:
            cleanup_methods = [
                self.cleanup_articles,
                self.cleanup_embeddings,
                self.cleanup_sentiments,
                self.cleanup_collection_logs,
                self.cleanup_digests,
                self.cleanup_scraped_posts,
                self.cleanup_scrape_jobs,
            ]

            total_deleted = 0
            for method in cleanup_methods:
                try:
                    result = method(db)
                    table = result.get("table", "unknown")
                    results["tables"][table] = result
                    total_deleted += result.get("deleted", 0)
                except Exception as e:
                    logger.error(f"[Retention] {method.__name__} failed: {e}")
                    results["tables"][method.__name__] = {"error": str(e)}

            results["total_deleted"] = total_deleted

            # Update table statistics after cleanup
            try:
                db.execute("ANALYZE articles")
                db.execute("ANALYZE collection_logs")
            except Exception:
                pass  # ANALYZE may not work in all setups

            logger.info(f"[Retention] Cleanup complete: {total_deleted} rows deleted")
        finally:
            db.close()

        return results
