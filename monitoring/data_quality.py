"""Data quality checks — detect schema changes, stale data, anomalies."""

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Run periodic data quality validations."""

    def __init__(self):
        self.staleness_hours = {
            # Structured collectors
            "fred_api": 24,
            "rbi_dbie": 48,
            "rbi_circulars": 12,
            "nse_bhavcopy": 24,
            "bse_api": 24,
            "ccil_rates": 12,
            "data_gov_in": 48,
            "sebi_circulars": 24,
            "world_bank": 168,  # Weekly
            "imf_data": 744,   # Monthly
            # Social/news collectors
            "rss_feeds": 6,
            "twitter_lists": 6,
            "telegram_channels": 6,
        }

    def run_all_checks(self) -> list[dict]:
        """Run all quality checks, return list of issues."""
        issues = []
        issues.extend(self.check_staleness())
        issues.extend(self.check_empty_fields())
        issues.extend(self.check_duplicate_rate())
        return issues

    def check_staleness(self) -> list[dict]:
        """Check if any source has stopped producing data."""
        from api.database import SessionLocal
        from storage.models import CollectionLog

        issues = []
        db = SessionLocal()
        try:
            for source, max_hours in self.staleness_hours.items():
                cutoff = datetime.now(timezone.utc) - timedelta(hours=max_hours)
                recent = (
                    db.query(CollectionLog)
                    .filter(
                        CollectionLog.source == source,
                        CollectionLog.status == "success",
                        CollectionLog.run_at >= cutoff,
                    )
                    .first()
                )
                if not recent:
                    issues.append({
                        "type": "stale_data",
                        "source": source,
                        "severity": "warning",
                        "message": f"No successful collection in {max_hours}h",
                    })
        finally:
            db.close()
        return issues

    def check_empty_fields(self) -> list[dict]:
        """Check for articles with missing critical fields."""
        from sqlalchemy import func

        from api.database import SessionLocal
        from storage.models import Article

        issues = []
        db = SessionLocal()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

            no_text = (
                db.query(func.count(Article.id))
                .filter(
                    Article.collected_at >= cutoff,
                    (Article.full_text.is_(None)) | (Article.full_text == ""),
                )
                .scalar()
            )

            total = (
                db.query(func.count(Article.id))
                .filter(Article.collected_at >= cutoff)
                .scalar()
            )

            if total and (no_text / total) > 0.5:
                issues.append({
                    "type": "empty_fields",
                    "severity": "warning",
                    "message": f"{no_text}/{total} articles missing full_text in last 24h",
                })
        finally:
            db.close()
        return issues

    def check_duplicate_rate(self) -> list[dict]:
        """Check if duplicate rate is abnormally high."""
        from sqlalchemy import func

        from api.database import SessionLocal
        from storage.models import Article

        issues = []
        db = SessionLocal()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

            total = (
                db.query(func.count(Article.id))
                .filter(Article.collected_at >= cutoff)
                .scalar()
            )

            unique_urls = (
                db.query(func.count(func.distinct(Article.url_hash)))
                .filter(Article.collected_at >= cutoff)
                .scalar()
            )

            if total and total > 10:
                dup_rate = 1 - (unique_urls / total)
                if dup_rate > 0.7:
                    issues.append({
                        "type": "high_duplication",
                        "severity": "warning",
                        "message": f"{dup_rate:.0%} duplicate rate in last 24h ({total} total, {unique_urls} unique)",
                    })
        finally:
            db.close()
        return issues
