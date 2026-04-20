"""Prometheus-compatible metrics export for econscraper.

Metrics:
- scraper_articles_total (counter, by source)
- scraper_errors_total (counter, by source, error_type)
- scraper_duration_seconds (histogram, by source)
- nlp_processing_duration_seconds (histogram)
- sentiment_score_distribution (histogram)
- queue_depth (gauge, by queue_name)
- articles_quality_score (histogram)
- connector_push_total (counter, by destination)

Exposes /metrics endpoint in Prometheus text format.
"""

import logging
import os
import time
import threading
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
METRICS_PREFIX = "metrics:"


class _Counter:
    """Thread-safe counter metric."""

    def __init__(self, name: str, help_text: str, labels: list[str]):
        self.name = name
        self.help = help_text
        self.labels = labels
        self._values: dict[tuple, float] = {}
        self._lock = threading.Lock()

    def inc(self, value: float = 1.0, **label_values):
        key = tuple(label_values.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + value

    def collect(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} counter"]
        with self._lock:
            for key, val in sorted(self._values.items()):
                label_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key) if v)
                if label_str:
                    lines.append(f"{self.name}{{{label_str}}} {val}")
                else:
                    lines.append(f"{self.name} {val}")
        return lines


class _Gauge:
    """Thread-safe gauge metric."""

    def __init__(self, name: str, help_text: str, labels: list[str]):
        self.name = name
        self.help = help_text
        self.labels = labels
        self._values: dict[tuple, float] = {}
        self._lock = threading.Lock()

    def set(self, value: float, **label_values):
        key = tuple(label_values.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] = value

    def collect(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} gauge"]
        with self._lock:
            for key, val in sorted(self._values.items()):
                label_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key) if v)
                if label_str:
                    lines.append(f"{self.name}{{{label_str}}} {val}")
                else:
                    lines.append(f"{self.name} {val}")
        return lines


class _Histogram:
    """Thread-safe histogram metric with pre-defined buckets."""

    DEFAULT_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, float("inf"))

    def __init__(self, name: str, help_text: str, labels: list[str], buckets=None):
        self.name = name
        self.help = help_text
        self.labels = labels
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._data: dict[tuple, dict] = {}
        self._lock = threading.Lock()

    def observe(self, value: float, **label_values):
        key = tuple(label_values.get(l, "") for l in self.labels)
        with self._lock:
            if key not in self._data:
                self._data[key] = {
                    "buckets": {b: 0 for b in self.buckets},
                    "sum": 0.0,
                    "count": 0,
                }
            entry = self._data[key]
            entry["sum"] += value
            entry["count"] += 1
            for b in self.buckets:
                if value <= b:
                    entry["buckets"][b] += 1

    def collect(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} histogram"]
        with self._lock:
            for key, data in sorted(self._data.items()):
                label_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key) if v)
                prefix = f"{self.name}{{{label_str}," if label_str else f"{self.name}{{"
                for b, count in sorted(data["buckets"].items()):
                    le = "+Inf" if b == float("inf") else str(b)
                    lines.append(f"{prefix}le=\"{le}\"}} {count}")
                if label_str:
                    lines.append(f"{self.name}_sum{{{label_str}}} {data['sum']}")
                    lines.append(f"{self.name}_count{{{label_str}}} {data['count']}")
                else:
                    lines.append(f"{self.name}_sum {data['sum']}")
                    lines.append(f"{self.name}_count {data['count']}")
        return lines


class MetricsRegistry:
    """Central registry for all application metrics."""

    def __init__(self):
        # Counters
        self.scraper_articles_total = _Counter(
            "scraper_articles_total",
            "Total articles scraped by source",
            ["source"],
        )
        self.scraper_errors_total = _Counter(
            "scraper_errors_total",
            "Total scraper errors by source and error type",
            ["source", "error_type"],
        )
        self.connector_push_total = _Counter(
            "connector_push_total",
            "Total items pushed to destinations",
            ["destination"],
        )
        self.webhook_deliveries_total = _Counter(
            "webhook_deliveries_total",
            "Total webhook deliveries by status",
            ["status"],
        )

        # Gauges
        self.queue_depth = _Gauge(
            "queue_depth",
            "Current queue depth by queue name",
            ["queue_name"],
        )
        self.active_scrapers = _Gauge(
            "active_scrapers",
            "Number of currently active scrapers",
            [],
        )
        self.backpressure_level = _Gauge(
            "backpressure_level",
            "Current backpressure level (0=normal, 1=warn, 2=critical)",
            [],
        )

        # Histograms
        self.scraper_duration_seconds = _Histogram(
            "scraper_duration_seconds",
            "Scraper execution duration in seconds",
            ["source"],
        )
        self.nlp_processing_duration_seconds = _Histogram(
            "nlp_processing_duration_seconds",
            "NLP processing duration in seconds",
            ["processor"],
        )
        self.sentiment_score_distribution = _Histogram(
            "sentiment_score_distribution",
            "Distribution of sentiment scores",
            [],
            buckets=(-1.0, -0.5, -0.2, 0.0, 0.2, 0.5, 1.0, float("inf")),
        )
        self.articles_quality_score = _Histogram(
            "articles_quality_score",
            "Distribution of article quality scores",
            [],
            buckets=(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, float("inf")),
        )

        self._all_metrics = [
            self.scraper_articles_total,
            self.scraper_errors_total,
            self.connector_push_total,
            self.webhook_deliveries_total,
            self.queue_depth,
            self.active_scrapers,
            self.backpressure_level,
            self.scraper_duration_seconds,
            self.nlp_processing_duration_seconds,
            self.sentiment_score_distribution,
            self.articles_quality_score,
        ]

    def collect_all(self) -> str:
        """Collect all metrics in Prometheus text exposition format."""
        lines = []
        for metric in self._all_metrics:
            lines.extend(metric.collect())
            lines.append("")  # Blank line between metrics
        return "\n".join(lines) + "\n"

    def update_queue_depths(self):
        """Refresh queue depth gauges from Redis."""
        try:
            import redis
            r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
            try:
                for q in ["collectors", "processors", "routing", "health", "celery"]:
                    try:
                        depth = r.llen(q)
                        self.queue_depth.set(depth, queue_name=q)
                    except Exception:
                        pass
            finally:
                r.close()
        except Exception as e:
            logger.debug(f"[Metrics] Queue depth refresh failed: {e}")

    def update_from_db(self):
        """Refresh metrics from database (called periodically).

        Uses a high-water mark to avoid double-counting: only articles/errors
        with IDs greater than the last-seen ID are counted.
        """
        try:
            from api.database import SessionLocal
            from storage.models import Article, CollectionLog
            from sqlalchemy import func
            from datetime import timedelta

            db = SessionLocal()
            try:
                # Articles by source — only count rows newer than our high-water mark
                last_article_id = getattr(self, "_last_article_id", 0)
                source_counts = (
                    db.query(Article.source, func.count().label("count"), func.max(Article.id).label("max_id"))
                    .filter(Article.id > last_article_id)
                    .group_by(Article.source)
                    .all()
                )
                max_seen = last_article_id
                for source, count, max_id in source_counts:
                    self.scraper_articles_total.inc(count, source=source)
                    if max_id and max_id > max_seen:
                        max_seen = max_id
                self._last_article_id = max_seen

                # Errors by source — same high-water mark approach
                last_log_id = getattr(self, "_last_error_log_id", 0)
                error_counts = (
                    db.query(CollectionLog.source, func.count().label("count"), func.max(CollectionLog.id).label("max_id"))
                    .filter(
                        CollectionLog.id > last_log_id,
                        CollectionLog.status == "failed",
                    )
                    .group_by(CollectionLog.source)
                    .all()
                )
                max_err = last_log_id
                for source, count, max_id in error_counts:
                    self.scraper_errors_total.inc(count, source=source, error_type="task_failure")
                    if max_id and max_id > max_err:
                        max_err = max_id
                self._last_error_log_id = max_err
            finally:
                db.close()
        except Exception as e:
            logger.debug(f"[Metrics] DB refresh failed: {e}")


# Singleton registry
_registry: Optional[MetricsRegistry] = None


def get_metrics_registry() -> MetricsRegistry:
    """Get or create the singleton metrics registry."""
    global _registry
    if _registry is None:
        _registry = MetricsRegistry()
    return _registry


# ── Grafana Dashboard Template ──────────────────────────────

GRAFANA_DASHBOARD_JSON = {
    "dashboard": {
        "title": "EconScraper Metrics",
        "panels": [
            {
                "title": "Articles Scraped (by source)",
                "type": "timeseries",
                "targets": [{"expr": "rate(scraper_articles_total[5m])", "legendFormat": "{{source}}"}],
            },
            {
                "title": "Scraper Errors",
                "type": "timeseries",
                "targets": [{"expr": "rate(scraper_errors_total[5m])", "legendFormat": "{{source}}: {{error_type}}"}],
            },
            {
                "title": "Queue Depths",
                "type": "gauge",
                "targets": [{"expr": "queue_depth", "legendFormat": "{{queue_name}}"}],
            },
            {
                "title": "Scraper Duration (p95)",
                "type": "timeseries",
                "targets": [{"expr": "histogram_quantile(0.95, rate(scraper_duration_seconds_bucket[5m]))", "legendFormat": "{{source}}"}],
            },
            {
                "title": "Sentiment Distribution",
                "type": "histogram",
                "targets": [{"expr": "sentiment_score_distribution_bucket"}],
            },
            {
                "title": "Quality Score Distribution",
                "type": "histogram",
                "targets": [{"expr": "articles_quality_score_bucket"}],
            },
            {
                "title": "Connector Push Rate",
                "type": "timeseries",
                "targets": [{"expr": "rate(connector_push_total[5m])", "legendFormat": "{{destination}}"}],
            },
            {
                "title": "Backpressure Level",
                "type": "stat",
                "targets": [{"expr": "backpressure_level"}],
            },
        ],
    }
}
