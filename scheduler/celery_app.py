"""Celery application for 24/7 scraping orchestration."""

import os

from celery import Celery

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery(
    "social_scraper",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,

    # Results
    result_expires=3600,

    # Concurrency
    worker_concurrency=4,
    worker_max_tasks_per_child=200,

    # Retry
    task_default_retry_delay=30,
    task_max_retries=3,

    # Beat schedule
    beat_schedule_filename="/tmp/celerybeat-schedule",
)

# Auto-discover tasks
app.autodiscover_tasks(["scheduler"])
