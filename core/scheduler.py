"""Celery Beat scheduler — reads schedules from sources.yaml.

Dynamically generates the beat_schedule from config so adding a new
source only requires a sources.yaml entry, not code changes.
"""

import logging
import os

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery(
    "econscraper",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
    worker_concurrency=4,
    worker_max_tasks_per_child=200,
    task_default_retry_delay=30,
    task_max_retries=3,
    beat_schedule_filename="/tmp/econscraper-beat-schedule",
)


def _parse_cron(expr: str) -> crontab:
    """Parse a standard cron expression into a Celery crontab."""
    parts = expr.strip().split()
    if len(parts) != 5:
        return crontab(minute="0")

    return crontab(
        minute=parts[0],
        hour=parts[1],
        day_of_week=parts[4],
        day_of_month=parts[2],
        month_of_year=parts[3],
    )


def build_beat_schedule() -> dict:
    """Build Celery beat_schedule from sources.yaml."""
    from core.registry import load_sources_config

    sources = load_sources_config()
    schedule = {}

    for name, cfg in sources.items():
        if not cfg.get("enabled", True):
            continue

        cron_expr = cfg.get("schedule", "0 * * * *")

        schedule[f"collect-{name}"] = {
            "task": "core.tasks.run_collector",
            "schedule": _parse_cron(cron_expr),
            "args": [name],
            "options": {"queue": "collectors"},
        }

    # Processing pipeline — runs every 2 minutes
    schedule["process-articles"] = {
        "task": "core.tasks.process_pipeline",
        "schedule": crontab(minute="*/2"),
        "options": {"queue": "processors"},
    }

    # Daily digest — 9 PM IST (3:30 PM UTC)
    schedule["daily-digest"] = {
        "task": "core.tasks.generate_digest",
        "schedule": crontab(hour=15, minute=30),
        "options": {"queue": "processors"},
    }

    # Health check — every 5 minutes
    schedule["health-check"] = {
        "task": "core.tasks.health_check_all",
        "schedule": crontab(minute="*/5"),
        "options": {"queue": "health"},
    }

    # Data quality check — hourly
    schedule["data-quality"] = {
        "task": "core.tasks.check_data_quality",
        "schedule": crontab(minute=0),
        "options": {"queue": "health"},
    }

    # Route to DragonScope + LiquiFi — every 3 minutes
    schedule["route-to-destinations"] = {
        "task": "core.tasks.route_to_destinations",
        "schedule": crontab(minute="*/3"),
        "options": {"queue": "routing"},
    }

    return schedule


# Build and apply schedule
app.conf.beat_schedule = build_beat_schedule()

app.conf.task_routes = {
    "core.tasks.run_collector": {"queue": "collectors"},
    "core.tasks.process_pipeline": {"queue": "processors"},
    "core.tasks.generate_digest": {"queue": "processors"},
    "core.tasks.health_check_all": {"queue": "health"},
    "core.tasks.route_to_destinations": {"queue": "routing"},
}

app.autodiscover_tasks(["core"])
