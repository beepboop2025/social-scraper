"""Celery application for 24/7 scraping orchestration.

Re-exports the app from core.scheduler to avoid duplicate Celery instances.
"""

from core.scheduler import app

__all__ = ["app"]
