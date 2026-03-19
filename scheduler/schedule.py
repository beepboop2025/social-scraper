"""DEPRECATED — Legacy beat schedule.

The canonical schedule is now built dynamically in core/scheduler.py via
build_beat_schedule(), which reads sources.yaml.  This file is kept only
as a reference; importing it no longer mutates the Celery app config.

To add or change a schedule, edit config/sources.yaml (for YAML-driven
collectors) or the social scraper block in core/scheduler.py.
"""

import logging
import warnings

warnings.warn(
    "scheduler.schedule is deprecated — the beat schedule is now managed by "
    "core.scheduler.build_beat_schedule(). This import is a no-op.",
    DeprecationWarning,
    stacklevel=2,
)
logging.getLogger(__name__).warning(
    "scheduler.schedule imported but is deprecated. "
    "Beat schedule is managed by core.scheduler."
)

# ── Reference only — the schedule that was previously defined here ──
# See core/scheduler.py build_beat_schedule() for the live schedule.
#
# Frequency tiers:
# - CRITICAL (2min):  Central banks, RSS financial news
# - HIGH (5min):      Reddit, Twitter, Telegram
# - MEDIUM (15min):   Hacker News, YouTube, Mastodon, Web
# - LOW (30min):      GitHub, SEC EDGAR, Discord
# - BACKGROUND (1hr): Dark web, full RSS sweep
# - DAILY (6hr):      GitHub trending, comprehensive SEC scan
