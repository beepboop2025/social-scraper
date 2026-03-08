"""Beat schedule — defines when each scraper runs.

Frequency tiers:
- CRITICAL (2min):  Central banks, RSS financial news
- HIGH (5min):      Reddit financial subs, Twitter queries, Telegram
- MEDIUM (15min):   Hacker News, YouTube, Mastodon, Web scraping
- LOW (30min):      GitHub, SEC EDGAR, Discord
- BACKGROUND (1hr): Dark web threat intel, full RSS sweep
- DAILY (6hr):      GitHub trending, comprehensive SEC scan

All tasks route through the DataRouter to DragonScope + LiquiFi.
"""

from celery.schedules import crontab
from scheduler.celery_app import app

app.conf.beat_schedule = {
    # ── CRITICAL: Every 2 minutes ─────────────────────────────
    "scrape-central-banks": {
        "task": "scheduler.tasks.scrape_central_banks",
        "schedule": 120.0,  # 2min
        "options": {"queue": "critical"},
    },
    "scrape-rss-financial": {
        "task": "scheduler.tasks.scrape_rss_financial",
        "schedule": 120.0,
        "options": {"queue": "critical"},
    },

    # ── HIGH: Every 5 minutes ─────────────────────────────────
    "scrape-reddit-financial": {
        "task": "scheduler.tasks.scrape_reddit",
        "schedule": 300.0,  # 5min
        "options": {"queue": "high"},
    },
    "scrape-twitter-queries": {
        "task": "scheduler.tasks.scrape_twitter",
        "schedule": 300.0,
        "options": {"queue": "high"},
    },
    "scrape-telegram-channels": {
        "task": "scheduler.tasks.scrape_telegram",
        "schedule": 300.0,
        "options": {"queue": "high"},
    },

    # ── MEDIUM: Every 15 minutes ──────────────────────────────
    "scrape-hackernews": {
        "task": "scheduler.tasks.scrape_hackernews",
        "schedule": 900.0,  # 15min
        "options": {"queue": "default"},
    },
    "scrape-youtube-financial": {
        "task": "scheduler.tasks.scrape_youtube",
        "schedule": 900.0,
        "options": {"queue": "default"},
    },
    "scrape-mastodon": {
        "task": "scheduler.tasks.scrape_mastodon",
        "schedule": 900.0,
        "options": {"queue": "default"},
    },
    "scrape-web-targets": {
        "task": "scheduler.tasks.scrape_web",
        "schedule": 900.0,
        "options": {"queue": "default"},
    },

    # ── LOW: Every 30 minutes ─────────────────────────────────
    "scrape-github-monitored": {
        "task": "scheduler.tasks.scrape_github",
        "schedule": 1800.0,  # 30min
        "options": {"queue": "default"},
    },
    "scrape-sec-filings": {
        "task": "scheduler.tasks.scrape_sec",
        "schedule": 1800.0,
        "options": {"queue": "default"},
    },
    "scrape-discord-channels": {
        "task": "scheduler.tasks.scrape_discord",
        "schedule": 1800.0,
        "options": {"queue": "default"},
    },

    # ── BACKGROUND: Every hour ────────────────────────────────
    "scrape-darkweb-threatintel": {
        "task": "scheduler.tasks.scrape_darkweb",
        "schedule": 3600.0,  # 1hr
        "options": {"queue": "background"},
    },
    "scrape-rss-all-feeds": {
        "task": "scheduler.tasks.scrape_rss_all",
        "schedule": 3600.0,
        "options": {"queue": "background"},
    },

    # ── DAILY: Every 6 hours ──────────────────────────────────
    "scrape-github-trending": {
        "task": "scheduler.tasks.scrape_github_trending",
        "schedule": 21600.0,  # 6hr
        "options": {"queue": "background"},
    },
    "scrape-sec-comprehensive": {
        "task": "scheduler.tasks.scrape_sec_comprehensive",
        "schedule": 21600.0,
        "options": {"queue": "background"},
    },

    # ── HEALTH: Every 5 minutes ───────────────────────────────
    "health-check-all-scrapers": {
        "task": "scheduler.tasks.health_check",
        "schedule": 300.0,
        "options": {"queue": "health"},
    },
    "push-stats-to-destinations": {
        "task": "scheduler.tasks.push_stats",
        "schedule": 600.0,  # 10min
        "options": {"queue": "health"},
    },
}

# Queue routing
app.conf.task_routes = {
    "scheduler.tasks.scrape_central_banks": {"queue": "critical"},
    "scheduler.tasks.scrape_rss_financial": {"queue": "critical"},
    "scheduler.tasks.scrape_reddit": {"queue": "high"},
    "scheduler.tasks.scrape_twitter": {"queue": "high"},
    "scheduler.tasks.scrape_telegram": {"queue": "high"},
    "scheduler.tasks.health_check": {"queue": "health"},
    "scheduler.tasks.push_stats": {"queue": "health"},
}
