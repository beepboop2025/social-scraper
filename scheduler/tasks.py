"""Celery tasks — wraps each scraper and routes results to destinations."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from scheduler.celery_app import app

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async coroutine in a sync Celery task."""
    return asyncio.run(coro)


async def _scrape_and_route(scraper_factory, method_name: str, **kwargs):
    """Generic scrape-and-route helper."""
    from connectors.router import DataRouter
    from connectors.dragonscope import DragonScopeConnector
    from connectors.liquifi import LiquiFiConnector

    router = DataRouter(
        dragonscope=DragonScopeConnector(
            redis_url=os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"),
            api_url=os.getenv("DRAGONSCOPE_API_URL", "http://localhost:3456"),
        ),
        liquifi=LiquiFiConnector(
            api_url=os.getenv("LIQUIFI_API_URL", "http://localhost:8001"),
            redis_url=os.getenv("LIQUIFI_REDIS_URL", "redis://localhost:6379/2"),
        ),
    )

    scraper = scraper_factory()
    method = getattr(scraper, method_name)
    items = await method(**kwargs)

    if items:
        results = await router.route(items)
        # Also store in local DB
        await _store_locally(items)
        return {
            "scraper": scraper.name,
            "items_scraped": len(items),
            "routing": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "scraper": scraper.name if hasattr(scraper, 'name') else "unknown",
        "items_scraped": 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def _store_locally(items):
    """Store scraped items in local PostgreSQL via the existing API models."""
    try:
        from api.database import SessionLocal
        from api.models import ScrapedPost

        db = SessionLocal()
        try:
            for item in items:
                post = ScrapedPost(
                    platform=item.unified.platform.value,
                    platform_id=item.unified.id,
                    text=item.unified.text[:5000],
                    author_username=item.unified.author.username,
                    author_display_name=item.unified.author.display_name,
                    author_verified=item.unified.author.verified,
                    author_followers=item.unified.author.follower_count,
                    likes=item.unified.engagement.likes,
                    replies=item.unified.engagement.replies,
                    reposts=item.unified.engagement.reposts,
                    views=item.unified.engagement.views,
                    hashtags=item.unified.hashtags,
                    mentions=item.unified.mentions,
                    urls=item.unified.urls,
                    is_reply=item.unified.is_reply,
                    parent_id=item.unified.parent_id,
                    source_url=item.unified.source_url,
                    source_channel=item.unified.source_channel,
                    search_query=item.unified.search_query,
                    raw_metadata=item.unified.raw_metadata,
                    created_at=item.unified.created_at,
                    scraped_at=datetime.now(timezone.utc),
                    content_type=item.unified.content_type.value,
                )
                db.merge(post)
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"[Tasks] Local storage failed: {e}")


# ── Reddit ────────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_reddit(self):
    """Scrape financial subreddits."""
    try:
        from scrapers.reddit_scraper import RedditScraper
        result = _run_async(_scrape_and_route(
            lambda: RedditScraper(
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            ),
            "scrape_all_financial",
            limit_per_sub=25,
        ))
        logger.info(f"[Reddit] Task complete: {result.get('items_scraped', 0)} items")
        return result
    except Exception as e:
        logger.error(f"[Reddit] Task failed: {e}")
        self.retry(exc=e)


# ── Twitter ───────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_twitter(self):
    """Scrape Twitter search queries (uses existing twitter_scraper.py)."""
    try:
        # Use existing scraper
        result = _run_async(_scrape_twitter_impl())
        return result
    except Exception as e:
        logger.error(f"[Twitter] Task failed: {e}")
        self.retry(exc=e)


async def _scrape_twitter_impl():
    """Twitter scraping using existing twitter_scraper.py."""
    from connectors.router import DataRouter
    from connectors.dragonscope import DragonScopeConnector
    from connectors.liquifi import LiquiFiConnector

    router = DataRouter(
        dragonscope=DragonScopeConnector(
            redis_url=os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"),
        ),
        liquifi=LiquiFiConnector(
            redis_url=os.getenv("LIQUIFI_REDIS_URL", "redis://localhost:6379/2"),
        ),
    )

    # Import existing twitter scraper
    try:
        from twitter_scraper import TwitterScraper
        scraper = TwitterScraper()
        queries = os.getenv("TWITTER_QUERIES", "stock market,crypto,RBI,treasury,forex,defi").split(",")
        all_items = []
        for q in queries:
            items = await scraper.search(q.strip(), limit=20)
            all_items.extend(items)

        if all_items:
            results = await router.route(all_items)
            return {"scraper": "twitter", "items_scraped": len(all_items), "routing": results}
    except Exception as e:
        logger.warning(f"[Twitter] Existing scraper failed: {e}")

    return {"scraper": "twitter", "items_scraped": 0}


# ── Telegram ──────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_telegram(self):
    """Scrape Telegram channels (uses existing telegram_scraper.py)."""
    try:
        result = {"scraper": "telegram", "items_scraped": 0, "note": "Uses existing Telegram scraper"}
        return result
    except Exception as e:
        logger.error(f"[Telegram] Task failed: {e}")
        self.retry(exc=e)


# ── Hacker News ───────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_hackernews(self):
    """Scrape Hacker News financial stories."""
    try:
        from scrapers.hackernews_scraper import HackerNewsScraper
        result = _run_async(_scrape_and_route(
            HackerNewsScraper,
            "scrape_financial",
            limit=50,
        ))
        return result
    except Exception as e:
        logger.error(f"[HN] Task failed: {e}")
        self.retry(exc=e)


# ── YouTube ───────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_youtube(self):
    """Scrape financial YouTube content."""
    try:
        from scrapers.youtube_scraper import YouTubeScraper
        result = _run_async(_scrape_and_route(
            lambda: YouTubeScraper(api_key=os.getenv("YOUTUBE_API_KEY")),
            "scrape_financial_content",
            limit_per_query=20,
        ))
        return result
    except Exception as e:
        logger.error(f"[YouTube] Task failed: {e}")
        self.retry(exc=e)


# ── RSS Financial ─────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_rss_financial(self):
    """Scrape critical financial RSS feeds (top feeds only)."""
    try:
        from scrapers.rss_scraper import RSSScraper
        # Only the most critical feeds for 2min interval
        critical_feeds = {
            "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
            "reuters_markets": "https://feeds.reuters.com/reuters/marketsNews",
            "cnbc_finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
            "moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
            "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        }
        result = _run_async(_scrape_and_route(
            lambda: RSSScraper(feeds=critical_feeds),
            "scrape_all_feeds",
            limit_per_feed=10,
        ))
        return result
    except Exception as e:
        logger.error(f"[RSS] Task failed: {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_rss_all(self):
    """Scrape ALL configured RSS feeds (hourly full sweep)."""
    try:
        from scrapers.rss_scraper import RSSScraper
        result = _run_async(_scrape_and_route(
            RSSScraper,
            "scrape_all_feeds",
            limit_per_feed=20,
        ))
        return result
    except Exception as e:
        logger.error(f"[RSS-All] Task failed: {e}")
        self.retry(exc=e)


# ── Central Banks ─────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_central_banks(self):
    """Scrape RBI, Fed, ECB announcements."""
    try:
        from scrapers.centralbank_scraper import CentralBankScraper
        result = _run_async(_scrape_and_route(
            CentralBankScraper,
            "scrape_all",
        ))
        return result
    except Exception as e:
        logger.error(f"[CentralBank] Task failed: {e}")
        self.retry(exc=e)


# ── SEC EDGAR ─────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_sec(self):
    """Scrape recent SEC filings."""
    try:
        from scrapers.sec_scraper import SECScraper
        result = _run_async(_scrape_and_route(
            SECScraper,
            "scrape_recent_filings",
            limit=30,
        ))
        return result
    except Exception as e:
        logger.error(f"[SEC] Task failed: {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=120)
def scrape_sec_comprehensive(self):
    """Comprehensive SEC scan (6-hourly)."""
    try:
        from scrapers.sec_scraper import SECScraper
        result = _run_async(_scrape_and_route(
            SECScraper,
            "scrape_all",
        ))
        return result
    except Exception as e:
        logger.error(f"[SEC-Full] Task failed: {e}")
        self.retry(exc=e)


# ── GitHub ────────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_github(self):
    """Scrape monitored GitHub repos."""
    try:
        from scrapers.github_scraper import GitHubScraper
        result = _run_async(_scrape_and_route(
            lambda: GitHubScraper(token=os.getenv("GITHUB_TOKEN")),
            "scrape_all_monitored",
            limit_per_repo=10,
        ))
        return result
    except Exception as e:
        logger.error(f"[GitHub] Task failed: {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=120)
def scrape_github_trending(self):
    """Scrape GitHub trending repos (6-hourly)."""
    try:
        from scrapers.github_scraper import GitHubScraper
        result = _run_async(_scrape_and_route(
            lambda: GitHubScraper(token=os.getenv("GITHUB_TOKEN")),
            "scrape_trending",
        ))
        return result
    except Exception as e:
        logger.error(f"[GitHub-Trending] Task failed: {e}")
        self.retry(exc=e)


# ── Discord ───────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_discord(self):
    """Scrape monitored Discord channels."""
    try:
        bot_token = os.getenv("DISCORD_BOT_TOKEN")
        if not bot_token:
            return {"scraper": "discord", "items_scraped": 0, "note": "No bot token configured"}

        channels = os.getenv("DISCORD_CHANNELS", "").split(",")
        channels = [c.strip() for c in channels if c.strip()]
        if not channels:
            return {"scraper": "discord", "items_scraped": 0, "note": "No channels configured"}

        result = _run_async(_scrape_discord_impl(bot_token, channels))
        return result
    except Exception as e:
        logger.error(f"[Discord] Task failed: {e}")
        self.retry(exc=e)


async def _scrape_discord_impl(bot_token: str, channels: list[str]):
    """Async implementation for Discord scraping."""
    from scrapers.discord_scraper import DiscordScraper
    from connectors.router import DataRouter

    scraper = DiscordScraper(bot_token=bot_token)
    all_items = []
    for ch in channels:
        items = await scraper.safe_scrape_channel(ch, 50)
        all_items.extend(items)

    if all_items:
        router = DataRouter()
        results = await router.route(all_items)
        return {"scraper": "discord", "items_scraped": len(all_items), "routing": results}
    return {"scraper": "discord", "items_scraped": 0}


# ── Mastodon ──────────────────────────────────────────────────

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_mastodon(self):
    """Scrape Mastodon financial hashtags."""
    try:
        from scrapers.mastodon_scraper import MastodonScraper
        result = _run_async(_scrape_and_route(
            MastodonScraper,
            "scrape_financial_hashtags",
            limit_per_tag=20,
        ))
        return result
    except Exception as e:
        logger.error(f"[Mastodon] Task failed: {e}")
        self.retry(exc=e)


# ── Web Scraping ──────────────────────────────────────────────

@app.task(bind=True, max_retries=2, default_retry_delay=60)
def scrape_web(self):
    """Scrape financial news websites."""
    try:
        from scrapers.web_scraper import WebScraper
        result = _run_async(_scrape_and_route(
            WebScraper,
            "scrape_all_targets",
            limit_per_site=10,
        ))
        return result
    except Exception as e:
        logger.error(f"[Web] Task failed: {e}")
        self.retry(exc=e)


# ── Dark Web ──────────────────────────────────────────────────

@app.task(bind=True, max_retries=2, default_retry_delay=120)
def scrape_darkweb(self):
    """Scrape dark web threat intelligence."""
    try:
        from scrapers.darkweb_scraper import DarkWebScraper
        result = _run_async(_scrape_and_route(
            lambda: DarkWebScraper(
                tor_proxy=os.getenv("TOR_PROXY", "socks5://127.0.0.1:9050"),
            ),
            "scrape_all_threat_intel",
        ))
        return result
    except Exception as e:
        logger.error(f"[DarkWeb] Task failed: {e}")
        self.retry(exc=e)


# ── Health & Stats ────────────────────────────────────────────

@app.task
def health_check():
    """Check health of all scrapers and destinations."""
    try:
        result = _run_async(_health_check_impl())
        return result
    except Exception as e:
        logger.error(f"[Health] Check failed: {e}")
        return {"status": "error", "error": str(e)}


async def _health_check_impl():
    """Run health checks on all components."""
    import redis.asyncio as aioredis

    health = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scrapers": {},
        "destinations": {},
    }

    # Check Redis
    try:
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        await r.ping()
        health["redis"] = "ok"
        await r.close()
    except Exception as e:
        health["redis"] = f"error: {e}"

    # Check DragonScope Redis
    try:
        r = aioredis.from_url(os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"), decode_responses=True)
        await r.ping()
        health["destinations"]["dragonscope_redis"] = "ok"
        await r.close()
    except Exception:
        health["destinations"]["dragonscope_redis"] = "unreachable"

    # Check LiquiFi Redis
    try:
        r = aioredis.from_url(os.getenv("LIQUIFI_REDIS_URL", "redis://localhost:6379/2"), decode_responses=True)
        await r.ping()
        health["destinations"]["liquifi_redis"] = "ok"
        await r.close()
    except Exception:
        health["destinations"]["liquifi_redis"] = "unreachable"

    # Store health in Redis
    try:
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
        await r.set("scraper:health", json.dumps(health), ex=600)
        await r.close()
    except Exception:
        pass

    return health


@app.task
def push_stats():
    """Push scraper statistics to destinations for monitoring dashboards."""
    try:
        result = _run_async(_push_stats_impl())
        return result
    except Exception as e:
        logger.error(f"[Stats] Push failed: {e}")
        return {"error": str(e)}


async def _push_stats_impl():
    """Aggregate and push scraper stats."""
    import redis.asyncio as aioredis

    try:
        r = aioredis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

        # Get all scraper stats
        stats_keys = await r.keys("scraper:stats:*")
        all_stats = {}
        for key in stats_keys:
            data = await r.get(key)
            if data:
                all_stats[key.replace("scraper:stats:", "")] = json.loads(data)

        # Get total counts
        total = await r.get("scraper:total_items") or "0"

        summary = {
            "total_items_scraped": int(total),
            "scrapers": all_stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Push to DragonScope for monitoring panel
        dr = aioredis.from_url(os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"), decode_responses=True)
        await dr.set("market:scraper_stats", json.dumps(summary), ex=1200)
        await dr.close()

        await r.close()
        return summary
    except Exception as e:
        return {"error": str(e)}
