"""Unified Celery tasks — collectors, scrapers, processors, routing, health.

This is the SINGLE task module for econscraper. All tasks run through
the core.scheduler Celery app. The old scheduler/tasks.py is deprecated.

Task groups:
1. Collectors (YAML-driven, BaseCollector subclasses)
2. Social scrapers (code-driven, BaseScraper subclasses)
3. NLP processors (article extraction → dedup → embed → sentiment → NER → topics)
4. Routing (push data to DragonScope + LiquiFi)
5. Health & monitoring
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from core.scheduler import app

logger = logging.getLogger(__name__)

_collectors_cache = None


def _run_async(coro):
    """Run an async coroutine from a sync Celery task."""
    return asyncio.run(coro)


# ══════════════════════════════════════════════════════════════
# 1. COLLECTOR TASKS (YAML-driven via sources.yaml)
# ══════════════════════════════════════════════════════════════

@app.task(bind=True, max_retries=3, default_retry_delay=60)
def run_collector(self, source_name: str):
    """Run a single collector by name (from sources.yaml registry)."""
    try:
        global _collectors_cache
        if _collectors_cache is None:
            from core.registry import discover_collectors
            _collectors_cache = discover_collectors()
        collectors = _collectors_cache

        if source_name not in collectors:
            logger.error(f"Unknown source: {source_name}")
            return {"error": f"Unknown source: {source_name}"}

        collector = collectors[source_name]
        result = _run_async(collector.run())
        logger.info(f"[{source_name}] {result.get('status')}: {result.get('records_collected', 0)} records")
        return result
    except Exception as e:
        logger.error(f"[{source_name}] Task failed: {e}")
        self.retry(exc=e)


# ══════════════════════════════════════════════════════════════
# 2. SOCIAL SCRAPER TASKS (route to DragonScope + LiquiFi)
# ══════════════════════════════════════════════════════════════

def _make_router():
    """Create a DataRouter with DragonScope + LiquiFi connectors."""
    from connectors.router import DataRouter
    from connectors.dragonscope import DragonScopeConnector
    from connectors.liquifi import LiquiFiConnector

    return DataRouter(
        dragonscope=DragonScopeConnector(
            redis_url=os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"),
            api_url=os.getenv("DRAGONSCOPE_API_URL", "http://localhost:3456"),
        ),
        liquifi=LiquiFiConnector(
            api_url=os.getenv("LIQUIFI_API_URL", "http://localhost:8001"),
            redis_url=os.getenv("LIQUIFI_REDIS_URL", "redis://localhost:6379/2"),
        ),
    )


async def _scrape_and_route(scraper_factory, method_name: str, **kwargs):
    """Generic: create scraper → call method → route results → store locally."""
    router = _make_router()
    scraper = None
    try:
        scraper = scraper_factory()
        method = getattr(scraper, method_name)
        items = await method(**kwargs)

        result = {
            "scraper": getattr(scraper, "name", "unknown"),
            "items_scraped": len(items) if items else 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if items:
            routing = await router.route(items)
            result["routing"] = routing
            await _store_scraped_items(items)

        return result
    finally:
        if scraper and hasattr(scraper, "close"):
            await scraper.close()
        await router.close()


async def _store_scraped_items(items):
    """Store scraped items in v4 articles table (not the old scraped_posts)."""
    try:
        import hashlib
        from api.database import SessionLocal
        from storage.models import Article

        db = SessionLocal()
        try:
            for item in items:
                u = item.unified
                url = u.source_url or ""
                url_hash = hashlib.sha256(url.encode()).hexdigest()[:32] if url else None

                article = Article(
                    source=u.platform.value,
                    source_type="social",
                    url=url,
                    url_hash=url_hash,
                    title=u.raw_metadata.get("title", u.text[:200] if u.text else ""),
                    author=u.author.username or u.author.display_name,
                    published_at=u.created_at or datetime.now(timezone.utc),
                    collected_at=datetime.now(timezone.utc),
                    full_text=u.text[:10000] if u.text else "",
                    category=u.platform.value,
                )
                db.merge(article)
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"[Tasks] Local storage failed: {e}")


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_reddit(self):
    try:
        from scrapers.reddit_scraper import RedditScraper
        return _run_async(_scrape_and_route(
            lambda: RedditScraper(
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            ),
            "scrape_all_financial", limit_per_sub=25,
        ))
    except Exception as e:
        logger.error(f"[Reddit] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_twitter(self):
    try:
        return _run_async(_scrape_twitter_impl())
    except Exception as e:
        logger.error(f"[Twitter] {e}")
        self.retry(exc=e)


async def _scrape_twitter_impl():
    router = _make_router()
    try:
        from twitter_scraper import TwitterScraper
        scraper = TwitterScraper()
        queries = os.getenv("TWITTER_QUERIES", "stock market,crypto,RBI,treasury,forex").split(",")
        all_items = []
        for q in queries:
            try:
                items = await scraper.search(q.strip(), limit=20)
                all_items.extend(items)
            except Exception:
                pass

        if all_items:
            routing = await router.route(all_items)
            await _store_scraped_items(all_items)
            return {"scraper": "twitter", "items_scraped": len(all_items), "routing": routing}
    except ImportError:
        logger.debug("[Twitter] twitter_scraper not available")
    finally:
        await router.close()
    return {"scraper": "twitter", "items_scraped": 0}


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_hackernews(self):
    try:
        from scrapers.hackernews_scraper import HackerNewsScraper
        return _run_async(_scrape_and_route(HackerNewsScraper, "scrape_financial", limit=50))
    except Exception as e:
        logger.error(f"[HN] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_youtube(self):
    try:
        from scrapers.youtube_scraper import YouTubeScraper
        return _run_async(_scrape_and_route(
            lambda: YouTubeScraper(api_key=os.getenv("YOUTUBE_API_KEY")),
            "scrape_financial_content", limit_per_query=20,
        ))
    except Exception as e:
        logger.error(f"[YouTube] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_rss_financial(self):
    try:
        from scrapers.rss_scraper import RSSScraper
        critical_feeds = {
            "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
            "reuters_markets": "https://feeds.reuters.com/reuters/marketsNews",
            "cnbc_finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
            "moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
            "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        }
        return _run_async(_scrape_and_route(
            lambda: RSSScraper(feeds=critical_feeds),
            "scrape_all_feeds", limit_per_feed=10,
        ))
    except Exception as e:
        logger.error(f"[RSS] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_central_banks(self):
    try:
        from scrapers.centralbank_scraper import CentralBankScraper
        return _run_async(_scrape_and_route(CentralBankScraper, "scrape_all"))
    except Exception as e:
        logger.error(f"[CentralBank] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_sec(self):
    try:
        from scrapers.sec_scraper import SECScraper
        return _run_async(_scrape_and_route(SECScraper, "scrape_recent_filings", limit=30))
    except Exception as e:
        logger.error(f"[SEC] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_github(self):
    try:
        from scrapers.github_scraper import GitHubScraper
        return _run_async(_scrape_and_route(
            lambda: GitHubScraper(token=os.getenv("GITHUB_TOKEN")),
            "scrape_all_monitored", limit_per_repo=10,
        ))
    except Exception as e:
        logger.error(f"[GitHub] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_mastodon(self):
    try:
        from scrapers.mastodon_scraper import MastodonScraper
        return _run_async(_scrape_and_route(MastodonScraper, "scrape_financial_hashtags", limit_per_tag=20))
    except Exception as e:
        logger.error(f"[Mastodon] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=120)
def scrape_darkweb(self):
    try:
        from scrapers.darkweb_scraper import DarkWebScraper
        return _run_async(_scrape_and_route(
            lambda: DarkWebScraper(tor_proxy=os.getenv("TOR_PROXY", "socks5://127.0.0.1:9050")),
            "scrape_all_threat_intel",
        ))
    except Exception as e:
        logger.error(f"[DarkWeb] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=60)
def scrape_web(self):
    try:
        from scrapers.web_scraper import WebScraper
        return _run_async(_scrape_and_route(WebScraper, "scrape_all_targets", limit_per_site=10))
    except Exception as e:
        logger.error(f"[Web] {e}")
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_discord(self):
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    if not bot_token:
        return {"scraper": "discord", "items_scraped": 0, "note": "No bot token"}
    try:
        from scrapers.discord_scraper import DiscordScraper
        return _run_async(_scrape_and_route(
            lambda: DiscordScraper(bot_token=bot_token),
            "scrape",
        ))
    except Exception as e:
        logger.error(f"[Discord] {e}")
        self.retry(exc=e)


# ══════════════════════════════════════════════════════════════
# 3. NLP PROCESSOR TASKS
# ══════════════════════════════════════════════════════════════

@app.task
def process_pipeline():
    """Run the full NLP pipeline on unprocessed articles."""
    results = {}

    processors = [
        ("article_extractor", "processors.article_extractor", "ArticleExtractor"),
        ("deduplicator", "processors.deduplicator", "Deduplicator"),
        ("embedder", "processors.embedder", "Embedder"),
        ("sentiment", "processors.sentiment", "SentimentAnalyzer"),
        ("entities", "processors.entity_extractor", "EntityExtractor"),
        ("topics", "processors.topic_classifier", "TopicClassifier"),
    ]

    for name, module_path, class_name in processors:
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            results[name] = cls().run()
        except Exception as e:
            results[name] = {"error": str(e)}

    return results


@app.task
def generate_digest():
    """Generate daily LLM digest."""
    try:
        from processors.daily_digest import DailyDigestGenerator
        return DailyDigestGenerator().run()
    except Exception as e:
        logger.error(f"Digest failed: {e}")
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════
# 4. ROUTING — Push collected data to DragonScope + LiquiFi
# ══════════════════════════════════════════════════════════════

@app.task
def route_to_destinations():
    """Route recently collected articles + economic data to DragonScope and LiquiFi.

    Reads from the v4 storage.models tables (Article, EconomicData),
    not the old api.models.ScrapedPost table.
    """
    try:
        return _run_async(_route_collected_data())
    except Exception as e:
        logger.error(f"Routing failed: {e}")
        return {"error": str(e)}


async def _route_collected_data():
    """Push recent articles and economic data to both destinations."""
    import hashlib

    from api.database import SessionLocal
    from storage.models import Article, EconomicData

    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)

        # --- Route articles to DragonScope + LiquiFi ---
        recent_articles = (
            db.query(Article)
            .filter(Article.collected_at >= cutoff)
            .order_by(Article.collected_at.desc())
            .limit(200)
            .all()
        )

        # --- Route economic data to both ---
        recent_econ = (
            db.query(EconomicData)
            .filter(EconomicData.collected_at >= cutoff)
            .order_by(EconomicData.collected_at.desc())
            .limit(200)
            .all()
        )

        results = {"articles": 0, "economic_data": 0, "dragonscope": {}, "liquifi": {}}

        if not recent_articles and not recent_econ:
            return {"routed": 0, "message": "No new data"}

        # Push articles via Redis to DragonScope
        ds_redis = None
        try:
            import redis.asyncio as aioredis
            ds_redis = aioredis.from_url(
                os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"),
                decode_responses=True,
            )

            if recent_articles:
                article_payload = {
                    "articles": [
                        {
                            "title": a.title or "",
                            "content": (a.full_text or "")[:1000],
                            "source": a.source,
                            "category": a.category or "news",
                            "url": a.url or "",
                            "author": a.author or "",
                            "published_at": a.published_at.isoformat() if a.published_at else "",
                            "collected_at": a.collected_at.isoformat() if a.collected_at else "",
                        }
                        for a in recent_articles
                    ],
                    "source": "econscraper",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await ds_redis.set("market:news", json.dumps(article_payload, default=str), ex=300)
                results["articles"] = len(recent_articles)

                # Categorize for DragonScope
                for category in set(a.category or "news" for a in recent_articles):
                    cat_articles = [a for a in recent_articles if (a.category or "news") == category]
                    await ds_redis.set(
                        f"market:{category}",
                        json.dumps({
                            "articles": [{"title": a.title, "url": a.url, "source": a.source} for a in cat_articles],
                            "count": len(cat_articles),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }, default=str),
                        ex=300,
                    )

                # Publish update notification
                await ds_redis.publish("market:updates", json.dumps({
                    "type": "new_articles",
                    "count": len(recent_articles),
                    "source": "econscraper",
                }))
                results["dragonscope"]["articles"] = len(recent_articles)

            if recent_econ:
                econ_payload = {
                    "indicators": [
                        {
                            "source": e.source,
                            "indicator": e.indicator,
                            "value": float(e.value) if e.value else None,
                            "date": e.date.isoformat() if e.date else "",
                            "unit": e.unit or "",
                        }
                        for e in recent_econ
                    ],
                    "source": "econscraper",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await ds_redis.set("market:economic_data", json.dumps(econ_payload, default=str), ex=600)
                await ds_redis.publish("market:updates", json.dumps({
                    "type": "economic_data",
                    "count": len(recent_econ),
                }))
                results["dragonscope"]["economic_data"] = len(recent_econ)
                results["economic_data"] = len(recent_econ)
        except Exception as e:
            logger.warning(f"[Route] DragonScope push failed: {e}")
            results["dragonscope"]["error"] = str(e)
        finally:
            if ds_redis:
                await ds_redis.close()

        # Push treasury-relevant articles to LiquiFi
        lf_redis = None
        try:
            import redis.asyncio as aioredis
            lf_redis = aioredis.from_url(
                os.getenv("LIQUIFI_REDIS_URL", "redis://localhost:6379/2"),
                decode_responses=True,
            )

            # Filter for treasury relevance
            treasury_keywords = [
                "repo rate", "rbi", "mibor", "sofr", "crr", "slr",
                "monetary policy", "interest rate", "treasury", "bond yield",
                "usd/inr", "rupee", "forex", "g-sec", "liquidity",
                "inflation", "cpi", "gdp", "fiscal deficit",
            ]

            treasury_articles = []
            for a in recent_articles:
                text = ((a.full_text or "") + " " + (a.title or "")).lower()
                hits = sum(1 for kw in treasury_keywords if kw in text)
                if hits >= 1:
                    treasury_articles.append({
                        "title": a.title or "",
                        "body": (a.full_text or "")[:500],
                        "source": a.source,
                        "url": a.url or "",
                        "published_at": a.published_at.isoformat() if a.published_at else "",
                        "relevance_hits": hits,
                    })

            if treasury_articles:
                await lf_redis.set("liquifi:treasury_news", json.dumps({
                    "news": treasury_articles,
                    "source": "econscraper",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, default=str), ex=600)

                await lf_redis.publish("liquifi:updates", json.dumps({
                    "type": "treasury_news",
                    "count": len(treasury_articles),
                }))

            # Push rate data from FRED, RBI, CCIL collectors
            rate_sources = {"fred_api", "rbi_dbie", "ccil_rates"}
            rate_data = [e for e in recent_econ if e.source in rate_sources]
            if rate_data:
                await lf_redis.set("liquifi:rate_data", json.dumps({
                    "rates": [
                        {
                            "indicator": e.indicator,
                            "value": float(e.value) if e.value else None,
                            "source": e.source,
                            "date": e.date.isoformat() if e.date else "",
                        }
                        for e in rate_data
                    ],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, default=str), ex=600)

            results["liquifi"]["treasury_articles"] = len(treasury_articles)
            results["liquifi"]["rate_data"] = len(rate_data)
        except Exception as e:
            logger.warning(f"[Route] LiquiFi push failed: {e}")
            results["liquifi"]["error"] = str(e)
        finally:
            if lf_redis:
                await lf_redis.close()

        return results
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════
# 5. HEALTH & MONITORING
# ══════════════════════════════════════════════════════════════

@app.task
def health_check_all():
    """Check health of all sources + destinations."""
    from core.health import system_status
    return system_status()


@app.task
def check_data_quality():
    """Run data quality checks."""
    try:
        from monitoring.data_quality import DataQualityChecker
        checker = DataQualityChecker()
        return checker.run_all_checks()
    except Exception as e:
        return {"error": str(e)}


@app.task
def generate_and_email_report():
    """Generate the daily World Intelligence PDF and email it."""
    try:
        from reports.pdf_generator import generate_report, _fetch_report_data
        from reports.mailer import send_report_email

        pdf_path = generate_report()
        data = _fetch_report_data()
        stats = {
            "total_articles": data.get("total_articles", 0),
            "total_sources": data.get("total_sources", 0),
            "avg_sentiment": data.get("avg_sentiment", 0),
        }
        success = send_report_email(pdf_path=pdf_path, stats=stats)
        return {
            "status": "sent" if success else "email_failed",
            "pdf_path": pdf_path,
            "articles": stats["total_articles"],
        }
    except Exception as e:
        logger.error(f"[Report] {e}")
        return {"error": str(e)}


@app.task
def push_stats():
    """Push aggregate stats to DragonScope for monitoring dashboards."""
    try:
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

        stats_keys = list(r.scan_iter("health:*"))
        health = {}
        for key in stats_keys:
            data = r.get(key)
            if data:
                health[key.replace("health:", "")] = json.loads(data)

        summary = {
            "sources": health,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Push to DragonScope
        try:
            ds = redis.from_url(os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"), decode_responses=True)
            ds.set("market:scraper_stats", json.dumps(summary, default=str), ex=1200)
            ds.close()
        except Exception:
            pass

        r.close()
        return summary
    except Exception as e:
        return {"error": str(e)}
