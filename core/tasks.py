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


def _check_backpressure(scraper_name: str, is_critical: bool = False) -> bool:
    """Check backpressure before running a scraper. Returns True if should skip."""
    try:
        from core.backpressure import get_backpressure_manager
        bp = get_backpressure_manager()
        return bp.should_skip_scraper(scraper_name, is_critical=is_critical)
    except Exception:
        return False


def _record_metrics(scraper_name: str, items_count: int, duration: float, error: str = ""):
    """Record scraper metrics for Prometheus export."""
    try:
        from monitoring.metrics import get_metrics_registry
        registry = get_metrics_registry()
        registry.scraper_articles_total.inc(items_count, source=scraper_name)
        registry.scraper_duration_seconds.observe(duration, source=scraper_name)
        if error:
            registry.scraper_errors_total.inc(1, source=scraper_name, error_type=error)
    except Exception:
        pass


def _log_scraper_collection(source: str, status: str, records: int, duration: float, error: str = ""):
    """Write a CollectionLog entry for a social scraper run.

    Without this, data_quality.py staleness checks always report social
    scrapers as stale because they only query CollectionLog (which was
    previously only written by YAML-driven BaseCollector subclasses).
    """
    try:
        from storage.models import CollectionLog
        from api.database import SessionLocal

        db = SessionLocal()
        try:
            log = CollectionLog(
                source=source,
                status=status,
                records_collected=records,
                duration_seconds=round(duration, 2),
                error_message=error[:1000] if error else None,
                run_at=datetime.now(timezone.utc),
            )
            db.add(log)
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.debug(f"[Tasks] CollectionLog write failed for {source}: {e}")


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
        logger.error(
            "Collector task failed",
            extra={"source": source_name, "error_type": type(e).__name__, "error": str(e)},
        )
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
    """Generic: create scraper → call method → dedup → route results → store locally."""
    import time as _time
    from core.dedup import URLDeduplicator

    start = _time.monotonic()
    router = _make_router()
    dedup = URLDeduplicator()
    scraper = None
    scraper_name = "unknown"
    try:
        scraper = scraper_factory()
        scraper_name = getattr(scraper, "name", "unknown")
        method = getattr(scraper, method_name)
        items = await method(**kwargs)

        # Deduplicate by URL before routing
        if items:
            unique_items = []
            for item in items:
                url = getattr(item.unified, "source_url", None) or ""
                if url and await dedup.is_seen(url):
                    continue
                unique_items.append(item)
                if url:
                    await dedup.mark_seen(url)
            deduped_count = len(items) - len(unique_items)
            items = unique_items
        else:
            deduped_count = 0

        result = {
            "scraper": scraper_name,
            "items_scraped": len(items) if items else 0,
            "deduped": deduped_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if items:
            routing = await router.route(items)
            result["routing"] = routing
            await _store_scraped_items(items)

            # Fire webhook for new articles if significant batch
            if len(items) >= 5:
                try:
                    from core.webhooks import fire_event
                    fire_event("new_article", {
                        "source": scraper_name,
                        "count": len(items),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as e:
                    logger.warning(f"[{scraper_name}] Webhook fire failed: {e}")

        # Record metrics
        duration = _time.monotonic() - start
        _record_metrics(scraper_name, len(items) if items else 0, duration)

        # Write CollectionLog so data quality staleness checks work
        _log_scraper_collection(
            source=scraper_name,
            status="success",
            records=len(items) if items else 0,
            duration=duration,
        )

        return result
    except Exception as e:
        duration = _time.monotonic() - start
        _record_metrics(scraper_name, 0, duration, error=type(e).__name__)
        _log_scraper_collection(
            source=scraper_name,
            status="failed",
            records=0,
            duration=duration,
            error=str(e),
        )
        raise
    finally:
        if scraper and hasattr(scraper, "close"):
            await scraper.close()
        await router.close()
        await dedup.close()


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
    if _check_backpressure("reddit"):
        return {"scraper": "reddit", "items_scraped": 0, "skipped": "backpressure"}
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
        logger.error(
            "Scrape task failed",
            extra={"source": "reddit", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_twitter(self):
    if _check_backpressure("twitter"):
        return {"scraper": "twitter", "items_scraped": 0, "skipped": "backpressure"}
    try:
        return _run_async(_scrape_twitter_impl())
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "twitter", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


async def _scrape_twitter_impl():
    import time as _time

    start = _time.monotonic()
    router = _make_router()
    dedup = None
    scraper = None
    try:
        from core.dedup import URLDeduplicator
        from scrapers.twitter_scraper import TwitterScraper

        dedup = URLDeduplicator()
        scraper = TwitterScraper()
        queries = os.getenv("TWITTER_QUERIES", "stock market,crypto,RBI,treasury,forex").split(",")
        all_items = []
        query_errors = 0
        for q in queries:
            try:
                items = await scraper.search_tweets(q.strip(), count=20)
                all_items.extend(items)
            except Exception as e:
                query_errors += 1
                logger.warning(f"[Twitter] Query '{q.strip()}' failed: {e}")

        if query_errors == len(queries):
            logger.error(f"[Twitter] All {len(queries)} queries failed — possible auth/rate-limit issue")

        # Deduplicate before routing
        if all_items:
            unique_items = []
            for item in all_items:
                url = getattr(item.unified, "source_url", None) or ""
                if url and await dedup.is_seen(url):
                    continue
                unique_items.append(item)
                if url:
                    await dedup.mark_seen(url)
            deduped_count = len(all_items) - len(unique_items)
            all_items = unique_items
        else:
            deduped_count = 0

        result = {
            "scraper": "twitter",
            "items_scraped": len(all_items),
            "deduped": deduped_count,
            "query_errors": query_errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if all_items:
            routing = await router.route(all_items)
            await _store_scraped_items(all_items)
            result["routing"] = routing

        duration = _time.monotonic() - start
        _record_metrics("twitter", len(all_items), duration)
        _log_scraper_collection(
            source="twitter", status="success",
            records=len(all_items), duration=duration,
        )
        return result
    except ImportError:
        logger.debug("[Twitter] twitter_scraper not available")
        return {"scraper": "twitter", "items_scraped": 0}
    except Exception:
        duration = _time.monotonic() - start
        _record_metrics("twitter", 0, duration, error="scrape_failed")
        _log_scraper_collection(
            source="twitter", status="failed",
            records=0, duration=duration, error="scrape_failed",
        )
        raise
    finally:
        if scraper and hasattr(scraper, "close"):
            await scraper.close()
        await router.close()
        if dedup:
            await dedup.close()


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_hackernews(self):
    if _check_backpressure("hackernews"):
        return {"scraper": "hackernews", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.hackernews_scraper import HackerNewsScraper
        return _run_async(_scrape_and_route(HackerNewsScraper, "scrape_financial", limit=50))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "hackernews", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_youtube(self):
    if _check_backpressure("youtube"):
        return {"scraper": "youtube", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.youtube_scraper import YouTubeScraper
        return _run_async(_scrape_and_route(
            lambda: YouTubeScraper(api_key=os.getenv("YOUTUBE_API_KEY")),
            "scrape_financial_content", limit_per_query=20,
        ))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "youtube", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_rss_financial(self):
    if _check_backpressure("rss", is_critical=True):
        return {"scraper": "rss", "items_scraped": 0, "skipped": "backpressure"}
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
        logger.error(
            "Scrape task failed",
            extra={"source": "rss", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def scrape_central_banks(self):
    if _check_backpressure("central_bank", is_critical=True):
        return {"scraper": "central_bank", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.centralbank_scraper import CentralBankScraper
        return _run_async(_scrape_and_route(CentralBankScraper, "scrape_all"))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "central_bank", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_sec(self):
    if _check_backpressure("sec_edgar"):
        return {"scraper": "sec_edgar", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.sec_scraper import SECScraper
        return _run_async(_scrape_and_route(SECScraper, "scrape_recent_filings", limit=30))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "sec_edgar", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_github(self):
    if _check_backpressure("github"):
        return {"scraper": "github", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.github_scraper import GitHubScraper
        return _run_async(_scrape_and_route(
            lambda: GitHubScraper(token=os.getenv("GITHUB_TOKEN")),
            "scrape_all_monitored", limit_per_repo=10,
        ))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "github", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_mastodon(self):
    if _check_backpressure("mastodon"):
        return {"scraper": "mastodon", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.mastodon_scraper import MastodonScraper
        return _run_async(_scrape_and_route(MastodonScraper, "scrape_financial_hashtags", limit_per_tag=20))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "mastodon", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=120)
def scrape_darkweb(self):
    if _check_backpressure("darkweb"):
        return {"scraper": "darkweb", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.darkweb_scraper import DarkWebScraper
        return _run_async(_scrape_and_route(
            lambda: DarkWebScraper(tor_proxy=os.getenv("TOR_PROXY", "socks5://127.0.0.1:9050")),
            "scrape_all_threat_intel",
        ))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "darkweb", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=2, default_retry_delay=60)
def scrape_web(self):
    if _check_backpressure("web"):
        return {"scraper": "web", "items_scraped": 0, "skipped": "backpressure"}
    try:
        from scrapers.web_scraper import WebScraper
        return _run_async(_scrape_and_route(WebScraper, "scrape_all_targets", limit_per_site=10))
    except Exception as e:
        logger.error(
            "Scrape task failed",
            extra={"source": "web", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def scrape_discord(self):
    if _check_backpressure("discord"):
        return {"scraper": "discord", "items_scraped": 0, "skipped": "backpressure"}
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
        logger.error(
            "Scrape task failed",
            extra={"source": "discord", "error_type": type(e).__name__, "error": str(e)},
        )
        self.retry(exc=e)


# ══════════════════════════════════════════════════════════════
# 3. NLP PROCESSOR TASKS
# ══════════════════════════════════════════════════════════════

@app.task
def process_pipeline():
    """Run the full NLP pipeline on unprocessed articles, with quality scoring."""
    import time as _time
    results = {}

    # Quality scoring first — filter low-quality items before heavy NLP
    try:
        from processors.quality_scorer import QualityScorer
        from api.database import SessionLocal
        from storage.models import Article

        scorer = QualityScorer()
        db = SessionLocal()
        try:
            unprocessed = (
                db.query(Article)
                .filter(Article.is_processed == False)
                .limit(200)
                .all()
            )
            if unprocessed:
                articles = [
                    {
                        "id": a.id, "title": a.title, "full_text": a.full_text,
                        "author": a.author, "published_at": str(a.published_at) if a.published_at else None,
                        "source": a.source, "url_hash": a.url_hash,
                    }
                    for a in unprocessed
                ]
                scored = scorer.score_batch(articles)
                results["quality_scoring"] = {
                    "total": len(scored),
                    "passed": sum(1 for s in scored if s["quality_score"]["passed"]),
                    "filtered": sum(1 for s in scored if not s["quality_score"]["passed"]),
                    "avg_score": round(scorer.stats.get("avg_score", 0), 1),
                }

                # Record quality metrics
                try:
                    from monitoring.metrics import get_metrics_registry
                    registry = get_metrics_registry()
                    for s in scored:
                        registry.articles_quality_score.observe(s["quality_score"]["total"])
                except Exception:
                    pass
        finally:
            db.close()
    except Exception as e:
        results["quality_scoring"] = {"error": str(e)}

    processors = [
        ("article_extractor", "processors.article_extractor", "ArticleExtractor"),
        ("deduplicator", "processors.deduplicator", "Deduplicator"),
        ("embedder", "processors.embedder", "Embedder"),
        ("sentiment", "processors.sentiment", "SentimentAnalyzer"),
        ("entities", "processors.entity_extractor", "EntityExtractor"),
        ("topics", "processors.topic_classifier", "TopicClassifier"),
    ]

    for name, module_path, class_name in processors:
        start = _time.monotonic()
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            results[name] = cls().run()

            # Record NLP processing duration
            duration = _time.monotonic() - start
            try:
                from monitoring.metrics import get_metrics_registry
                registry = get_metrics_registry()
                registry.nlp_processing_duration_seconds.observe(duration, processor=name)
            except Exception:
                pass
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
    """Check health of all sources + destinations. Fires source_down webhook if needed."""
    from core.health import system_status
    status = system_status()

    # Fire webhook for failed sources
    failed_sources = [
        name for name, info in status.get("sources", {}).items()
        if info.get("status") == "failed"
    ]
    if failed_sources:
        try:
            from core.webhooks import fire_event
            fire_event("source_down", {
                "sources": failed_sources,
                "overall_status": status.get("status"),
                "timestamp": status.get("timestamp"),
            })
        except Exception:
            pass

    return status


@app.task
def check_data_quality():
    """Run data quality checks and fire webhooks for critical issues."""
    try:
        from monitoring.data_quality import DataQualityChecker
        checker = DataQualityChecker()
        issues = checker.run_all_checks()

        # Fire webhook if there are critical or warning-level issues
        critical_issues = [i for i in issues if i.get("severity") in ("critical", "warning")]
        if critical_issues:
            try:
                from core.webhooks import fire_event
                fire_event("data_quality_alert", {
                    "issues": critical_issues,
                    "total_issues": len(issues),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                logger.warning(f"[DataQuality] Webhook fire failed: {e}")

        return issues
    except Exception as e:
        return {"error": str(e)}


@app.task
def run_retention_cleanup():
    """Run daily data retention cleanup and archival."""
    try:
        from core.retention import RetentionManager
        mgr = RetentionManager(
            archive_to_s3=bool(os.getenv("MINIO_ENDPOINT")),
        )
        return mgr.run_all()
    except Exception as e:
        logger.error(f"[Retention] Cleanup failed: {e}")
        return {"error": str(e)}


@app.task
def check_backpressure():
    """Update backpressure state and fire source_down webhooks if needed."""
    try:
        from core.backpressure import get_backpressure_manager, PressureLevel
        bp = get_backpressure_manager()
        state = bp.check()

        # Fire webhook if critical
        if state.get("level") == PressureLevel.CRITICAL.value:
            try:
                from core.webhooks import fire_event
                fire_event("anomaly_detected", {
                    "type": "backpressure_critical",
                    "total_depth": state.get("total_depth", 0),
                    "celery_depth": state.get("celery_depth", 0),
                    "kafka_lag": state.get("kafka_lag", 0),
                })
            except Exception as e:
                logger.warning(f"[Backpressure] Webhook fire failed: {e}")

        # Update Prometheus gauge
        try:
            from monitoring.metrics import get_metrics_registry
            registry = get_metrics_registry()
            level_map = {"normal": 0, "warn": 1, "critical": 2}
            registry.backpressure_level.set(level_map.get(state.get("level", "normal"), 0))
        except Exception:
            pass

        return state
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

        try:
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
        finally:
            r.close()

        # Push to DragonScope
        try:
            ds = redis.from_url(os.getenv("DRAGONSCOPE_REDIS_URL", "redis://localhost:6379/1"), decode_responses=True)
            try:
                ds.set("market:scraper_stats", json.dumps(summary, default=str), ex=1200)
            finally:
                ds.close()
        except Exception:
            pass

        return summary
    except Exception as e:
        return {"error": str(e)}
