"""Celery tasks — runs collectors, processors, routing, health checks."""

import asyncio
import logging
import os

from core.scheduler import app

logger = logging.getLogger(__name__)


def _run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def run_collector(self, source_name: str):
    """Run a single collector by name (looked up from registry)."""
    try:
        from core.registry import discover_collectors
        collectors = discover_collectors()

        if source_name not in collectors:
            logger.error(f"Unknown source: {source_name}")
            return {"error": f"Unknown source: {source_name}"}

        collector = collectors[source_name]
        result = _run_async(collector.run())
        logger.info(f"[{source_name}] Result: {result.get('status')} ({result.get('records_collected', 0)} records)")
        return result
    except Exception as e:
        logger.error(f"[{source_name}] Task failed: {e}")
        self.retry(exc=e)


@app.task
def process_pipeline():
    """Run the NLP processing pipeline on unprocessed articles."""
    results = {}

    # Article extraction
    try:
        from processors.article_extractor import ArticleExtractor
        ext = ArticleExtractor()
        results["article_extractor"] = ext.run()
    except Exception as e:
        results["article_extractor"] = {"error": str(e)}

    # Deduplication
    try:
        from processors.deduplicator import Deduplicator
        dedup = Deduplicator()
        results["deduplicator"] = dedup.run()
    except Exception as e:
        results["deduplicator"] = {"error": str(e)}

    # Embedding
    try:
        from processors.embedder import Embedder
        emb = Embedder()
        results["embedder"] = emb.run()
    except Exception as e:
        results["embedder"] = {"error": str(e)}

    # Sentiment
    try:
        from processors.sentiment import SentimentAnalyzer
        sent = SentimentAnalyzer()
        results["sentiment"] = sent.run()
    except Exception as e:
        results["sentiment"] = {"error": str(e)}

    # Entity extraction
    try:
        from processors.entity_extractor import EntityExtractor
        ent = EntityExtractor()
        results["entities"] = ent.run()
    except Exception as e:
        results["entities"] = {"error": str(e)}

    # Topic classification
    try:
        from processors.topic_classifier import TopicClassifier
        tc = TopicClassifier()
        results["topics"] = tc.run()
    except Exception as e:
        results["topics"] = {"error": str(e)}

    return results


@app.task
def generate_digest():
    """Generate daily digest via LLM."""
    try:
        from processors.daily_digest import DailyDigestGenerator
        gen = DailyDigestGenerator()
        return gen.run()
    except Exception as e:
        logger.error(f"Digest generation failed: {e}")
        return {"error": str(e)}


@app.task
def health_check_all():
    """Check health of all sources."""
    from core.health import system_status
    return system_status()


@app.task
def check_data_quality():
    """Run data quality checks."""
    try:
        from monitoring.data_quality import DataQualityChecker
        checker = DataQualityChecker()
        return checker.run()
    except Exception as e:
        return {"error": str(e)}


@app.task
def route_to_destinations():
    """Route recent data to DragonScope + LiquiFi."""
    try:
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

        # Get recent unrouted items from social scrapers
        from api.database import SessionLocal
        from api.models import ScrapedPost
        from sqlalchemy import desc
        from models import ScrapedContent, ScrapedItem, AuthorInfo, EngagementMetrics, Platform
        from datetime import datetime, timezone, timedelta

        db = SessionLocal()
        try:
            recent = (
                db.query(ScrapedPost)
                .filter(ScrapedPost.scraped_at >= datetime.now(timezone.utc) - timedelta(minutes=5))
                .order_by(desc(ScrapedPost.scraped_at))
                .limit(200)
                .all()
            )

            if not recent:
                return {"routed": 0}

            items = []
            for post in recent:
                try:
                    platform = Platform(post.platform) if post.platform in [p.value for p in Platform] else Platform.WEB
                except ValueError:
                    platform = Platform.WEB

                content = ScrapedContent(
                    id=post.platform_id or str(post.id),
                    platform=platform,
                    content_type="post",
                    text=post.text or "",
                    author=AuthorInfo(
                        username=post.author_username or "",
                        display_name=post.author_display_name or "",
                    ),
                    engagement=EngagementMetrics(
                        likes=post.likes or 0,
                        replies=post.replies or 0,
                        reposts=post.reposts or 0,
                    ),
                    created_at=post.created_at or datetime.now(timezone.utc),
                    source_url=post.source_url or "",
                    source_channel=post.source_channel or "",
                    hashtags=post.hashtags or [],
                    raw_metadata=post.raw_metadata or {},
                )
                items.append(ScrapedItem(unified=content))

            result = _run_async(router.route(items))
            return result
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Routing failed: {e}")
        return {"error": str(e)}
