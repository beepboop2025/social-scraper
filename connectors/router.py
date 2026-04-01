"""Data router — classifies scraped content and routes to the right destination.

Routes data to:
- DragonScope: Market analytics, crypto, DeFi, research, sentiment
- LiquiFi: Treasury rates, RBI/Fed/ECB, Indian banking, forex
- Both: Major financial news, SEC filings, threat intel
- Kafka: All data (for persistence and async processing)
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from models import DestinationTag, Platform, ScrapedItem
from connectors.dragonscope import DragonScopeConnector
from connectors.liquifi import LiquiFiConnector

logger = logging.getLogger(__name__)

# Classification rules
DRAGONSCOPE_PLATFORMS = {
    Platform.REDDIT, Platform.DISCORD, Platform.YOUTUBE,
    Platform.HACKERNEWS, Platform.MASTODON, Platform.GITHUB,
}

LIQUIFI_PLATFORMS = {
    Platform.CENTRAL_BANK,
}

BOTH_PLATFORMS = {
    Platform.TWITTER, Platform.TELEGRAM, Platform.RSS,
    Platform.WEB, Platform.SEC_EDGAR, Platform.DARKWEB,
}


class DataRouter:
    """Intelligent data router that classifies and distributes scraped content.

    Classification pipeline:
    1. Platform-based routing (default)
    2. Content-based override (keyword analysis)
    3. Priority scoring for ordering
    4. Parallel push to destinations

    All data also goes through Kafka for persistence regardless of destination.
    """

    def __init__(
        self,
        dragonscope: Optional[DragonScopeConnector] = None,
        liquifi: Optional[LiquiFiConnector] = None,
        kafka_producer=None,
    ):
        self.dragonscope = dragonscope or DragonScopeConnector()
        self.liquifi = liquifi or LiquiFiConnector()
        self.kafka_producer = kafka_producer
        self._stats = {
            "total_routed": 0,
            "dragonscope_pushed": 0,
            "liquifi_pushed": 0,
            "kafka_published": 0,
            "errors": 0,
        }

    def classify(self, item: ScrapedItem) -> DestinationTag:
        """Classify which destination(s) should receive this item."""
        platform = item.unified.platform

        # Platform-based default
        if platform in LIQUIFI_PLATFORMS:
            dest = DestinationTag.LIQUIFI
        elif platform in DRAGONSCOPE_PLATFORMS:
            dest = DestinationTag.DRAGONSCOPE
        elif platform in BOTH_PLATFORMS:
            dest = DestinationTag.BOTH
        else:
            dest = DestinationTag.DRAGONSCOPE

        # Content-based override: check if treasury-relevant
        treasury_score, _ = self.liquifi.score_treasury_relevance(item)
        if treasury_score >= 0.3 and dest == DestinationTag.DRAGONSCOPE:
            dest = DestinationTag.BOTH

        # Threat intel always goes to both (security implications)
        if item.unified.platform == Platform.DARKWEB:
            dest = DestinationTag.BOTH

        return dest

    KAFKA_PUBLISH_TIMEOUT = 45  # seconds — must exceed producer flush_timeout (30s)

    async def _publish_to_kafka(self, items: list[ScrapedItem], destination: str):
        """Publish items to Kafka for persistence."""
        if not self.kafka_producer:
            return

        try:
            from pipeline.producer import publish_batch
            batch = []
            for item in items:
                data = item.unified.model_dump()
                data["_destination"] = destination
                data["_routed_at"] = datetime.now(timezone.utc).isoformat()
                batch.append(data)

            await asyncio.wait_for(
                asyncio.to_thread(
                    publish_batch, self.kafka_producer, batch, f"routed-{destination}"
                ),
                timeout=self.KAFKA_PUBLISH_TIMEOUT,
            )
            self._stats["kafka_published"] += len(items)
        except asyncio.TimeoutError:
            logger.error(
                f"[Router] Kafka publish timed out after {self.KAFKA_PUBLISH_TIMEOUT}s "
                f"for {len(items)} items to routed-{destination}"
            )
            self._stats["errors"] += 1
        except Exception as e:
            logger.warning(f"[Router] Kafka publish failed: {e}")

    async def route(self, items: list[ScrapedItem]) -> dict:
        """Route items to their destinations.

        Returns detailed routing results.
        """
        dragonscope_items = []
        liquifi_items = []

        for item in items:
            dest = self.classify(item)
            if dest == DestinationTag.DRAGONSCOPE:
                dragonscope_items.append(item)
            elif dest == DestinationTag.LIQUIFI:
                liquifi_items.append(item)
            elif dest == DestinationTag.BOTH:
                dragonscope_items.append(item)
                liquifi_items.append(item)

        self._stats["total_routed"] += len(items)

        # Push to destinations in parallel
        results = {}
        coros = {}

        if dragonscope_items:
            coros["dragonscope"] = self.dragonscope.push(dragonscope_items)
        if liquifi_items:
            coros["liquifi"] = self.liquifi.push(liquifi_items)

        if coros:
            gathered = await asyncio.gather(
                *coros.values(), return_exceptions=True
            )
            for name, result in zip(coros.keys(), gathered):
                if isinstance(result, Exception):
                    logger.error(f"[Router] {name} push failed: {result}")
                    results[name] = {"error": str(result)}
                    self._stats["errors"] += 1
                else:
                    results[name] = result
                    # Only count items if push actually succeeded
                    if name == "dragonscope":
                        # DragonScope returns {cat: {success, count}}
                        push_ok = isinstance(result, dict) and all(
                            v.get("success", True)
                            for v in result.values()
                            if isinstance(v, dict)
                        )
                        if push_ok:
                            self._stats["dragonscope_pushed"] += len(dragonscope_items)
                    elif name == "liquifi":
                        # LiquiFi returns {success, pushed, filtered_out}
                        if isinstance(result, dict) and result.get("success", False):
                            self._stats["liquifi_pushed"] += result.get("pushed", 0)

        # Publish all to Kafka for persistence
        await self._publish_to_kafka(items, "all")

        logger.info(
            f"[Router] Routed {len(items)} items: "
            f"DragonScope={len(dragonscope_items)}, LiquiFi={len(liquifi_items)}"
        )

        return {
            "total": len(items),
            "dragonscope": len(dragonscope_items),
            "liquifi": len(liquifi_items),
            "results": results,
            "stats": self._stats,
        }

    async def close(self):
        """Close underlying connector resources including Kafka producer."""
        try:
            await self.dragonscope.close()
        except Exception as e:
            logger.debug(f"[Router] DragonScope close error: {e}")
        try:
            await self.liquifi.close()
        except Exception as e:
            logger.debug(f"[Router] LiquiFi close error: {e}")
        if self.kafka_producer is not None:
            try:
                self.kafka_producer.flush(timeout=5)
                self.kafka_producer.close(timeout=5)
                logger.debug("[Router] Kafka producer closed")
            except Exception as e:
                logger.debug(f"[Router] Kafka producer close error: {e}")
            self.kafka_producer = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    @property
    def stats(self) -> dict:
        return {**self._stats}
