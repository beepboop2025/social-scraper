"""Kafka producer — publishes scraped items to raw-posts topic."""

import json
import logging
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "raw-posts"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def publish_scraped_item(producer: KafkaProducer, item: dict, platform: str):
    """Publish a single scraped item to the raw-posts topic."""
    key = f"{platform}:{item.get('id', 'unknown')}"
    envelope = {
        "platform": platform,
        "item": item,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC_RAW, key=key, value=envelope)


def publish_batch(producer: KafkaProducer, items: list[dict], platform: str, flush_timeout: float = 30):
    """Publish a batch of scraped items.

    Args:
        flush_timeout: Max seconds to wait for flush. Prevents indefinite blocking
                       when Kafka is unreachable.
    """
    for item in items:
        publish_scraped_item(producer, item, platform)
    remaining = producer.flush(timeout=flush_timeout)
    if remaining > 0:
        logger.warning(f"[Producer] Flush timed out after {flush_timeout}s — {remaining} messages unsent")
    else:
        logger.info(f"[Producer] Published {len(items)} items to {TOPIC_RAW}")
