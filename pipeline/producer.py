"""Kafka producer — publishes scraped items to raw-posts topic."""

import json
import os
from datetime import datetime
from kafka import KafkaProducer

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
        "published_at": datetime.utcnow().isoformat(),
    }
    producer.send(TOPIC_RAW, key=key, value=envelope)


def publish_batch(producer: KafkaProducer, items: list[dict], platform: str):
    """Publish a batch of scraped items."""
    for item in items:
        publish_scraped_item(producer, item, platform)
    producer.flush()
    print(f"[Producer] Published {len(items)} items to {TOPIC_RAW}")
