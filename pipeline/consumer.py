"""Kafka consumer — processes raw posts, enriches, and stores in PostgreSQL."""

import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

from pipeline.transforms import enrich_item

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "raw-posts"
TOPIC_ENRICHED = "enriched-posts"
TOPIC_ANALYSIS = "analysis-results"
GROUP_ID = "scraper-enrichment"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def create_enrichment_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def run_consumer():
    """Main consumer loop — reads raw posts, enriches, publishes to enriched topic, stores in DB."""
    consumer = create_consumer()
    producer = create_enrichment_producer()

    # Lazy DB import to avoid import at module level
    from api.database import SessionLocal
    from api.models import ScrapedPost

    print(f"[Consumer] Listening on {TOPIC_RAW}...")

    for message in consumer:
        try:
            envelope = message.value
            platform = envelope.get("platform", "unknown")
            raw_item = envelope.get("item", {})

            # Enrich
            enriched = enrich_item(raw_item)

            # Publish to enriched topic
            key = f"{platform}:{enriched.get('id', 'unknown')}"
            producer.send(TOPIC_ENRICHED, key=key, value={
                "platform": platform,
                "item": enriched,
                "enriched_at": datetime.now(timezone.utc).isoformat(),
            })

            # Store in PostgreSQL
            db = SessionLocal()
            try:
                post = ScrapedPost(
                    platform=platform,
                    platform_id=str(enriched.get("id", "")),
                    text=enriched.get("text", ""),
                    author_username=enriched.get("author_username"),
                    author_display_name=enriched.get("author_display_name", "Unknown"),
                    likes=enriched.get("likes", 0),
                    replies=enriched.get("replies", 0),
                    reposts=enriched.get("reposts", 0),
                    views=enriched.get("views"),
                    hashtags=enriched.get("hashtags", []),
                    mentions=enriched.get("mentions", []),
                    batch_id=enriched.get("batch_id"),
                    created_at=datetime.fromisoformat(enriched["created_at"]) if enriched.get("created_at") else datetime.now(timezone.utc),
                    scraped_at=datetime.now(timezone.utc),
                )
                db.merge(post)
                db.commit()
            finally:
                db.close()

        except Exception as e:
            print(f"[Consumer] Error processing message: {e}")

    consumer.close()
    producer.close()


if __name__ == "__main__":
    run_consumer()
