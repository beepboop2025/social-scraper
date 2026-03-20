"""Kafka consumer — processes raw posts, enriches, and stores in PostgreSQL."""

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

from pipeline.transforms import enrich_item

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "raw-posts"
TOPIC_ENRICHED = "enriched-posts"
TOPIC_ANALYSIS = "analysis-results"
GROUP_ID = "scraper-enrichment"

_shutdown_requested = False


def _safe_parse_datetime(value: str | None) -> datetime:
    """Parse a datetime string, returning utcnow on failure."""
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        logger.warning(f"[Consumer] Unparseable datetime '{value}', using now()")
        return datetime.now(timezone.utc)


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
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
    global _shutdown_requested

    def _handle_signal(sig, frame):
        global _shutdown_requested
        logger.info("[Consumer] Shutdown requested, finishing current message...")
        _shutdown_requested = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer = create_consumer()
    producer = create_enrichment_producer()

    # Lazy DB import to avoid import at module level
    from api.database import SessionLocal
    from api.models import ScrapedPost

    logger.info(f"[Consumer] Listening on {TOPIC_RAW}...")

    try:
        for message in consumer:
            if _shutdown_requested:
                break
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
                producer.flush()

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
                        created_at=_safe_parse_datetime(enriched.get("created_at")),
                        scraped_at=datetime.now(timezone.utc),
                    )
                    db.merge(post)
                    db.commit()
                finally:
                    db.close()

                # Commit offset only after successful processing + storage
                consumer.commit()

            except KeyError as e:
                # Missing required field — skip this message to avoid poison pill
                logger.error(f"[Consumer] Skipping message with missing field {e}: {message.key}")
                consumer.commit()
            except Exception as e:
                logger.error(f"[Consumer] Error processing message: {e}", exc_info=True)
                # Commit offset to prevent poison pill — the message is logged for investigation
                consumer.commit()
    finally:
        producer.flush()
        consumer.close()
        producer.close()
        logger.info("[Consumer] Shut down cleanly.")


if __name__ == "__main__":
    run_consumer()
