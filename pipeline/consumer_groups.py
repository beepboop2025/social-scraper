"""Kafka consumer group management — lag monitoring, DLQ, message replay.

Features:
- Consumer group configuration with multiple topic support
- Consumer lag monitoring per partition
- Dead letter queue for failed messages
- Message replay from specific offsets
- Stats exposed via health endpoint
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics
TOPIC_RAW = "raw-posts"
TOPIC_ENRICHED = "enriched-posts"
TOPIC_ANALYSIS = "analysis-results"
TOPIC_DLQ = "dead-letter-queue"

# Consumer groups
GROUPS = {
    "scraper-enrichment": {
        "topics": [TOPIC_RAW],
        "description": "Enriches raw posts with NLP and financial analysis",
    },
    "analysis-pipeline": {
        "topics": [TOPIC_ENRICHED],
        "description": "Runs sentiment, NER, and topic classification",
    },
    "routing-pipeline": {
        "topics": [TOPIC_ANALYSIS],
        "description": "Routes analyzed content to DragonScope/LiquiFi",
    },
}


class ConsumerGroupManager:
    """Manages Kafka consumer groups, lag monitoring, and DLQ."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP
        self._admin = None
        self._dlq_producer = None

    def _get_admin(self) -> KafkaAdminClient:
        """Lazy admin client."""
        if self._admin is None:
            self._admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="econscraper-admin",
            )
        return self._admin

    def _get_dlq_producer(self) -> KafkaProducer:
        """Lazy DLQ producer."""
        if self._dlq_producer is None:
            self._dlq_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        return self._dlq_producer

    def ensure_topics(self):
        """Create required topics if they don't exist."""
        admin = self._get_admin()
        existing = admin.list_topics()

        topics_to_create = []
        for topic in [TOPIC_RAW, TOPIC_ENRICHED, TOPIC_ANALYSIS, TOPIC_DLQ]:
            if topic not in existing:
                topics_to_create.append(
                    NewTopic(name=topic, num_partitions=3, replication_factor=1)
                )

        if topics_to_create:
            try:
                admin.create_topics(topics_to_create)
                logger.info(f"[KafkaGroups] Created topics: {[t.name for t in topics_to_create]}")
            except Exception as e:
                logger.warning(f"[KafkaGroups] Topic creation error (may already exist): {e}")

    def get_consumer_lag(self, group_id: Optional[str] = None) -> dict:
        """Get consumer lag for all groups or a specific group.

        Returns per-partition lag: end_offset - committed_offset.
        """
        groups = {group_id: GROUPS[group_id]} if group_id and group_id in GROUPS else GROUPS
        result = {}

        for gid, config in groups.items():
            consumer = None
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=gid,
                    enable_auto_commit=False,
                )

                group_lag = {"topics": {}, "total_lag": 0}

                for topic in config["topics"]:
                    partitions = consumer.partitions_for_topic(topic)
                    if not partitions:
                        continue

                    tps = [TopicPartition(topic, p) for p in partitions]
                    end_offsets = consumer.end_offsets(tps)

                    topic_lag = {}
                    for tp in tps:
                        committed = consumer.committed(tp) or 0
                        end = end_offsets.get(tp, 0)
                        lag = max(0, end - committed)
                        topic_lag[tp.partition] = {
                            "committed": committed,
                            "end_offset": end,
                            "lag": lag,
                        }
                        group_lag["total_lag"] += lag

                    group_lag["topics"][topic] = topic_lag

                result[gid] = group_lag

            except Exception as e:
                result[gid] = {"error": str(e), "total_lag": -1}
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

        return result

    def send_to_dlq(self, original_topic: str, key: str, value: dict, error: str):
        """Send a failed message to the dead letter queue."""
        try:
            producer = self._get_dlq_producer()
            dlq_message = {
                "original_topic": original_topic,
                "original_key": key,
                "original_value": value,
                "error": error,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(TOPIC_DLQ, key=f"dlq:{key}", value=dlq_message)
            producer.flush()
            logger.info(f"[DLQ] Sent failed message from {original_topic} to DLQ: {error[:100]}")
        except Exception as e:
            logger.error(f"[DLQ] Failed to send to DLQ: {e}")

    def get_dlq_messages(self, limit: int = 50) -> list[dict]:
        """Read messages from the dead letter queue (peek, no commit)."""
        try:
            consumer = KafkaConsumer(
                TOPIC_DLQ,
                bootstrap_servers=self.bootstrap_servers,
                group_id="dlq-reader",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            messages = []
            for msg in consumer:
                messages.append({
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode("utf-8") if msg.key else None,
                    "value": msg.value,
                    "timestamp": msg.timestamp,
                })
                if len(messages) >= limit:
                    break

            consumer.close()
            return messages
        except Exception as e:
            logger.error(f"[DLQ] Read failed: {e}")
            return []

    def replay_messages(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
        from_offset: int = 0,
        limit: int = 100,
    ) -> dict:
        """Replay messages from a specific offset on a topic/partition.

        Re-processes by sending them back to the raw-posts topic.
        """
        try:
            tp = TopicPartition(topic, partition)
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=f"{group_id}-replay",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            consumer.assign([tp])
            consumer.seek(tp, from_offset)

            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )

            replayed = 0
            for msg in consumer:
                if replayed >= limit:
                    break
                # Republish to the original topic for reprocessing
                producer.send(
                    TOPIC_RAW,
                    key=msg.key.decode("utf-8") if msg.key else None,
                    value=msg.value,
                )
                replayed += 1

            producer.flush()
            producer.close()
            consumer.close()

            logger.info(
                f"[Replay] Replayed {replayed} messages from {topic}:{partition} "
                f"offset {from_offset}"
            )
            return {
                "topic": topic,
                "partition": partition,
                "from_offset": from_offset,
                "replayed": replayed,
            }
        except Exception as e:
            logger.error(f"[Replay] Failed: {e}")
            return {"error": str(e)}

    def health(self) -> dict:
        """Get Kafka consumer group health stats."""
        try:
            lag_info = self.get_consumer_lag()
            total_lag = sum(
                g.get("total_lag", 0) for g in lag_info.values() if isinstance(g, dict)
            )

            # DLQ depth — subtract beginning offsets from end offsets to get actual message count
            dlq_depth = 0
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    group_id="dlq-health",
                    enable_auto_commit=False,
                )
                try:
                    partitions = consumer.partitions_for_topic(TOPIC_DLQ) or set()
                    if partitions:
                        tps = [TopicPartition(TOPIC_DLQ, p) for p in partitions]
                        end_offsets = consumer.end_offsets(tps)
                        begin_offsets = consumer.beginning_offsets(tps)
                        dlq_depth = sum(
                            end_offsets.get(tp, 0) - begin_offsets.get(tp, 0) for tp in tps
                        )
                finally:
                    consumer.close()
            except Exception:
                pass

            return {
                "status": "healthy" if total_lag < 5000 else "lagging",
                "total_lag": total_lag,
                "groups": lag_info,
                "dlq_depth": dlq_depth,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def close(self):
        """Close resources."""
        if self._admin:
            try:
                self._admin.close()
            except Exception:
                pass
        if self._dlq_producer:
            try:
                self._dlq_producer.close()
            except Exception:
                pass
