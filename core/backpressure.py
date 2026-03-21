"""Backpressure management — monitors queue depths and throttles producers.

Levels:
- NORMAL: queue < 1000 items -> full speed
- WARN: 1000-5000 -> reduce scraping frequency by 50%
- CRITICAL: >5000 -> pause all scraping, let consumers catch up

State is stored in Redis and checked by scraper tasks before execution.
"""

import json
import logging
import os
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class PressureLevel(str, Enum):
    NORMAL = "normal"
    WARN = "warn"
    CRITICAL = "critical"


# Thresholds
WARN_THRESHOLD = 1000
CRITICAL_THRESHOLD = 5000

# Redis key for persisting backpressure state
STATE_KEY = "backpressure:state"


class BackpressureManager:
    """Monitors Celery, Kafka, and Redis queue depths and signals producers to slow down.

    Usage in scraper tasks:
        bp = BackpressureManager()
        level = bp.current_level()
        if level == PressureLevel.CRITICAL:
            return {"skipped": True, "reason": "backpressure_critical"}
        if level == PressureLevel.WARN:
            # reduce batch size or skip non-critical sources
            ...
    """

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or REDIS_URL
        self._last_level = PressureLevel.NORMAL
        self._last_check = 0.0
        self._check_interval = 10  # seconds between re-checks

    def _get_redis(self):
        import redis
        return redis.from_url(self.redis_url, decode_responses=True)

    def _measure_celery_depth(self, r) -> int:
        """Count pending tasks across known Celery queues."""
        total = 0
        queues = ["collectors", "processors", "routing", "health", "celery"]
        for q in queues:
            try:
                depth = r.llen(q)
                total += depth
            except Exception:
                pass
        return total

    def _measure_kafka_lag(self) -> int:
        """Estimate Kafka consumer lag. Returns 0 if Kafka is not available."""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                group_id="scraper-enrichment",
                enable_auto_commit=False,
            )
            partitions = consumer.assignment()
            lag = 0
            if partitions:
                end_offsets = consumer.end_offsets(partitions)
                committed = {tp: consumer.committed(tp) or 0 for tp in partitions}
                for tp in partitions:
                    lag += end_offsets.get(tp, 0) - committed.get(tp, 0)
            consumer.close()
            return lag
        except Exception:
            return 0

    def _measure_redis_pending(self, r) -> int:
        """Count items in Redis processing streams/lists."""
        total = 0
        try:
            # Check dedup queue size as proxy for processing backlog
            keys = list(r.scan_iter("dedup:url:*", count=100))
            total += len(keys)
        except Exception:
            pass
        return total

    def check(self) -> dict:
        """Perform a full backpressure check. Returns state dict with level and depths."""
        try:
            r = self._get_redis()
            try:
                celery_depth = self._measure_celery_depth(r)
                kafka_lag = self._measure_kafka_lag()
                redis_pending = self._measure_redis_pending(r)

                total_depth = celery_depth + kafka_lag

                if total_depth > CRITICAL_THRESHOLD:
                    level = PressureLevel.CRITICAL
                elif total_depth > WARN_THRESHOLD:
                    level = PressureLevel.WARN
                else:
                    level = PressureLevel.NORMAL

                state = {
                    "level": level.value,
                    "total_depth": total_depth,
                    "celery_depth": celery_depth,
                    "kafka_lag": kafka_lag,
                    "redis_pending": redis_pending,
                    "thresholds": {
                        "warn": WARN_THRESHOLD,
                        "critical": CRITICAL_THRESHOLD,
                    },
                    "checked_at": time.time(),
                }

                # Log state transitions
                prev = self._last_level
                if level != prev:
                    logger.warning(
                        f"[Backpressure] {prev.value} -> {level.value} "
                        f"(total_depth={total_depth}, celery={celery_depth}, kafka={kafka_lag})"
                    )
                self._last_level = level

                # Persist to Redis for other workers
                r.set(STATE_KEY, json.dumps(state), ex=60)

                return state
            finally:
                r.close()
        except Exception as e:
            logger.error(f"[Backpressure] Check failed: {e}")
            return {
                "level": PressureLevel.NORMAL.value,
                "total_depth": 0,
                "error": str(e),
                "checked_at": time.time(),
            }

    def current_level(self) -> PressureLevel:
        """Get current backpressure level (cached for _check_interval seconds)."""
        now = time.time()
        if now - self._last_check < self._check_interval:
            return self._last_level

        self._last_check = now

        # Try reading cached state from Redis first
        try:
            r = self._get_redis()
            try:
                cached = r.get(STATE_KEY)
                if cached:
                    data = json.loads(cached)
                    age = now - data.get("checked_at", 0)
                    if age < 30:  # Accept cached state if < 30s old
                        self._last_level = PressureLevel(data["level"])
                        return self._last_level
            finally:
                r.close()
        except Exception:
            pass

        # Full check if cache is stale
        state = self.check()
        return PressureLevel(state["level"])

    def should_skip_scraper(self, scraper_name: str, is_critical: bool = False) -> bool:
        """Determine if a scraper should skip this run due to backpressure.

        Critical scrapers (RSS, central_banks) continue in WARN state.
        All scrapers pause in CRITICAL state.
        """
        level = self.current_level()

        if level == PressureLevel.NORMAL:
            return False

        if level == PressureLevel.CRITICAL:
            logger.info(f"[Backpressure] Skipping {scraper_name} — CRITICAL pressure")
            return True

        # WARN: skip non-critical scrapers
        if level == PressureLevel.WARN and not is_critical:
            logger.info(f"[Backpressure] Skipping {scraper_name} — WARN pressure (non-critical)")
            return True

        return False

    def get_throttle_factor(self) -> float:
        """Return a multiplier for scraping frequency.

        NORMAL: 1.0 (full speed)
        WARN: 0.5 (half speed)
        CRITICAL: 0.0 (paused)
        """
        level = self.current_level()
        if level == PressureLevel.CRITICAL:
            return 0.0
        elif level == PressureLevel.WARN:
            return 0.5
        return 1.0


# Module-level singleton
_manager: Optional[BackpressureManager] = None


def get_backpressure_manager() -> BackpressureManager:
    """Get or create the singleton BackpressureManager."""
    global _manager
    if _manager is None:
        _manager = BackpressureManager()
    return _manager
