"""Webhook delivery system — event-driven notifications to external URLs.

Events:
- new_article: High-quality article collected
- high_sentiment: Extreme positive/negative sentiment detected
- anomaly_detected: Unusual data pattern
- source_down: A data source has stopped producing

Features:
- Retry with exponential backoff (3 attempts)
- Delivery status tracking (pending, delivered, failed)
- Webhook configs stored in database via Redis (lightweight)
- Test endpoint for verification
"""

import hashlib
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
WEBHOOK_PREFIX = "webhook:"
DELIVERY_PREFIX = "webhook_delivery:"

# Supported event types
EVENT_TYPES = {"new_article", "high_sentiment", "anomaly_detected", "source_down"}

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5  # seconds: 5, 10, 20


class WebhookConfig:
    """A registered webhook configuration."""

    def __init__(
        self,
        webhook_id: str,
        url: str,
        events: list[str],
        name: str = "",
        secret: str = "",
        active: bool = True,
        created_at: Optional[str] = None,
    ):
        self.webhook_id = webhook_id
        self.url = url
        self.events = events
        self.name = name
        self.secret = secret
        self.active = active
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "webhook_id": self.webhook_id,
            "url": self.url,
            "events": self.events,
            "name": self.name,
            "secret": self.secret,
            "active": self.active,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WebhookConfig":
        return cls(**data)


class WebhookManager:
    """Manages webhook registration, delivery, and retry."""

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or REDIS_URL

    def _get_redis(self):
        import redis
        return redis.from_url(self.redis_url, decode_responses=True)

    def register(self, url: str, events: list[str], name: str = "") -> WebhookConfig:
        """Register a new webhook. Returns the config with generated ID and secret."""
        # Validate events
        invalid = set(events) - EVENT_TYPES
        if invalid:
            raise ValueError(f"Invalid event types: {invalid}. Valid: {EVENT_TYPES}")

        webhook_id = secrets.token_hex(8)
        secret = secrets.token_hex(16)

        config = WebhookConfig(
            webhook_id=webhook_id,
            url=url,
            events=events,
            name=name,
            secret=secret,
        )

        r = self._get_redis()
        try:
            r.hset(f"{WEBHOOK_PREFIX}configs", webhook_id, json.dumps(config.to_dict()))
            logger.info(f"[Webhooks] Registered webhook {webhook_id} for events {events} -> {url}")
        finally:
            r.close()

        return config

    def delete(self, webhook_id: str) -> bool:
        """Delete a registered webhook."""
        r = self._get_redis()
        try:
            removed = r.hdel(f"{WEBHOOK_PREFIX}configs", webhook_id)
            if removed:
                logger.info(f"[Webhooks] Deleted webhook {webhook_id}")
            return bool(removed)
        finally:
            r.close()

    def list_webhooks(self) -> list[dict]:
        """List all registered webhooks."""
        r = self._get_redis()
        try:
            raw = r.hgetall(f"{WEBHOOK_PREFIX}configs")
            webhooks = []
            for wid, data in raw.items():
                cfg = json.loads(data)
                # Mask the secret for listing
                cfg["secret"] = cfg["secret"][:8] + "..." if cfg.get("secret") else ""
                webhooks.append(cfg)
            return webhooks
        finally:
            r.close()

    def get_webhook(self, webhook_id: str) -> Optional[WebhookConfig]:
        """Get a single webhook config by ID."""
        r = self._get_redis()
        try:
            data = r.hget(f"{WEBHOOK_PREFIX}configs", webhook_id)
            if data:
                return WebhookConfig.from_dict(json.loads(data))
            return None
        finally:
            r.close()

    def _get_subscribers(self, event_type: str) -> list[WebhookConfig]:
        """Get all active webhooks subscribed to an event type."""
        r = self._get_redis()
        try:
            raw = r.hgetall(f"{WEBHOOK_PREFIX}configs")
            subscribers = []
            for wid, data in raw.items():
                cfg = WebhookConfig.from_dict(json.loads(data))
                if cfg.active and event_type in cfg.events:
                    subscribers.append(cfg)
            return subscribers
        finally:
            r.close()

    def _sign_payload(self, payload: str, secret: str) -> str:
        """Create HMAC signature for webhook payload."""
        return hashlib.sha256(f"{secret}:{payload}".encode()).hexdigest()

    def _record_delivery(self, webhook_id: str, event_type: str, status: str, response_code: int = 0):
        """Record delivery attempt in Redis."""
        try:
            r = self._get_redis()
            try:
                delivery = {
                    "webhook_id": webhook_id,
                    "event_type": event_type,
                    "status": status,
                    "response_code": response_code,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                key = f"{DELIVERY_PREFIX}{webhook_id}"
                r.lpush(key, json.dumps(delivery))
                r.ltrim(key, 0, 99)  # Keep last 100 deliveries
                r.expire(key, 86400 * 7)  # 7 day TTL
            finally:
                r.close()
        except Exception as e:
            logger.debug(f"[Webhooks] Failed to record delivery: {e}")

    def deliver(self, event_type: str, payload: dict) -> dict:
        """Deliver an event to all subscribed webhooks with retry.

        Returns delivery summary.
        """
        if event_type not in EVENT_TYPES:
            return {"error": f"Unknown event type: {event_type}"}

        subscribers = self._get_subscribers(event_type)
        if not subscribers:
            return {"event": event_type, "subscribers": 0, "delivered": 0}

        results = {"event": event_type, "subscribers": len(subscribers), "delivered": 0, "failed": 0}

        envelope = {
            "event": event_type,
            "data": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        for webhook in subscribers:
            success = self._deliver_to_webhook(webhook, envelope)
            if success:
                results["delivered"] += 1
            else:
                results["failed"] += 1

        return results

    def _deliver_to_webhook(self, webhook: WebhookConfig, envelope: dict) -> bool:
        """Deliver to a single webhook with retry logic."""
        payload_str = json.dumps(envelope, default=str)
        signature = self._sign_payload(payload_str, webhook.secret)

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": envelope["event"],
            "X-Webhook-ID": webhook.webhook_id,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=10) as client:
                    resp = client.post(webhook.url, content=payload_str, headers=headers)

                if resp.status_code < 300:
                    self._record_delivery(
                        webhook.webhook_id, envelope["event"], "delivered", resp.status_code
                    )
                    return True

                logger.warning(
                    f"[Webhooks] {webhook.webhook_id} returned {resp.status_code} "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
            except Exception as e:
                logger.warning(
                    f"[Webhooks] Delivery to {webhook.webhook_id} failed: {e} "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )

            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                time.sleep(backoff)

        self._record_delivery(webhook.webhook_id, envelope["event"], "failed")
        return False

    def test_webhook(self, webhook_id: str) -> dict:
        """Send a test event to a webhook to verify it works."""
        webhook = self.get_webhook(webhook_id)
        if not webhook:
            return {"error": "Webhook not found"}

        test_payload = {
            "message": "This is a test event from EconScraper",
            "webhook_id": webhook_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        envelope = {
            "event": "test",
            "data": test_payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        payload_str = json.dumps(envelope, default=str)
        signature = self._sign_payload(payload_str, webhook.secret)

        try:
            with httpx.Client(timeout=10) as client:
                resp = client.post(
                    webhook.url,
                    content=payload_str,
                    headers={
                        "Content-Type": "application/json",
                        "X-Webhook-Signature": signature,
                        "X-Webhook-Event": "test",
                        "X-Webhook-ID": webhook_id,
                    },
                )
            return {
                "webhook_id": webhook_id,
                "url": webhook.url,
                "status_code": resp.status_code,
                "success": resp.status_code < 300,
            }
        except Exception as e:
            return {
                "webhook_id": webhook_id,
                "url": webhook.url,
                "error": str(e),
                "success": False,
            }

    def get_delivery_history(self, webhook_id: str, limit: int = 20) -> list[dict]:
        """Get recent delivery history for a webhook."""
        try:
            r = self._get_redis()
            try:
                key = f"{DELIVERY_PREFIX}{webhook_id}"
                raw = r.lrange(key, 0, limit - 1)
                return [json.loads(d) for d in raw]
            finally:
                r.close()
        except Exception:
            return []


# Convenience functions for firing events from anywhere in the codebase

def fire_event(event_type: str, payload: dict) -> dict:
    """Fire a webhook event. Safe to call even if no webhooks are registered."""
    try:
        mgr = WebhookManager()
        return mgr.deliver(event_type, payload)
    except Exception as e:
        logger.debug(f"[Webhooks] fire_event failed: {e}")
        return {"error": str(e)}
