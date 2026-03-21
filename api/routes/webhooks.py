"""Webhook management API routes — register, list, delete, test webhooks."""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, HttpUrl

from core.webhooks import WebhookManager, EVENT_TYPES

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])


class WebhookRegister(BaseModel):
    url: str
    events: list[str]
    name: str = ""


class WebhookFireEvent(BaseModel):
    event_type: str
    payload: dict = {}


@router.post("")
async def register_webhook(body: WebhookRegister):
    """Register a new webhook for specific events.

    Valid events: new_article, high_sentiment, anomaly_detected, source_down
    """
    try:
        mgr = WebhookManager()
        config = mgr.register(url=body.url, events=body.events, name=body.name)
        return {
            "webhook_id": config.webhook_id,
            "url": config.url,
            "events": config.events,
            "secret": config.secret,
            "note": "Save the secret — use it to verify webhook signatures.",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def list_webhooks():
    """List all registered webhooks."""
    mgr = WebhookManager()
    webhooks = mgr.list_webhooks()
    return {"webhooks": webhooks, "total": len(webhooks)}


@router.delete("/{webhook_id}")
async def delete_webhook(webhook_id: str):
    """Delete a registered webhook."""
    mgr = WebhookManager()
    deleted = mgr.delete(webhook_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Webhook {webhook_id} not found")
    return {"deleted": True, "webhook_id": webhook_id}


@router.post("/{webhook_id}/test")
async def test_webhook(webhook_id: str):
    """Send a test event to a webhook to verify connectivity."""
    mgr = WebhookManager()
    result = mgr.test_webhook(webhook_id)
    if "error" in result and result.get("error") == "Webhook not found":
        raise HTTPException(status_code=404, detail="Webhook not found")
    return result


@router.get("/{webhook_id}/deliveries")
async def webhook_deliveries(webhook_id: str, limit: int = 20):
    """Get recent delivery history for a webhook."""
    mgr = WebhookManager()
    webhook = mgr.get_webhook(webhook_id)
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    history = mgr.get_delivery_history(webhook_id, limit=limit)
    return {"webhook_id": webhook_id, "deliveries": history}


@router.post("/fire")
async def fire_webhook_event(body: WebhookFireEvent):
    """Manually fire a webhook event (admin/testing)."""
    if body.event_type not in EVENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event type. Valid: {sorted(EVENT_TYPES)}",
        )
    mgr = WebhookManager()
    result = mgr.deliver(body.event_type, body.payload)
    return result


@router.get("/events")
async def list_event_types():
    """List all available webhook event types."""
    return {"event_types": sorted(EVENT_TYPES)}
