"""API key authentication and rate limiting middleware + management endpoints.

Features:
- API key authentication via X-API-Key header
- Per-key rate limiting (default 100 req/min)
- Key management: create, list, revoke
- Proper 401/403/429 responses
"""

import hashlib
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
API_KEYS_HASH = "api:keys"
RATE_LIMIT_PREFIX = "api:rate:"
ADMIN_KEY = os.getenv("API_ADMIN_KEY", "")  # Master admin key from env

router = APIRouter(prefix="/auth", tags=["auth"])


def _hash_key(raw_key: str) -> str:
    """SHA-256 hash of an API key for storage."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _get_redis():
    import redis
    return redis.from_url(REDIS_URL, decode_responses=True)


class APIKeyCreate(BaseModel):
    name: str
    permissions: list[str] = ["read"]
    rate_limit: int = 100  # requests per minute
    expires_in_days: Optional[int] = 365


class APIKeyInfo(BaseModel):
    key_id: str
    name: str
    permissions: list[str]
    rate_limit: int
    created_at: str
    expires_at: Optional[str]
    last_used: Optional[str] = None
    request_count: int = 0


def validate_api_key(request: Request) -> dict:
    """FastAPI dependency: validate API key from X-API-Key header.

    Returns the key metadata dict if valid.
    Raises HTTPException(401) if missing/invalid, 429 if rate limited.
    """
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    key_hash = _hash_key(api_key)

    try:
        r = _get_redis()
        try:
            raw = r.hget(API_KEYS_HASH, key_hash)
            if not raw:
                raise HTTPException(status_code=401, detail="Invalid API key")

            key_data = json.loads(raw)

            # Check expiration
            expires_at = key_data.get("expires_at")
            if expires_at:
                exp = datetime.fromisoformat(expires_at)
                if datetime.now(timezone.utc) > exp:
                    raise HTTPException(status_code=401, detail="API key expired")

            # Rate limiting
            rate_limit = key_data.get("rate_limit", 100)
            rate_key = f"{RATE_LIMIT_PREFIX}{key_hash}"
            current = r.incr(rate_key)
            if current == 1:
                r.expire(rate_key, 60)  # 60-second window

            if current > rate_limit:
                ttl = r.ttl(rate_key)
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded ({rate_limit}/min). Retry in {ttl}s.",
                    headers={"Retry-After": str(max(ttl, 1))},
                )

            # Update usage stats
            key_data["last_used"] = datetime.now(timezone.utc).isoformat()
            key_data["request_count"] = key_data.get("request_count", 0) + 1
            r.hset(API_KEYS_HASH, key_hash, json.dumps(key_data))

            return key_data
        finally:
            r.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Auth] Key validation error: {e}")
        raise HTTPException(status_code=500, detail="Authentication service error")


def require_permission(permission: str):
    """Create a dependency that checks for a specific permission."""
    def _check(key_data: dict = Depends(validate_api_key)):
        perms = key_data.get("permissions", [])
        if "admin" in perms:
            return key_data
        if permission not in perms:
            raise HTTPException(
                status_code=403,
                detail=f"Missing required permission: {permission}",
            )
        return key_data
    return _check


def _check_admin(request: Request):
    """Verify the request has admin privileges (admin key or admin permission)."""
    api_key = request.headers.get("X-API-Key", "")

    # Check if it's the master admin key
    if ADMIN_KEY and api_key == ADMIN_KEY:
        return {"name": "admin", "permissions": ["admin"]}

    # Otherwise validate normally and check permissions
    key_data = validate_api_key(request)
    if "admin" not in key_data.get("permissions", []):
        raise HTTPException(status_code=403, detail="Admin access required")
    return key_data


# ── Endpoints ──────────────────────────────────────────────


@router.post("/keys")
async def create_api_key(body: APIKeyCreate, admin: dict = Depends(_check_admin)):
    """Create a new API key. Requires admin access."""
    raw_key = f"ess_{secrets.token_hex(24)}"
    key_hash = _hash_key(raw_key)

    now = datetime.now(timezone.utc)
    expires_at = None
    if body.expires_in_days:
        from datetime import timedelta
        expires_at = (now + timedelta(days=body.expires_in_days)).isoformat()

    key_data = {
        "key_id": key_hash[:16],
        "name": body.name,
        "key_hash": key_hash,
        "permissions": body.permissions,
        "rate_limit": body.rate_limit,
        "created_at": now.isoformat(),
        "expires_at": expires_at,
        "request_count": 0,
    }

    r = _get_redis()
    try:
        r.hset(API_KEYS_HASH, key_hash, json.dumps(key_data))
    finally:
        r.close()

    logger.info(f"[Auth] Created API key '{body.name}' (id={key_hash[:16]})")

    return {
        "key": raw_key,
        "key_id": key_hash[:16],
        "name": body.name,
        "permissions": body.permissions,
        "rate_limit": body.rate_limit,
        "expires_at": expires_at,
        "note": "Save this key — it cannot be retrieved again.",
    }


@router.get("/keys")
async def list_api_keys(admin: dict = Depends(_check_admin)):
    """List all API keys (admin only). Keys are not shown."""
    r = _get_redis()
    try:
        raw = r.hgetall(API_KEYS_HASH)
        keys = []
        for key_hash, data in raw.items():
            info = json.loads(data)
            keys.append({
                "key_id": info.get("key_id", key_hash[:16]),
                "name": info.get("name", ""),
                "permissions": info.get("permissions", []),
                "rate_limit": info.get("rate_limit", 100),
                "created_at": info.get("created_at", ""),
                "expires_at": info.get("expires_at"),
                "last_used": info.get("last_used"),
                "request_count": info.get("request_count", 0),
            })
        return {"keys": keys, "total": len(keys)}
    finally:
        r.close()


@router.delete("/keys/{key_id}")
async def revoke_api_key(key_id: str, admin: dict = Depends(_check_admin)):
    """Revoke an API key by its key_id. Requires admin access."""
    r = _get_redis()
    try:
        raw = r.hgetall(API_KEYS_HASH)
        for key_hash, data in raw.items():
            info = json.loads(data)
            if info.get("key_id") == key_id:
                r.hdel(API_KEYS_HASH, key_hash)
                logger.info(f"[Auth] Revoked API key '{info.get('name')}' (id={key_id})")
                return {"deleted": True, "key_id": key_id, "name": info.get("name")}

        raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
    finally:
        r.close()
