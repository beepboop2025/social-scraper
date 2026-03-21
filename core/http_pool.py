"""Singleton async HTTPx client pool for all scrapers.

Provides connection pooling with configurable limits, proper lifecycle
management, and a shared client that all scrapers can reuse instead of
creating their own connections.

Usage:
    from core.http_pool import get_http_client, shutdown_http_pool

    client = get_http_client()  # returns shared AsyncClient
    resp = await client.get(url)

Call shutdown_http_pool() during application shutdown.
"""

import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_pool: Optional[httpx.AsyncClient] = None
_lock = asyncio.Lock()


def _create_pool() -> httpx.AsyncClient:
    """Create a new connection-pooled AsyncClient."""
    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    )
    transport = httpx.AsyncHTTPTransport(
        limits=limits,
        retries=1,
    )
    client = httpx.AsyncClient(
        transport=transport,
        timeout=httpx.Timeout(30.0, connect=10.0),
        headers={"User-Agent": "SocialScraper/3.0"},
        follow_redirects=True,
    )
    logger.info("[HTTPPool] Created shared AsyncClient pool (max=100, keepalive=20)")
    return client


async def get_http_client() -> httpx.AsyncClient:
    """Get the shared AsyncClient, creating it if needed.

    Thread-safe via asyncio.Lock. The client is created lazily on first call.
    """
    global _pool
    if _pool is not None and not _pool.is_closed:
        return _pool

    async with _lock:
        # Double-check after acquiring lock
        if _pool is not None and not _pool.is_closed:
            return _pool
        _pool = _create_pool()
        return _pool


async def shutdown_http_pool():
    """Close the shared client pool. Call during application shutdown."""
    global _pool
    if _pool is not None and not _pool.is_closed:
        await _pool.aclose()
        logger.info("[HTTPPool] Shared AsyncClient pool closed")
    _pool = None


def get_http_client_sync() -> httpx.AsyncClient:
    """Non-async accessor for contexts where the pool is already initialized.

    Returns the existing pool or creates one. Prefer get_http_client() in async code.
    """
    global _pool
    if _pool is None or _pool.is_closed:
        _pool = _create_pool()
    return _pool
