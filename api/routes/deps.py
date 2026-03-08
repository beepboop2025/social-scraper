"""Shared API dependencies for econscraper v4."""

import os
from functools import lru_cache

from api.database import get_db


@lru_cache
def get_vector_store():
    from storage.vectors import VectorStore
    return VectorStore()


@lru_cache
def get_embedder():
    from processors.embedder import Embedder
    return Embedder()


def get_redis():
    """Yield a Redis client."""
    import redis
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
    try:
        yield r
    finally:
        r.close()
