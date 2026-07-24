"""Redis client for the Auth service.

Redis is used ONLY by the Auth service (for rate limiting) and lives
here rather than in the shared package. On connection failure, callers
should fail open — rate limiting is an optimization, not a requirement.
"""

import os
from typing import Optional

import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

_redis_client: Optional[redis.Redis] = None


async def get_redis() -> Optional[redis.Redis]:
    """Get the Redis client, or None if unavailable.

    Returns None on connection failure — callers should fail open.
    """
    global _redis_client

    if _redis_client is None:
        try:
            _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            await _redis_client.ping()
            logger.info("redis_connected")
        except Exception as e:
            logger.warning("redis_connection_failed", error=str(e))
            _redis_client = None
            return None

    return _redis_client


async def close_redis():
    """Close the Redis connection if open."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None
