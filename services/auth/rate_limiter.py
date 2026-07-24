"""Redis-based rate limiting with fail-open behavior.

Uses a sliding window counter in Redis. If Redis is unavailable,
rate limiting is disabled entirely (fail open) — a warning is logged
but authentication and all business functionality continues working.

Why fail open:
- Rate limiting is a protection mechanism, not a business requirement.
- An in-memory fallback adds complexity without meaningful value
  in a single-instance setup.
- A brief period without rate limiting is acceptable; sustained
  Redis failure would trigger alerts in production.
"""

import structlog
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from redis_client import get_redis

logger = structlog.get_logger()

RATE_LIMIT = 30  # max requests per window
WINDOW_SECONDS = 60


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limits requests by client IP using Redis."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        client_ip = request.client.host if request.client else "unknown"
        redis_client = await get_redis()

        if redis_client is None:
            # Redis unavailable — fail open, skip rate limiting
            logger.warning(
                "rate_limiting_disabled",
                reason="redis_unavailable",
                client_ip=client_ip,
            )
            return await call_next(request)

        try:
            key = f"rate_limit:{client_ip}"
            current = await redis_client.incr(key)

            if current == 1:
                # First request in this window — set expiry
                await redis_client.expire(key, WINDOW_SECONDS)

            if current > RATE_LIMIT:
                logger.warning(
                    "rate_limit_exceeded",
                    client_ip=client_ip,
                    count=current,
                )
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded. Try again later.",
                )
        except HTTPException:
            raise
        except Exception as e:
            # Any Redis error — fail open
            logger.warning(
                "rate_limiting_error",
                reason=str(e),
                client_ip=client_ip,
            )

        return await call_next(request)
