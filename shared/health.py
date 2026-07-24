"""Health check endpoint factory.

Each service calls create_health_router() with its DB pool getter
and includes the router in its FastAPI app.
"""

from typing import Callable

import structlog
from asyncpg import Pool
from fastapi import APIRouter

logger = structlog.get_logger()


def create_health_router(get_db_pool: Callable[[], Pool]) -> APIRouter:
    """Create a /health router that checks database connectivity.

    Args:
        get_db_pool: Callable that returns the service's asyncpg pool.
                     Typically a lambda: lambda: app.state.db_pool
    """
    router = APIRouter()

    @router.get("/health")
    async def health_check():
        checks = {}

        # Database check
        try:
            pool = get_db_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            checks["database"] = "healthy"
        except Exception as e:
            checks["database"] = f"unhealthy: {e}"
            logger.warning("health_check_db_failed", error=str(e))

        overall = all(v == "healthy" for v in checks.values())
        status_code = 200 if overall else 503

        return {
            "status": "healthy" if overall else "unhealthy",
            "checks": checks,
        }

    return router
