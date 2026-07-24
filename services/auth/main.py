"""Auth service entrypoint.

Responsibilities:
  - User registration and login
  - JWT issuance and refresh
  - Redis-based rate limiting (fail-open on Redis failure)

This service does NOT publish Kafka events and does NOT use the
transactional outbox pattern — it has no domain events to emit.
"""

import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

# Add project root to path so shared package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.health import create_health_router
from shared.metrics import metrics_endpoint
from shared.middleware import RequestIdMiddleware, setup_logging

import db
import routes
from rate_limiter import RateLimitMiddleware
from redis_client import close_redis

setup_logging("auth-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown resources."""
    # Startup
    app.state.db_pool = await db.create_pool()

    yield

    # Shutdown
    await close_redis()
    await app.state.db_pool.close()


app = FastAPI(title="Auth Service", lifespan=lifespan)

# Middleware (applied in reverse order — last added runs first)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(RequestIdMiddleware)

# Routes
app.include_router(routes.router)
app.include_router(create_health_router(lambda: app.state.db_pool))
app.add_route("/metrics", metrics_endpoint)
