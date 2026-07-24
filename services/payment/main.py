"""Payment service entrypoint."""

import asyncio
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.health import create_health_router
from shared.kafka import create_producer
from shared.metrics import metrics_endpoint
from shared.middleware import RequestIdMiddleware, setup_logging
from shared.outbox import run_outbox_relay

import db
import routes
from consumers import run_consumer

setup_logging("payment-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await db.create_pool()
    app.state.producer = await create_producer()

    shutdown_event = asyncio.Event()

    consumer_task = asyncio.create_task(
        run_consumer(app.state.db_pool, shutdown_event)
    )
    outbox_task = asyncio.create_task(
        run_outbox_relay(app.state.db_pool, app.state.producer, shutdown_event)
    )

    yield

    shutdown_event.set()
    await asyncio.gather(consumer_task, outbox_task, return_exceptions=True)
    await app.state.producer.stop()
    await app.state.db_pool.close()


app = FastAPI(title="Payment Service", lifespan=lifespan)
app.add_middleware(RequestIdMiddleware)
app.include_router(routes.router)
app.include_router(create_health_router(lambda: app.state.db_pool))
app.add_route("/metrics", metrics_endpoint)
