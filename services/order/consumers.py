"""Order service Kafka consumer.

Owns its own consumer instance — no shared base class.
Subscribes to: payment.succeeded, payment.failed, stock.rejected

Each message is processed inside a single database transaction that
includes the idempotency check (processed_events insert) and the
business logic (order state transition + optional compensation events).

On payment failure, the order service also writes a stock.release
outbox event to trigger the compensation flow.
"""

import asyncio
import json
import uuid

import structlog
from asyncpg import Pool

from shared.events import wrap_event
from shared.kafka import create_consumer
from shared.metrics import kafka_messages_consumed_total
from state_machine import transition_order

logger = structlog.get_logger()


async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):
    """Main consumer loop. Runs until shutdown_event is set."""
    consumer = await create_consumer(
        topics=["payment.succeeded", "payment.failed", "stock.rejected"],
        group_id="order-service",
    )
    logger.info("consumer_started", topics=["payment.succeeded", "payment.failed", "stock.rejected"])

    try:
        while not shutdown_event.is_set():
            # getmany with timeout so we can check shutdown_event periodically
            result = await consumer.getmany(timeout_ms=1000)

            for tp, messages in result.items():
                for msg in messages:
                    try:
                        await _handle_message(db_pool, msg.value)
                        kafka_messages_consumed_total.labels(
                            topic=msg.topic, status="success"
                        ).inc()
                    except Exception:
                        logger.exception(
                            "consumer_error",
                            topic=msg.topic,
                            offset=msg.offset,
                        )
                        kafka_messages_consumed_total.labels(
                            topic=msg.topic, status="error"
                        ).inc()

            # Commit offsets for all successfully processed messages
            if result:
                await consumer.commit()
    finally:
        await consumer.stop()
        logger.info("consumer_stopped")


async def _handle_message(db_pool: Pool, envelope: dict):
    """Process a single event inside a transaction with idempotency check."""
    event_id = uuid.UUID(envelope["event_id"])
    event_type = envelope["event_type"]
    payload = envelope["payload"]

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # ── Idempotency check ────────────────────────────────
            existing = await conn.fetchval(
                "SELECT 1 FROM processed_events WHERE event_id = $1",
                event_id,
            )
            if existing:
                logger.info("duplicate_event_skipped", event_id=str(event_id))
                return

            # ── Business logic ───────────────────────────────────
            order_id = uuid.UUID(payload["order_id"])

            if event_type == "payment.succeeded":
                await transition_order(conn, order_id, "CONFIRMED")

            elif event_type == "payment.failed":
                await transition_order(conn, order_id, "PAYMENT_FAILED")
                # Compensation: trigger stock release via outbox
                release_event = wrap_event(
                    "stock.release", {"order_id": str(order_id)}
                )
                await conn.execute(
                    """
                    INSERT INTO outbox_events (id, topic, key, payload, created_at)
                    VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                    """,
                    "stock.release",
                    str(order_id),
                    release_event,
                )

            elif event_type == "stock.rejected":
                await transition_order(conn, order_id, "STOCK_UNAVAILABLE")

            else:
                logger.warning("unknown_event_type", event_type=event_type)
                return

            # ── Mark as processed ────────────────────────────────
            await conn.execute(
                "INSERT INTO processed_events (event_id) VALUES ($1)",
                event_id,
            )

    logger.info(
        "event_processed",
        event_id=str(event_id),
        event_type=event_type,
        order_id=payload["order_id"],
    )
