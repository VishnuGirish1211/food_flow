"""Restaurant service Kafka consumer.

Subscribes to:
- order.placed: Attempts to reserve stock. Publishes stock.reserved or stock.rejected.
- stock.release: Reverts a previous reservation (compensation flow).
"""

import asyncio
import uuid

import structlog
from asyncpg import Pool
import asyncpg

from shared.events import wrap_event
from shared.kafka import create_consumer
from shared.metrics import kafka_messages_consumed_total
from stock import InsufficientStockError, reserve_stock, release_stock

logger = structlog.get_logger()


async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):
    """Main consumer loop."""
    consumer = await create_consumer(
        topics=["order.placed", "stock.release"],
        group_id="restaurant-service",
    )
    logger.info("consumer_started", topics=["order.placed", "stock.release"])

    try:
        while not shutdown_event.is_set():
            result = await consumer.getmany(timeout_ms=1000)

            for tp, messages in result.items():
                for msg in messages:
                    try:
                        await _handle_message(db_pool, msg.value)
                        kafka_messages_consumed_total.labels(
                            topic=msg.topic, status="success"
                        ).inc()
                    except Exception:
                        logger.exception("consumer_error", topic=msg.topic)
                        kafka_messages_consumed_total.labels(
                            topic=msg.topic, status="error"
                        ).inc()

            if result:
                await consumer.commit()
    finally:
        await consumer.stop()
        logger.info("consumer_stopped")


async def _handle_message(db_pool: Pool, envelope: dict):
    event_id = uuid.UUID(envelope["event_id"])
    event_type = envelope["event_type"]
    payload = envelope["payload"]

    async with db_pool.acquire() as conn:
        try:
            async with conn.transaction():
                # ── Idempotency check ──
                existing = await conn.fetchval(
                    "SELECT 1 FROM processed_events WHERE event_id = $1",
                    event_id,
                )
                if existing:
                    logger.info("duplicate_event_skipped", event_id=str(event_id))
                    return

                order_id = payload["order_id"]

                if event_type == "order.placed":
                    try:
                        # Attempt to reserve stock inside a SAVEPOINT (a nested
                        # asyncpg transaction). If any item is short, the error
                        # escapes this block, the savepoint rolls back, and the
                        # deductions already made for earlier items are undone —
                        # while the outer transaction stays alive to record the
                        # rejection below.
                        #
                        # The `except` MUST stay outside this block. Catching
                        # InsufficientStockError inside it would swallow the error
                        # before the context manager sees it, so nothing would roll
                        # back and partial deductions would commit.
                        async with conn.transaction():
                            await reserve_stock(conn, order_id, payload["items"])

                        # Reservation succeeded, publish stock.reserved
                        outbox_event = wrap_event("stock.reserved", payload)
                        await conn.execute(
                            """
                            INSERT INTO outbox_events (id, topic, key, payload, created_at)
                            VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                            """,
                            "stock.reserved",
                            order_id,
                            outbox_event,
                        )

                    except InsufficientStockError as e:
                        logger.info("stock_reservation_failed", order_id=order_id, reason=str(e))
                        # Reservation failed, publish stock.rejected
                        outbox_event = wrap_event("stock.rejected", {"order_id": order_id, "reason": str(e)})
                        await conn.execute(
                            """
                            INSERT INTO outbox_events (id, topic, key, payload, created_at)
                            VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                            """,
                            "stock.rejected",
                            order_id,
                            outbox_event,
                        )

                elif event_type == "stock.release":
                    # Compensation flow
                    await release_stock(conn, order_id)

                else:
                    logger.warning("unknown_event_type", event_type=event_type)
                    return

                # ── Mark as processed ──
                await conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES ($1)",
                    event_id,
                )

        except asyncpg.UniqueViolationError as e:
            # If the stock_reservations UNIQUE constraint triggers, this order was already processed
            # but maybe the processed_events write failed or something else happened.
            # We can safely swallow this exception because the consumer is idempotent.
            if "stock_reservations_order_id_key" in str(e):
                 logger.info("duplicate_reservation_skipped", order_id=payload.get("order_id"))
            else:
                raise
