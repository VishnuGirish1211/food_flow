"""Payment service Kafka consumer.

Subscribes to: stock.reserved

Implements the two-transaction payment flow:
  1. Transaction 1: Insert payment as PROCESSING
  2. Call gateway simulator
  3. Transaction 2: Update payment to SUCCEEDED/FAILED and write outbox event

This guarantees we never have uncertain state regarding whether we charged the user.
"""

import asyncio
import uuid

import asyncpg
import structlog
from asyncpg import Pool

from shared.events import wrap_event
from shared.kafka import create_consumer
from shared.metrics import kafka_messages_consumed_total
from gateway import process_payment

logger = structlog.get_logger()


async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):
    """Main consumer loop."""
    consumer = await create_consumer(
        topics=["stock.reserved"],
        group_id="payment-service",
    )
    logger.info("consumer_started", topics=["stock.reserved"])

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
    payload = envelope["payload"]
    order_id = payload["order_id"]
    amount = payload["total_amount"]
    simulate_directive = payload.get("payment_simulate")

    payment_id = None

    # ── Transaction 1: Record Intent & Idempotency ──
    async with db_pool.acquire() as conn:
        try:
            async with conn.transaction():
                # Idempotency check
                existing = await conn.fetchval(
                    "SELECT 1 FROM processed_events WHERE event_id = $1",
                    event_id,
                )
                if existing:
                    logger.info("duplicate_event_skipped", event_id=str(event_id))
                    return

                # Record intent to process
                payment_id = await conn.fetchval(
                    """
                    INSERT INTO payments (order_id, amount, status)
                    VALUES ($1, $2, 'PROCESSING')
                    RETURNING id
                    """,
                    uuid.UUID(order_id),
                    amount,
                )

                # Mark as processed immediately so we don't retry if the gateway crashes
                await conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES ($1)",
                    event_id,
                )

        except asyncpg.UniqueViolationError as e:
            if "payments_order_id_key" in str(e):
                 logger.info("duplicate_payment_skipped", order_id=order_id)
                 return
            raise

    # If we get here, payment_id is set and we must call the gateway.
    # ── External Call (Gateway) ──
    logger.info("calling_payment_gateway", order_id=order_id, amount=amount)
    success = await process_payment(amount, simulate_directive)

    new_status = "SUCCEEDED" if success else "FAILED"
    outbox_topic = "payment.succeeded" if success else "payment.failed"
    outbox_event = wrap_event(outbox_topic, {"order_id": order_id})

    # ── Transaction 2: Record Outcome & Publish ──
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE payments SET status = $1, updated_at = NOW() WHERE id = $2",
                new_status,
                payment_id,
            )

            await conn.execute(
                """
                INSERT INTO outbox_events (id, topic, key, payload, created_at)
                VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                """,
                outbox_topic,
                order_id,
                outbox_event,
            )

    logger.info("payment_completed", order_id=order_id, status=new_status)
