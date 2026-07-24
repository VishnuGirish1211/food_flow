"""Transactional outbox relay.

Polls the outbox_events table using SELECT ... FOR UPDATE SKIP LOCKED
and publishes pending events to Kafka. Each service runs this as a
background asyncio task.

Why FOR UPDATE SKIP LOCKED:
- FOR UPDATE locks rows so concurrent relays don't double-publish.
- SKIP LOCKED skips already-locked rows instead of blocking, which
  keeps the polling loop fast.

Why per-event transactions:
- If a Kafka send fails, only that event's transaction rolls back.
- Already-published events stay marked as published.
- If Kafka send succeeds but the UPDATE fails, the event will be
  re-published on next poll — but consumers are idempotent, so this is safe.
"""

import asyncio

import structlog
from aiokafka import AIOKafkaProducer
from asyncpg import Pool

from shared.metrics import outbox_events_published_total

logger = structlog.get_logger()


async def run_outbox_relay(
    db_pool: Pool,
    producer: AIOKafkaProducer,
    shutdown_event: asyncio.Event,
    poll_interval: float = 1.0,
):
    """Poll outbox table and publish events to Kafka.

    Runs until shutdown_event is set. On shutdown, does one final poll
    to flush any remaining events before stopping.
    """
    logger.info("outbox_relay_started")

    while not shutdown_event.is_set():
        try:
            published = await _poll_and_publish(db_pool, producer)
            if not published:
                # No events found — wait before next poll, but wake up
                # immediately if shutdown is signaled.
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=poll_interval
                    )
                except asyncio.TimeoutError:
                    pass
        except Exception:
            logger.exception("outbox_relay_error")
            await asyncio.sleep(poll_interval)

    # Final flush on shutdown
    try:
        await _poll_and_publish(db_pool, producer)
    except Exception:
        logger.exception("outbox_relay_final_flush_error")

    logger.info("outbox_relay_stopped")


async def _poll_and_publish(db_pool: Pool, producer: AIOKafkaProducer) -> int:
    """Fetch and publish pending outbox events. Returns count published."""
    published = 0

    while True:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, topic, key, payload
                    FROM outbox_events
                    WHERE published_at IS NULL
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )
                if not row:
                    break

                await producer.send_and_wait(
                    topic=row["topic"],
                    key=row["key"].encode("utf-8") if row["key"] else None,
                    value=row["payload"],
                )
                await conn.execute(
                    "UPDATE outbox_events SET published_at = NOW() WHERE id = $1",
                    row["id"],
                )

        published += 1
        outbox_events_published_total.inc()
        logger.info(
            "outbox_event_published",
            event_id=str(row["id"]),
            topic=row["topic"],
        )

    return published
