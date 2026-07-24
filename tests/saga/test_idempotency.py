"""Test: Consumer Idempotency — duplicate events are safely ignored.

Publishes the same order.placed event (same event_id) twice directly to Kafka.
Asserts stock is only decremented once and only one processed_events row exists.
"""

import asyncio
import uuid

from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    ITEM_BURGER_STOCK,
    RESTAURANT_ID,
    poll_for_row,
    wrap_event,
)


async def test_idempotency_duplicate_event(db_pools, raw_kafka_producer):
    """Publish the same event twice, verify idempotent processing.

    Instead of going through the outbox, this test publishes directly
    to Kafka using a raw producer — giving us control over the event_id.

    Flow:
      1. Create an order row in order_db (PENDING)
      2. Build an order.placed event with a fixed event_id
      3. Publish it to Kafka twice
      4. Wait for the restaurant consumer to process it
      5. Verify only one stock deduction occurred

    Assertions:
      - menu_item_stock decremented only once
      - processed_events has exactly one row for this event_id
      - stock_reservations has exactly one row for this order
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()
    quantity = 2
    fixed_event_id = str(uuid.uuid4())

    items_data = [
        {"item_id": ITEM_BURGER_ID, "quantity": quantity, "price": ITEM_BURGER_PRICE}
    ]
    total_amount = quantity * ITEM_BURGER_PRICE

    # Create the order row first
    async with db_pools["order"].acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders (id, user_id, restaurant_id, items, total_amount, status)
            VALUES ($1, $2, $3, $4, $5, 'PENDING')
            """,
            order_id, user_id, uuid.UUID(RESTAURANT_ID),
            items_data, total_amount,
        )

    # Build the event with a fixed event_id
    event = {
        "event_id": fixed_event_id,
        "event_type": "order.placed",
        "occurred_at": "2026-01-01T00:00:00+00:00",
        "payload": {
            "order_id": str(order_id),
            "user_id": str(user_id),
            "restaurant_id": RESTAURANT_ID,
            "items": items_data,
            "total_amount": total_amount,
        },
    }

    # Publish to Kafka TWICE with the same event_id
    await raw_kafka_producer.send_and_wait(
        topic="order.placed",
        key=str(order_id).encode("utf-8"),
        value=event,
    )
    await raw_kafka_producer.send_and_wait(
        topic="order.placed",
        key=str(order_id).encode("utf-8"),
        value=event,
    )

    # Wait for the restaurant consumer to process at least one event
    reservation = await poll_for_row(
        db_pools["restaurant"],
        "SELECT * FROM stock_reservations WHERE order_id = $1",
        [str(order_id)],
    )
    assert reservation is not None

    # Give the second duplicate event time to be consumed (or skipped)
    await asyncio.sleep(3)

    # Assert stock was decremented only ONCE
    async with db_pools["restaurant"].acquire() as conn:
        stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert stock == ITEM_BURGER_STOCK - quantity, (
        f"Stock should be {ITEM_BURGER_STOCK - quantity} (decremented once), got {stock}"
    )

    # Assert exactly one processed_events row for this event_id
    async with db_pools["restaurant"].acquire() as conn:
        pe_count = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE event_id = $1",
            uuid.UUID(fixed_event_id),
        )
    assert pe_count == 1, (
        f"processed_events should have exactly 1 row for event_id, got {pe_count}"
    )

    # Assert exactly one stock_reservations row for this order
    async with db_pools["restaurant"].acquire() as conn:
        sr_count = await conn.fetchval(
            "SELECT COUNT(*) FROM stock_reservations WHERE order_id = $1",
            str(order_id),
        )
    assert sr_count == 1, (
        f"stock_reservations should have exactly 1 row, got {sr_count}"
    )
