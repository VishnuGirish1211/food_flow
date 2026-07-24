"""Test: Outbox Crash Recovery — unpublished outbox event is replayed.

Simulates the outbox relay crashing after Kafka send but before marking
the event as published. Manually inserts an outbox row with published_at=NULL,
lets the relay pick it up, and verifies downstream idempotency prevents
any duplicate side effects.
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


async def test_outbox_crash_recovery(db_pools):
    """Simulate outbox relay crash, verify republish + idempotency.

    Scenario:
      1. Insert an order (PENDING) into order_db
      2. Insert the outbox event with published_at=NULL (simulating crash)
      3. The relay picks it up and publishes to Kafka
      4. Restaurant consumer processes it (stock reserved)
      5. Insert another copy of the same event with published_at=NULL
      6. Relay publishes it again, consumer skips via idempotency

    Assertions:
      - Stock is decremented exactly once (not doubled)
      - processed_events has exactly one row for this event
      - Only one stock_reservations row exists
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()
    quantity = 4

    items_data = [
        {"item_id": ITEM_BURGER_ID, "quantity": quantity, "price": ITEM_BURGER_PRICE}
    ]
    total_amount = quantity * ITEM_BURGER_PRICE

    event_payload = {
        "order_id": str(order_id),
        "user_id": str(user_id),
        "restaurant_id": RESTAURANT_ID,
        "items": items_data,
        "total_amount": total_amount,
    }
    event = wrap_event("order.placed", event_payload)
    event_id = event["event_id"]

    # Step 1: Create the order row
    async with db_pools["order"].acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders (id, user_id, restaurant_id, items, total_amount, status)
            VALUES ($1, $2, $3, $4, $5, 'PENDING')
            """,
            order_id, user_id, uuid.UUID(RESTAURANT_ID),
            items_data, total_amount,
        )

    # Step 2: Insert outbox event with published_at=NULL (simulating crash)
    async with db_pools["order"].acquire() as conn:
        await conn.execute(
            """
            INSERT INTO outbox_events (id, topic, key, payload, created_at, published_at)
            VALUES (gen_random_uuid(), $1, $2, $3, NOW(), NULL)
            """,
            "order.placed", str(order_id), event,
        )

    # Wait for restaurant consumer to create the reservation
    reservation = await poll_for_row(
        db_pools["restaurant"],
        "SELECT * FROM stock_reservations WHERE order_id = $1",
        [str(order_id)],
        timeout=15.0,
    )
    assert reservation is not None, "stock_reservations row should be created"

    # Step 5: Insert another outbox event (same event payload, simulating
    # the relay "re-discovering" an unpublished event after crash)
    async with db_pools["order"].acquire() as conn:
        await conn.execute(
            """
            INSERT INTO outbox_events (id, topic, key, payload, created_at, published_at)
            VALUES (gen_random_uuid(), $1, $2, $3, NOW(), NULL)
            """,
            "order.placed", str(order_id), event,
        )

    # Wait for the relay to process the second outbox event
    await asyncio.sleep(4)

    # Assert stock was decremented only ONCE
    async with db_pools["restaurant"].acquire() as conn:
        stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert stock == ITEM_BURGER_STOCK - quantity, (
        f"Stock should be {ITEM_BURGER_STOCK - quantity} (decremented once), "
        f"got {stock}. Idempotency may have failed."
    )

    # Assert exactly one processed_events row for this event
    async with db_pools["restaurant"].acquire() as conn:
        pe_count = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE event_id = $1",
            uuid.UUID(event_id),
        )
    assert pe_count == 1, (
        f"processed_events should have exactly 1 row, got {pe_count}"
    )

    # Assert exactly one stock_reservations row
    async with db_pools["restaurant"].acquire() as conn:
        sr_count = await conn.fetchval(
            "SELECT COUNT(*) FROM stock_reservations WHERE order_id = $1",
            str(order_id),
        )
    assert sr_count == 1, (
        f"stock_reservations should have exactly 1 row, got {sr_count}"
    )
