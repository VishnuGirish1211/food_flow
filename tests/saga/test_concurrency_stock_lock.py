"""Test: Concurrent Stock Locking — no overselling under concurrent load.

Seeds a menu item with quantity=1, fires 5 concurrent order requests.
Asserts exactly 1 order is confirmed and 4 are rejected.
Proves the SELECT ... FOR UPDATE lock prevents overselling.
"""

import asyncio
import uuid

from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    RESTAURANT_ID,
    poll_for_status,
    wrap_event,
)


async def test_concurrency_stock_lock(db_pools):
    """Fire 5 concurrent orders for 1 item in stock.

    Flow:
      1. Set burger stock to exactly 1
      2. Create 5 orders concurrently, each requesting qty=1
      3. Wait for all orders to reach a terminal state
      4. Verify exactly 1 CONFIRMED, 4 STOCK_UNAVAILABLE
      5. Verify stock is exactly 0 (no overselling)
    """
    NUM_ORDERS = 5

    # Set stock to exactly 1
    async with db_pools["restaurant"].acquire() as conn:
        await conn.execute(
            "UPDATE menu_item_stock SET quantity = 1 WHERE item_id = $1",
            ITEM_BURGER_ID,
        )

    # Create 5 concurrent orders
    async def create_order():
        order_id = uuid.uuid4()
        user_id = uuid.uuid4()
        items_data = [
            {"item_id": ITEM_BURGER_ID, "quantity": 1, "price": ITEM_BURGER_PRICE}
        ]
        total_amount = ITEM_BURGER_PRICE

        event_payload = {
            "order_id": str(order_id),
            "user_id": str(user_id),
            "restaurant_id": RESTAURANT_ID,
            "items": items_data,
            "total_amount": total_amount,
        }
        event = wrap_event("order.placed", event_payload)

        async with db_pools["order"].acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO orders (id, user_id, restaurant_id, items, total_amount, status)
                    VALUES ($1, $2, $3, $4, $5, 'PENDING')
                    """,
                    order_id, user_id, uuid.UUID(RESTAURANT_ID),
                    items_data, total_amount,
                )
                await conn.execute(
                    """
                    INSERT INTO outbox_events (id, topic, key, payload, created_at)
                    VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                    """,
                    "order.placed", str(order_id), event,
                )

        return order_id

    # Fire all 5 concurrently
    order_ids = await asyncio.gather(*[create_order() for _ in range(NUM_ORDERS)])

    # Wait for all orders to reach a terminal state
    statuses = []
    for oid in order_ids:
        try:
            status = await poll_for_status(
                db_pools["order"], "orders", "id", oid, timeout=20.0,
            )
            statuses.append(status)
        except TimeoutError:
            async with db_pools["order"].acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT status FROM orders WHERE id = $1", oid
                )
            statuses.append(row["status"] if row else "NOT_FOUND")

    confirmed = statuses.count("CONFIRMED")
    stock_unavailable = statuses.count("STOCK_UNAVAILABLE")

    assert confirmed == 1, (
        f"Expected exactly 1 CONFIRMED, got {confirmed}. Statuses: {statuses}"
    )
    assert stock_unavailable == NUM_ORDERS - 1, (
        f"Expected {NUM_ORDERS - 1} STOCK_UNAVAILABLE, got {stock_unavailable}. "
        f"Statuses: {statuses}"
    )

    # Verify final stock is exactly 0
    async with db_pools["restaurant"].acquire() as conn:
        final_stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert final_stock == 0, (
        f"Stock should be exactly 0 (no overselling), got {final_stock}"
    )
