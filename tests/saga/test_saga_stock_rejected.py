"""Test: Stock Rejected — order with insufficient stock → STOCK_UNAVAILABLE.

Places an order requesting more stock than available.
Asserts the order is rejected, stock is untouched, and no payment is created.
"""

import uuid

from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    ITEM_BURGER_STOCK,
    RESTAURANT_ID,
    poll_for_status,
    wrap_event,
)


async def test_saga_stock_rejected(db_pools):
    """Place an order for qty exceeding stock, verify rejection.

    Flow:
      1. Insert order with quantity=9999 (stock=100)
      2. Restaurant consumer rejects with InsufficientStockError
      3. Publishes stock.rejected
      4. Order consumer transitions to STOCK_UNAVAILABLE

    Assertions:
      - Order reaches STOCK_UNAVAILABLE
      - menu_item_stock unchanged (still 100)
      - No payments row created for this order
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()
    quantity = 9999  # Way more than the 100 available

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

    # Insert order + outbox event
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

    # Wait for rejection
    final_status = await poll_for_status(
        db_pools["order"], "orders", "id", order_id
    )
    assert final_status == "STOCK_UNAVAILABLE", (
        f"Expected STOCK_UNAVAILABLE, got {final_status}"
    )

    # Assert stock was NOT decremented
    async with db_pools["restaurant"].acquire() as conn:
        stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert stock == ITEM_BURGER_STOCK, (
        f"Stock should remain at {ITEM_BURGER_STOCK}, got {stock}"
    )

    # Assert no payment was created
    async with db_pools["payment"].acquire() as conn:
        payment = await conn.fetchrow(
            "SELECT * FROM payments WHERE order_id = $1",
            order_id,
        )
    assert payment is None, "No payment should exist for a stock-rejected order"
