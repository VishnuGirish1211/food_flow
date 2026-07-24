"""Test: Saga Happy Path — order.placed → stock.reserved → payment.succeeded → CONFIRMED.

Places an order with valid stock and default payment success.
Asserts the full saga completes: stock decremented, payment recorded,
reservation exists, order confirmed.
"""

import uuid

import pytest

# Import helpers from conftest (auto-loaded by pytest)
from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    ITEM_BURGER_STOCK,
    RESTAURANT_ID,
    poll_for_status,
    poll_for_row,
    wrap_event,
)


async def test_happy_path_saga(db_pools):
    """Place an order, verify full saga completion.

    Flow:
      1. Insert order (PENDING) + outbox event (order.placed) into order_db
      2. Outbox relay publishes to Kafka
      3. Restaurant consumer reserves stock, publishes stock.reserved
      4. Payment consumer processes payment, publishes payment.succeeded
      5. Order consumer transitions order to CONFIRMED

    Assertions:
      - Order reaches CONFIRMED status
      - menu_item_stock decremented by order quantity
      - payments row exists with status SUCCEEDED
      - stock_reservations row exists for this order
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()
    quantity = 3

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

    # Insert order + outbox event in order_db
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

    # Wait for saga to complete
    final_status = await poll_for_status(
        db_pools["order"], "orders", "id", order_id
    )
    assert final_status == "CONFIRMED", f"Expected CONFIRMED, got {final_status}"

    # Assert stock was decremented
    async with db_pools["restaurant"].acquire() as conn:
        stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert stock == ITEM_BURGER_STOCK - quantity, (
        f"Expected stock {ITEM_BURGER_STOCK - quantity}, got {stock}"
    )

    # Assert payment row exists with SUCCEEDED
    payment_row = await poll_for_row(
        db_pools["payment"],
        "SELECT status FROM payments WHERE order_id = $1",
        [order_id],
    )
    assert payment_row["status"] == "SUCCEEDED"

    # Assert stock_reservations row exists
    async with db_pools["restaurant"].acquire() as conn:
        reservation = await conn.fetchrow(
            "SELECT * FROM stock_reservations WHERE order_id = $1",
            str(order_id),
        )
    assert reservation is not None, "stock_reservations row should exist"
