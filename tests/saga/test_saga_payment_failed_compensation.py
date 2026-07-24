"""Test: Payment Failed Compensation — forced failure triggers stock release.

Forces a payment failure via the payment_simulate field.
Asserts the full compensation flow: stock temporarily reserved, then released.
"""

import asyncio
import uuid

from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    ITEM_BURGER_STOCK,
    RESTAURANT_ID,
    poll_for_status,
    wrap_event,
)


async def test_saga_payment_failed_compensation(db_pools):
    """Force payment failure, verify compensation restores stock.

    Flow:
      1. Insert order with payment_simulate="failure"
      2. Restaurant consumer reserves stock, publishes stock.reserved
      3. Payment consumer processes payment → FAILED, publishes payment.failed
      4. Order consumer transitions to PAYMENT_FAILED, publishes stock.release
      5. Restaurant consumer releases stock, deletes reservation

    Assertions:
      - Order reaches PAYMENT_FAILED
      - Stock is restored to original value (100)
      - stock_reservations row is deleted for this order
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()
    quantity = 5

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
        "payment_simulate": "failure",  # Force payment to fail
    }
    event = wrap_event("order.placed", event_payload)

    # Record initial stock
    async with db_pools["restaurant"].acquire() as conn:
        initial_stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
            ITEM_BURGER_ID,
        )
    assert initial_stock == ITEM_BURGER_STOCK

    # Insert order + outbox event
    async with db_pools["order"].acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO orders (id, user_id, restaurant_id, items, total_amount, status, payment_simulate)
                VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)
                """,
                order_id, user_id, uuid.UUID(RESTAURANT_ID),
                items_data, total_amount, "failure",
            )
            await conn.execute(
                """
                INSERT INTO outbox_events (id, topic, key, payload, created_at)
                VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                """,
                "order.placed", str(order_id), event,
            )

    # Wait for order to reach PAYMENT_FAILED
    final_status = await poll_for_status(
        db_pools["order"], "orders", "id", order_id
    )
    assert final_status == "PAYMENT_FAILED", (
        f"Expected PAYMENT_FAILED, got {final_status}"
    )

    # Wait for the compensation flow to complete (stock.release)
    timeout = 15.0
    elapsed = 0.0
    interval = 0.3
    stock_restored = False

    while elapsed < timeout:
        async with db_pools["restaurant"].acquire() as conn:
            current_stock = await conn.fetchval(
                "SELECT quantity FROM menu_item_stock WHERE item_id = $1",
                ITEM_BURGER_ID,
            )
        if current_stock == initial_stock:
            stock_restored = True
            break
        await asyncio.sleep(interval)
        elapsed += interval

    assert stock_restored, (
        f"Stock should be restored to {initial_stock}, but is {current_stock}"
    )

    # Assert stock_reservations row was deleted
    async with db_pools["restaurant"].acquire() as conn:
        reservation = await conn.fetchrow(
            "SELECT * FROM stock_reservations WHERE order_id = $1",
            str(order_id),
        )
    assert reservation is None, (
        "stock_reservations row should be deleted after compensation"
    )
