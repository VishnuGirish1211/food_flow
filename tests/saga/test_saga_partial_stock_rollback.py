"""Test: Partial stock deduction must not survive a multi-item rejection.

Regression test for the partial-deduction bug. `reserve_stock` deducts items
one at a time; if a later item is short it raises InsufficientStockError. That
error used to be caught *inside* the outer transaction, so the deductions
already made for earlier items committed alongside the stock.rejected event —
silently destroying inventory on every rejected multi-item order, with no
stock_reservations row for the compensation path to ever release.

The reservation now runs inside a SAVEPOINT, so the earlier deductions roll
back while the outer transaction survives to record the rejection.

This test needs a real database: a mock connection has no transaction
semantics, which is exactly why the original bug went unnoticed.
"""

import uuid

from tests.saga.conftest import (
    ITEM_BURGER_ID,
    ITEM_BURGER_PRICE,
    ITEM_FRIES_ID,
    RESTAURANT_ID,
    poll_for_status,
    wrap_event,
)


async def test_partial_deduction_rolled_back_on_rejection(db_pools):
    """First item is available, second is not — neither may be deducted.

    Flow:
      1. Order burger (qty 2, in stock) + fries (qty 9999, not in stock)
      2. reserve_stock deducts the burger, then raises on the fries
      3. The savepoint rolls the burger deduction back
      4. stock.rejected still commits; order → STOCK_UNAVAILABLE

    Assertions:
      - Order reaches STOCK_UNAVAILABLE
      - Burger stock is unchanged (the bug decremented it by 2)
      - Fries stock is unchanged
      - No stock_reservations row exists for the order
    """
    order_id = uuid.uuid4()
    user_id = uuid.uuid4()

    # Burger first so it is deducted before the fries failure is hit.
    items_data = [
        {"item_id": ITEM_BURGER_ID, "quantity": 2, "price": ITEM_BURGER_PRICE},
        {"item_id": ITEM_FRIES_ID, "quantity": 9999, "price": 4.99},
    ]
    total_amount = 2 * ITEM_BURGER_PRICE + 9999 * 4.99

    # Record starting stock so the assertions do not depend on seed values.
    async with db_pools["restaurant"].acquire() as conn:
        burger_before = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1", ITEM_BURGER_ID
        )
        fries_before = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1", ITEM_FRIES_ID
        )

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

    final_status = await poll_for_status(db_pools["order"], "orders", "id", order_id)
    assert final_status == "STOCK_UNAVAILABLE", (
        f"Expected STOCK_UNAVAILABLE, got {final_status}"
    )

    async with db_pools["restaurant"].acquire() as conn:
        burger_after = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1", ITEM_BURGER_ID
        )
        fries_after = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1", ITEM_FRIES_ID
        )
        reservation = await conn.fetchrow(
            "SELECT 1 FROM stock_reservations WHERE order_id = $1", order_id
        )

    assert burger_after == burger_before, (
        f"Burger stock leaked: {burger_before} -> {burger_after}. The deduction "
        f"for the first item was not rolled back when the second item failed."
    )
    assert fries_after == fries_before, (
        f"Fries stock changed unexpectedly: {fries_before} -> {fries_after}"
    )
    assert reservation is None, (
        "No stock_reservations row should exist for a rejected order"
    )
