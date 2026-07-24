"""Stock reservation and release logic."""

import uuid

import structlog

logger = structlog.get_logger()


class InsufficientStockError(Exception):
    pass


async def reserve_stock(conn, order_id: str, items: list[dict]):
    """Reserve stock for an order.

    Deducts stock and creates a reservation record.
    Must be called within an active transaction.

    Raises:
        InsufficientStockError: If any item doesn't have enough stock.
    """
    for item in items:
        item_id = item["item_id"]
        qty = item["quantity"]

        # SELECT ... FOR UPDATE locks the row to prevent concurrent modifications
        stock = await conn.fetchval(
            "SELECT quantity FROM menu_item_stock WHERE item_id = $1 FOR UPDATE",
            item_id,
        )

        if stock is None:
            raise InsufficientStockError(f"Item {item_id} not found")
        if stock < qty:
            raise InsufficientStockError(
                f"Insufficient stock for item {item_id}. Requested {qty}, available {stock}"
            )

        # Deduct stock
        await conn.execute(
            "UPDATE menu_item_stock SET quantity = quantity - $1 WHERE item_id = $2",
            qty,
            item_id,
        )

    # Record the reservation for future release (compensation)
    await conn.execute(
        "INSERT INTO stock_reservations (order_id, items) VALUES ($1, $2)",
        order_id,
        items,
    )

    logger.info("stock_reserved", order_id=order_id)


async def release_stock(conn, order_id: str):
    """Release stock for a previously reserved order.

    Used as compensation when payment fails.
    Must be called within an active transaction.
    """
    row = await conn.fetchrow(
        "SELECT items FROM stock_reservations WHERE order_id = $1",
        order_id,
    )

    if not row:
        logger.warning("stock_release_not_found", order_id=order_id)
        return

    items = row["items"]

    for item in items:
        item_id = item["item_id"]
        qty = item["quantity"]

        await conn.execute(
            "UPDATE menu_item_stock SET quantity = quantity + $1 WHERE item_id = $2",
            qty,
            item_id,
        )

    # Delete the reservation record
    await conn.execute(
        "DELETE FROM stock_reservations WHERE order_id = $1",
        order_id,
    )

    logger.info("stock_released", order_id=order_id)
