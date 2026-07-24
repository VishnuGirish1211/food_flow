"""Order state machine.

Implements explicit state transitions with a whitelist of allowed
transitions. Any transition not in the table is rejected.

States:
  PENDING             — Order created, waiting for saga completion
  CONFIRMED           — Payment succeeded (terminal)
  PAYMENT_FAILED      — Payment failed (terminal)
  STOCK_UNAVAILABLE   — Stock reservation rejected (terminal)
"""

import structlog

logger = structlog.get_logger()

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"CONFIRMED", "PAYMENT_FAILED", "STOCK_UNAVAILABLE"},
    # Terminal states have no allowed transitions
}


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(self, current: str, target: str):
        self.current = current
        self.target = target
        super().__init__(f"Cannot transition from {current} to {target}")


async def transition_order(conn, order_id: str, new_status: str):
    """Transition an order to a new status.

    Must be called within an active database transaction.
    Uses SELECT ... FOR UPDATE to lock the order row.

    Raises:
        ValueError: If order_id does not exist.
        InvalidTransitionError: If the transition is not allowed.
    """
    current_status = await conn.fetchval(
        "SELECT status FROM orders WHERE id = $1 FOR UPDATE",
        order_id,
    )

    if current_status is None:
        raise ValueError(f"Order {order_id} not found")

    allowed = ALLOWED_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise InvalidTransitionError(current_status, new_status)

    await conn.execute(
        "UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2",
        new_status,
        order_id,
    )

    logger.info(
        "order_transitioned",
        order_id=order_id,
        from_status=current_status,
        to_status=new_status,
    )
