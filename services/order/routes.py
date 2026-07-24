"""Order service API routes.

POST /orders      — Create a new order (returns 202 Accepted)
GET  /orders/{id} — Get order details and current status

POST /orders follows the transactional outbox pattern:
  1. Validate request
  2. BEGIN transaction
  3. INSERT order (status=PENDING)
  4. INSERT outbox event (topic=order.placed)
  5. COMMIT
  6. Return 202

The order is NOT confirmed at this point — the saga will
asynchronously process it through stock reservation and payment.
"""

import os
import uuid
from typing import Optional

import jwt
import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from models import CreateOrderRequest, OrderResponse
from shared.events import wrap_event

logger = structlog.get_logger()
router = APIRouter()

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"


async def get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    """Extract and validate the JWT from the Authorization header.

    Returns the decoded token payload with user_id and email.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")

    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"user_id": payload["sub"], "email": payload["email"]}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/orders", status_code=202)
async def create_order(
    request: Request,
    body: CreateOrderRequest,
    user: dict = Depends(get_current_user),
    x_payment_simulate: Optional[str] = Header(None),
):
    """Create a new order.

    Returns 202 Accepted because order creation is asynchronous.
    The order starts in PENDING status and will transition through
    the saga (stock reservation → payment → confirmation).

    The X-Payment-Simulate header is propagated through the saga
    to the payment service for deterministic testing.
    """
    pool = request.app.state.db_pool
    user_id = uuid.UUID(user["user_id"])
    order_id = uuid.uuid4()

    # Calculate total from submitted items
    items_data = [
        {
            "item_id": str(item.item_id),
            "quantity": item.quantity,
            "price": item.price,
        }
        for item in body.items
    ]
    total_amount = sum(item.quantity * item.price for item in body.items)

    # Build the event payload
    event_payload = {
        "order_id": str(order_id),
        "user_id": str(user_id),
        "restaurant_id": str(body.restaurant_id),
        "items": items_data,
        "total_amount": total_amount,
    }
    if x_payment_simulate:
        event_payload["payment_simulate"] = x_payment_simulate

    event = wrap_event("order.placed", event_payload)

    # ── Transactional outbox: order + event in one transaction ────
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO orders (id, user_id, restaurant_id, items, total_amount, status, payment_simulate)
                VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)
                """,
                order_id,
                user_id,
                body.restaurant_id,
                items_data,
                total_amount,
                x_payment_simulate,
            )
            await conn.execute(
                """
                INSERT INTO outbox_events (id, topic, key, payload, created_at)
                VALUES (gen_random_uuid(), $1, $2, $3, NOW())
                """,
                "order.placed",
                str(order_id),
                event,
            )

    logger.info(
        "order_created",
        order_id=str(order_id),
        user_id=str(user_id),
        total_amount=total_amount,
    )

    return {
        "id": str(order_id),
        "status": "PENDING",
        "message": "Order accepted for processing",
    }


@router.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(
    request: Request,
    order_id: uuid.UUID,
    user: dict = Depends(get_current_user),
):
    """Get order details. Only the order owner can view their order."""
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM orders WHERE id = $1",
            order_id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    # Verify ownership
    if str(row["user_id"]) != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied")

    return OrderResponse(
        id=str(row["id"]),
        user_id=str(row["user_id"]),
        restaurant_id=str(row["restaurant_id"]),
        items=row["items"],
        total_amount=float(row["total_amount"]),
        status=row["status"],
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
    )
