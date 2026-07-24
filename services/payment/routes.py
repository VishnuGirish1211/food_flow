"""Payment service API routes."""

import uuid

import structlog
from fastapi import APIRouter, HTTPException, Request

from models import PaymentResponse

logger = structlog.get_logger()
router = APIRouter()


@router.get("/payments/order/{order_id}", response_model=PaymentResponse)
async def get_payment_status(request: Request, order_id: uuid.UUID):
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM payments WHERE order_id = $1",
            order_id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Payment not found")

    return PaymentResponse(
        id=str(row["id"]),
        order_id=str(row["order_id"]),
        amount=float(row["amount"]),
        status=row["status"],
        created_at=row["created_at"].isoformat(),
        updated_at=row["updated_at"].isoformat(),
    )
