"""Pydantic models for the Payment service."""

from pydantic import BaseModel


class PaymentResponse(BaseModel):
    id: str
    order_id: str
    amount: float
    status: str
    created_at: str
    updated_at: str
