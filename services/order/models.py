"""Pydantic models for the Order service."""

import uuid
from pydantic import BaseModel


class OrderItem(BaseModel):
    item_id: uuid.UUID
    quantity: int
    price: float  # price per unit as displayed to the customer


class CreateOrderRequest(BaseModel):
    restaurant_id: uuid.UUID
    items: list[OrderItem]


class OrderResponse(BaseModel):
    id: str
    user_id: str
    restaurant_id: str
    items: list[dict]
    total_amount: float
    status: str
    created_at: str
    updated_at: str
