"""Pydantic models for the Restaurant service."""

from pydantic import BaseModel


class MenuItemResponse(BaseModel):
    id: str
    name: str
    description: str | None
    price: float
    stock_quantity: int


class RestaurantResponse(BaseModel):
    id: str
    name: str
    description: str | None
