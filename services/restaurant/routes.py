"""Restaurant service REST API."""

import uuid
import structlog
from fastapi import APIRouter, HTTPException, Request
from pydantic import UUID4

from models import MenuItemResponse, RestaurantResponse

logger = structlog.get_logger()
router = APIRouter()


@router.get("/restaurants", response_model=list[RestaurantResponse])
async def list_restaurants(request: Request):
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, description FROM restaurants ORDER BY name"
        )

    return [
        RestaurantResponse(
            id=str(r["id"]),
            name=r["name"],
            description=r["description"],
        )
        for r in rows
    ]


@router.get("/restaurants/{restaurant_id}/menu", response_model=list[MenuItemResponse])
async def get_menu(request: Request, restaurant_id: uuid.UUID):
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        # Check if restaurant exists
        exists = await conn.fetchval(
            "SELECT 1 FROM restaurants WHERE id = $1", restaurant_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Restaurant not found")

        # Fetch menu items with stock
        rows = await conn.fetch(
            """
            SELECT m.id, m.name, m.description, m.price, s.quantity
            FROM menu_items m
            LEFT JOIN menu_item_stock s ON s.item_id = m.id
            WHERE m.restaurant_id = $1
            ORDER BY m.name
            """,
            restaurant_id,
        )

    return [
        MenuItemResponse(
            id=str(r["id"]),
            name=r["name"],
            description=r["description"],
            price=float(r["price"]),
            stock_quantity=r["quantity"] or 0,
        )
        for r in rows
    ]
