"""Unit tests for stock reservation logic."""

import pytest
from services.restaurant.stock import InsufficientStockError

class MockConnection:
    def __init__(self, available_stock=100):
        self.available_stock = available_stock
        self.updates = []
        self.reservations = []

    async def fetchval(self, query, *args):
        if "invalid_item" in args:
            return None
        return self.available_stock

    async def execute(self, query, *args):
        if "UPDATE" in query:
            self.updates.append(args)
        elif "INSERT" in query:
            self.reservations.append(args)

@pytest.mark.asyncio
async def test_successful_reservation():
    from services.restaurant.stock import reserve_stock
    
    conn = MockConnection(available_stock=10)
    items = [{"item_id": "item-1", "quantity": 2}]
    
    await reserve_stock(conn, "order-1", items)
    
    assert len(conn.updates) == 1
    assert conn.updates[0][0] == 2 # quantity deducted
    assert len(conn.reservations) == 1

@pytest.mark.asyncio
async def test_insufficient_stock():
    from services.restaurant.stock import reserve_stock
    
    conn = MockConnection(available_stock=1)
    items = [{"item_id": "item-1", "quantity": 5}]
    
    with pytest.raises(InsufficientStockError) as exc_info:
        await reserve_stock(conn, "order-1", items)
        
    assert "Insufficient stock for item item-1. Requested 5, available 1" in str(exc_info.value)
