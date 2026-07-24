"""Unit tests for the order state machine."""

import pytest
from services.order.state_machine import ALLOWED_TRANSITIONS, InvalidTransitionError

# A dummy connection mock just for unit testing transition logic
class MockConnection:
    def __init__(self, initial_status="PENDING"):
        self.status = initial_status
        self.updates = []

    async def fetchval(self, query, *args):
        if args[0] == "invalid_id":
            return None
        return self.status

    async def execute(self, query, *args):
        self.updates.append(args)

@pytest.mark.asyncio
async def test_valid_transition():
    # Because transition_order is an async DB function, we mock it.
    from services.order.state_machine import transition_order
    
    conn = MockConnection("PENDING")
    await transition_order(conn, "order-123", "CONFIRMED")
    
    assert len(conn.updates) == 1
    assert conn.updates[0][0] == "CONFIRMED"

@pytest.mark.asyncio
async def test_invalid_transition():
    from services.order.state_machine import transition_order
    
    conn = MockConnection("CONFIRMED")
    
    with pytest.raises(InvalidTransitionError) as exc_info:
        await transition_order(conn, "order-123", "PENDING")
        
    assert "Cannot transition from CONFIRMED to PENDING" in str(exc_info.value)
