"""Unit tests for payment reconciliation."""

import pytest
import uuid
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../services/payment')))

from reconciliation import reconcile_stuck_payments

class MockTransaction:
    async def __aenter__(self):
        pass
    async def __aexit__(self, exc_type, exc, tb):
        pass

class MockConnection:
    def __init__(self, payments=None):
        self.payments = payments or []
        self.updates = []
        self.inserts = []
        
    def transaction(self):
        return MockTransaction()

    async def fetch(self, query, *args):
        if "FROM payments" in query and "status = 'PROCESSING'" in query:
            return self.payments
        return []

    async def fetchval(self, query, *args):
        if "FOR UPDATE" in query:
            payment_id = args[0]
            for p in self.payments:
                if p["id"] == payment_id:
                    return p["status"]
            return None
        return None

    async def execute(self, query, *args):
        if "UPDATE payments" in query:
            self.updates.append(args)
        elif "INSERT INTO outbox_events" in query:
            self.inserts.append(args)

class MockAcquireContext:
    def __init__(self, conn):
        self.conn = conn
    
    async def __aenter__(self):
        return self.conn
        
    async def __aexit__(self, exc_type, exc, tb):
        pass

class MockPool:
    def __init__(self, payments=None):
        self.conn = MockConnection(payments)
    
    def acquire(self):
        return MockAcquireContext(self.conn)

@pytest.mark.asyncio
async def test_reconcile_no_stuck_payments():
    pool = MockPool(payments=[])
    count = await reconcile_stuck_payments(pool)
    assert count == 0
    assert len(pool.conn.updates) == 0

@pytest.mark.asyncio
async def test_reconcile_stuck_payment_success():
    payment_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    payments = [
        {
            "id": payment_id,
            "order_id": order_id,
            "amount": 100.0,
            "status": "PROCESSING",
            "payment_simulate": None,
        }
    ]
    pool = MockPool(payments=payments)

    count = await reconcile_stuck_payments(pool)

    assert count == 1
    assert len(pool.conn.updates) == 1
    # Check update query args: (new_status, payment_id)
    assert pool.conn.updates[0][0] == "SUCCEEDED"
    assert pool.conn.updates[0][1] == payment_id

    assert len(pool.conn.inserts) == 1
    # Check insert query args: (topic, order_id, payload)
    assert pool.conn.inserts[0][0] == "payment.succeeded"
    assert pool.conn.inserts[0][1] == order_id


@pytest.mark.asyncio
async def test_reconcile_stuck_payment_honours_failure_directive():
    """A payment that crashed mid-gateway must not resolve to SUCCEEDED.

    Regression test: payment_simulate was previously not persisted, so
    reconciliation passed None, fell back to PAYMENT_SIMULATE_DEFAULT
    ("success"), and inverted the outcome of a deliberately-failing order.
    """
    payment_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    payments = [
        {
            "id": payment_id,
            "order_id": order_id,
            "amount": 100.0,
            "status": "PROCESSING",
            "payment_simulate": "failure",
        }
    ]
    pool = MockPool(payments=payments)

    count = await reconcile_stuck_payments(pool)

    assert count == 1
    assert pool.conn.updates[0][0] == "FAILED"
    assert pool.conn.inserts[0][0] == "payment.failed"
    assert pool.conn.inserts[0][1] == order_id

@pytest.mark.asyncio
async def test_reconcile_already_resolved():
    # Simulate a payment that was PROCESSING when queried by fetch(), 
    # but when locked by FOR UPDATE, its status had changed (e.g. by another worker).
    payment_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    
    class RaceConditionConnection(MockConnection):
        async def fetchval(self, query, *args):
            if "FOR UPDATE" in query:
                # Oops, status is no longer PROCESSING
                return "SUCCEEDED"
            return await super().fetchval(query, *args)
            
    conn = RaceConditionConnection([
        {
            "id": payment_id,
            "order_id": order_id,
            "amount": 100.0,
            "status": "PROCESSING",
            "payment_simulate": None,
        }
    ])
    
    class RaceConditionPool(MockPool):
        def __init__(self):
            self.conn = conn
            
    pool = RaceConditionPool()
    count = await reconcile_stuck_payments(pool)
    
    assert count == 0
    assert len(pool.conn.updates) == 0
    assert len(pool.conn.inserts) == 0
