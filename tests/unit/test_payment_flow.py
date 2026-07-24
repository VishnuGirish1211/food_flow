"""Unit tests for the payment flow simulator."""

import pytest
from services.payment.gateway import process_payment

@pytest.mark.asyncio
async def test_payment_success_default():
    assert await process_payment(100.0, None) is True

@pytest.mark.asyncio
async def test_payment_success_explicit():
    assert await process_payment(100.0, "success") is True

@pytest.mark.asyncio
async def test_payment_failure_explicit():
    assert await process_payment(100.0, "failure") is False
