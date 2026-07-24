"""Deterministic payment gateway simulator.

Does not use randomness. Reads the `X-Payment-Simulate` value propagated
from the original HTTP request through the saga events.
"""

import os

# Default behavior if the client doesn't specify an override
PAYMENT_SIMULATE_DEFAULT = os.getenv("PAYMENT_SIMULATE_DEFAULT", "success")


async def process_payment(amount: float, simulate_directive: str | None) -> bool:
    """Simulate processing a payment.

    Returns:
        True if the payment succeeds, False if it fails.
    """
    directive = simulate_directive or PAYMENT_SIMULATE_DEFAULT

    if directive.lower() == "failure":
        return False
    return True
