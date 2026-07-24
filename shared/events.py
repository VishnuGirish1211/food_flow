"""Standard event envelope for all Kafka events.

Every event published to Kafka follows this structure:
{
    "event_id": "uuid-v4",
    "event_type": "order.placed",
    "occurred_at": "2024-01-15T10:30:00+00:00",
    "payload": { ... }
}

The wrap_event() function generates event_id and occurred_at automatically.
Services pass only event_type and payload.
"""

import uuid
from datetime import datetime, timezone


def wrap_event(event_type: str, payload: dict) -> dict:
    """Create a standard event envelope.

    Args:
        event_type: Dot-separated event name (e.g. "order.placed").
        payload: Event-specific data.

    Returns:
        Complete event envelope with generated event_id and timestamp.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
