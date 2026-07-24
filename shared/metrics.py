"""Prometheus metrics definitions and /metrics endpoint.

Shared across all services. Each service imports the counters/histograms
it needs and increments them in its own code.
"""

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.requests import Request
from starlette.responses import Response

# ── HTTP metrics ─────────────────────────────────────────────────────────

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)

# ── Kafka metrics ────────────────────────────────────────────────────────

kafka_messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic", "status"],
)

# ── Outbox metrics ───────────────────────────────────────────────────────

outbox_events_published_total = Counter(
    "outbox_events_published_total",
    "Total outbox events published to Kafka",
)


# ── Endpoint ─────────────────────────────────────────────────────────────

async def metrics_endpoint(request: Request) -> Response:
    """Prometheus metrics scrape endpoint. Mount at /metrics."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
