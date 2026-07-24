"""Request ID middleware and structured logging setup.

Every HTTP request gets a unique request_id (either from the
X-Request-ID header or auto-generated). This ID is bound to
structlog's context so all log entries within the request include it.
"""

import logging
import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger()


def setup_logging(service_name: str):
    """Configure structlog for JSON output with service name binding.

    Call once at service startup before any logging happens.
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )
    # Bind service name globally so every log entry includes it.
    structlog.contextvars.bind_contextvars(service=service_name)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Injects a request_id into every HTTP request.

    If the client sends X-Request-ID, it's reused. Otherwise a new
    UUID is generated. The ID is added to structlog's context vars
    and returned in the response headers.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))

        # Bind to structlog context for this request
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        # Store on request.state so route handlers can access it
        request.state.request_id = request_id

        start_time = time.perf_counter()
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        response.headers["X-Request-ID"] = request_id

        logger.info(
            "request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        return response
