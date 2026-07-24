# Saga Integration Test Suite

Comprehensive integration tests for the FoodFlow v2 choreographed saga, exercising **real** Postgres transactions and **real** Kafka message delivery — no mocks.

## Prerequisites

- **Docker Desktop** must be running (testcontainers spins up ephemeral Postgres and Kafka containers automatically)
- **Python 3.12+**

## Setup

```powershell
# From the project root (c:\Ubuntu\home\vishn\projects\resto2)

# 1. Create and activate the virtual environment
python -m venv .venv
.venv\Scripts\activate

# 2. Install test dependencies
pip install -r requirements-test.txt
```

## Running the Tests

```powershell
# Activate venv first
.venv\Scripts\activate

# Run the full saga test suite
pytest tests/saga -v --tb=short

# Run a single test
pytest tests/saga/test_saga_happy_path.py -v

# Run with more output for debugging
pytest tests/saga -v --tb=long -s
```

No environment variables are required — testcontainers handles all container lifecycle and port allocation automatically.

## What Each Test Validates

| File | Scenario | Key Assertions |
|------|----------|----------------|
| `test_saga_happy_path.py` | Order → stock reserved → payment → confirmed | Stock decremented, payment SUCCEEDED, reservation exists |
| `test_saga_stock_rejected.py` | Order with qty > stock → rejected | Stock unchanged, no payment created, order STOCK_UNAVAILABLE |
| `test_saga_payment_failed_compensation.py` | Forced payment failure → compensation | Stock restored, reservation deleted, order PAYMENT_FAILED |
| `test_idempotency.py` | Same event published twice to Kafka | Stock decremented once, 1 processed_events row, 1 reservation |
| `test_concurrency_stock_lock.py` | 5 concurrent orders for 1 item | Exactly 1 confirmed, 4 rejected, stock=0 (no overselling) |
| `test_outbox_crash_recovery.py` | Unpublished outbox event replayed | Relay re-publishes, idempotency prevents double processing |

## Architecture

These tests run the service consumers and outbox relays **in-process** as asyncio background tasks, against testcontainer-managed Postgres and Kafka instances. This is different from the existing `tests/integration/` tests which hit the full docker-compose cluster through Nginx.

```
pytest process
├── Order consumer (asyncio task)
├── Restaurant consumer (asyncio task)
├── Payment consumer (asyncio task)
├── Order outbox relay (asyncio task)
├── Restaurant outbox relay (asyncio task)
├── Payment outbox relay (asyncio task)
├── PostgreSQL (testcontainer) ← order_db, restaurant_db, payment_db
└── Kafka (testcontainer) ← topics auto-created on first publish
```

## Isolation

- **Session-scoped containers**: Postgres and Kafka are started once for the session (they're expensive to boot).
- **Function-scoped reset**: Before each test, all tables are truncated and the restaurant seed data is re-inserted.
- **No backend code modified**: These tests only read service modules; they do not modify any application source.

## Troubleshooting

- **"Docker not available"**: Make sure Docker Desktop is running before you run the tests.
- **Tests hang**: The default poll timeout is 15 seconds. If the Kafka container is slow to start, the first test may take longer. Subsequent tests are fast.
- **Import errors**: Make sure you activated the venv (`.venv\Scripts\activate`) before running pytest.
