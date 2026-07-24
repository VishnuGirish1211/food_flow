"""Saga integration test fixtures.

Spins up ephemeral Postgres and Kafka containers via testcontainers,
creates the service databases and runs migrations, then launches each
service's consumer + outbox relay as background asyncio tasks.

The key challenge is event loop management: session-scoped async resources
(pools, Kafka consumers) must share the same event loop as the tests.
We solve this by using pytest-asyncio's session scope with careful loop
management.
"""

import asyncio
import importlib
import importlib.util
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import jwt as pyjwt
import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer

# ---------------------------------------------------------------------------
# Path setup — let us import shared modules without modifying services.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SERVICES_DIR = PROJECT_ROOT / "services"

# Ensure project root is on path for `shared.*` imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Constants ────────────────────────────────────────────────────────────────
RESTAURANT_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
ITEM_BURGER_ID = "11111111-1111-1111-1111-111111111111"
ITEM_BURGER_PRICE = 9.99
ITEM_BURGER_STOCK = 100
ITEM_FRIES_ID = "22222222-2222-2222-2222-222222222222"

JWT_SECRET = "dev-secret-change-in-production"
JWT_ALGORITHM = "HS256"


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_jwt_token(user_id: str | None = None, email: str = "test@example.com") -> str:
    """Generate a valid JWT token for test requests."""
    if user_id is None:
        user_id = str(uuid.uuid4())
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime(2099, 1, 1, tzinfo=timezone.utc),
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def wrap_event(event_type: str, payload: dict) -> dict:
    """Create a standard event envelope (mirrors shared/events.py)."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


async def poll_for_status(
    pool: asyncpg.Pool,
    table: str,
    id_column: str,
    id_value,
    status_column: str = "status",
    terminal_statuses: set[str] | None = None,
    timeout: float = 15.0,
    interval: float = 0.2,
) -> str:
    """Poll a database table until a row reaches a terminal status."""
    if terminal_statuses is None:
        terminal_statuses = {"CONFIRMED", "PAYMENT_FAILED", "STOCK_UNAVAILABLE"}

    elapsed = 0.0
    while elapsed < timeout:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT {status_column} FROM {table} WHERE {id_column} = $1",
                id_value,
            )
        if row and row[status_column] in terminal_statuses:
            return row[status_column]
        await asyncio.sleep(interval)
        elapsed += interval

    # Final check
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {status_column} FROM {table} WHERE {id_column} = $1",
            id_value,
        )
    if row:
        raise TimeoutError(
            f"Timed out waiting for {table}.{id_column}={id_value} to reach "
            f"one of {terminal_statuses}. Current status: {row[status_column]}"
        )
    raise TimeoutError(
        f"Timed out: row {table}.{id_column}={id_value} not found"
    )


async def poll_for_row(
    pool: asyncpg.Pool,
    query: str,
    params: list,
    timeout: float = 15.0,
    interval: float = 0.2,
):
    """Poll until a query returns a non-None result."""
    elapsed = 0.0
    while elapsed < timeout:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
        if row is not None:
            return row
        await asyncio.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Timed out waiting for row. Query: {query}, Params: {params}")


# ── JSONB codec helper ──────────────────────────────────────────────────────

async def _init_connection(conn: asyncpg.Connection):
    """Set up JSONB codec for asyncpg connections."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


# ── Module loader ────────────────────────────────────────────────────────────

def _load_service_module(service_name: str, module_file: str):
    """Load a service module by file path under a unique module name."""
    module_path = SERVICES_DIR / service_name / module_file
    unique_name = f"_svc_{service_name}_{module_file.replace('.py', '')}"

    if unique_name in sys.modules:
        return sys.modules[unique_name]

    spec = importlib.util.spec_from_file_location(unique_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = module

    svc_dir = str(SERVICES_DIR / service_name)
    sys.path.insert(0, svc_dir)
    try:
        spec.loader.exec_module(module)
    finally:
        if svc_dir in sys.path:
            sys.path.remove(svc_dir)

    return module


# ── Migration SQL ────────────────────────────────────────────────────────────

def _read_migration_files(service_name: str) -> list[str]:
    """Read all SQL migration files for a service."""
    migrations_dir = SERVICES_DIR / service_name / "migrations"
    if not migrations_dir.exists():
        return []
    files = sorted(migrations_dir.glob("*.sql"))
    return [f.read_text() for f in files]


ORDER_MIGRATIONS = _read_migration_files("order")
RESTAURANT_MIGRATIONS = _read_migration_files("restaurant")
PAYMENT_MIGRATIONS = _read_migration_files("payment")


# ── Seed SQL ─────────────────────────────────────────────────────────────────

RESTAURANT_SEED_SQL = """
INSERT INTO restaurants (id, name, description) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Burger Palace', 'Classic American burgers and fries'),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Pizza Planet', 'Wood-fired Neapolitan pizzas')
ON CONFLICT (id) DO NOTHING;

INSERT INTO menu_items (id, restaurant_id, name, description, price) VALUES
    ('11111111-1111-1111-1111-111111111111', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Classic Burger', 'Beef patty with lettuce, tomato, and cheese', 9.99),
    ('22222222-2222-2222-2222-222222222222', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Fries', 'Crispy golden fries', 4.99),
    ('33333333-3333-3333-3333-333333333333', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Milkshake', 'Vanilla milkshake', 5.99),
    ('44444444-4444-4444-4444-444444444444', 'b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Margherita Pizza', 'Fresh mozzarella, basil, and tomato sauce', 12.99),
    ('55555555-5555-5555-5555-555555555555', 'b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Pepperoni Pizza', 'Classic pepperoni with mozzarella', 14.99)
ON CONFLICT (id) DO NOTHING;

INSERT INTO menu_item_stock (item_id, quantity) VALUES
    ('11111111-1111-1111-1111-111111111111', 100),
    ('22222222-2222-2222-2222-222222222222', 200),
    ('33333333-3333-3333-3333-333333333333', 50),
    ('44444444-4444-4444-4444-444444444444', 80),
    ('55555555-5555-5555-5555-555555555555', 80)
ON CONFLICT (item_id) DO UPDATE SET quantity = EXCLUDED.quantity;
"""


# ═══════════════════════════════════════════════════════════════════════════
# Sync session-scoped fixtures (containers + connection details)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def postgres_container():
    """Start a PostgreSQL container for the test session."""
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def kafka_container():
    """Start a Kafka container for the test session."""
    with KafkaContainer("confluentinc/cp-kafka:7.5.3") as kafka:
        yield kafka


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container):
    """Return the Kafka bootstrap server address."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def pg_conn_info(postgres_container):
    """Return the Postgres connection info as a dict."""
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "user": postgres_container.username,
        "password": postgres_container.password,
        "dbname": postgres_container.dbname,
    }


@pytest.fixture(scope="session")
def _setup_databases(pg_conn_info):
    """Create service databases and run migrations (sync, runs once)."""
    import asyncio as _asyncio

    async def _setup():
        ci = pg_conn_info
        admin_conn = await asyncpg.connect(
            host=ci["host"], port=ci["port"],
            user=ci["user"], password=ci["password"],
            database=ci["dbname"],
        )
        for db_name in ("order_db", "restaurant_db", "payment_db"):
            exists = await admin_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", db_name
            )
            if not exists:
                await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
        await admin_conn.close()

        # Run migrations on each DB
        for svc_db, migrations in [
            ("order_db", ORDER_MIGRATIONS),
            ("restaurant_db", RESTAURANT_MIGRATIONS),
            ("payment_db", PAYMENT_MIGRATIONS),
        ]:
            conn = await asyncpg.connect(
                host=ci["host"], port=ci["port"],
                user=ci["user"], password=ci["password"],
                database=svc_db,
            )
            for sql in migrations:
                await conn.execute(sql)
            await conn.close()

    _asyncio.run(_setup())
    yield


# ═══════════════════════════════════════════════════════════════════════════
# Function-scoped async fixtures (pools, tasks, reset)
# These are created fresh for each test on the test's own event loop.
# ═══════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture
async def db_pools(pg_conn_info, _setup_databases):
    """Create asyncpg pools (one per service DB) for this test.

    Function-scoped: pools are created and closed per-test to avoid
    event loop conflicts.
    """
    ci = pg_conn_info
    pools = {}
    for svc_name, svc_db in [
        ("order", "order_db"),
        ("restaurant", "restaurant_db"),
        ("payment", "payment_db"),
    ]:
        pool = await asyncpg.create_pool(
            host=ci["host"], port=ci["port"],
            user=ci["user"], password=ci["password"],
            database=svc_db, min_size=2, max_size=10,
            init=_init_connection,
        )
        pools[svc_name] = pool

    yield pools

    for pool in pools.values():
        await pool.close()


@pytest_asyncio.fixture
async def kafka_producer_fn(kafka_bootstrap):
    """Create a Kafka producer for this test (function-scoped)."""
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest_asyncio.fixture
async def service_tasks(db_pools, kafka_bootstrap, kafka_producer_fn):
    """Start consumers + outbox relays as background tasks for this test."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kafka_bootstrap
    os.environ["PAYMENT_SIMULATE_DEFAULT"] = "success"

    shutdown_event = asyncio.Event()
    tasks = []

    # Load service consumer modules
    order_consumers = _load_service_module("order", "consumers.py")
    restaurant_consumers = _load_service_module("restaurant", "consumers.py")
    payment_consumers = _load_service_module("payment", "consumers.py")

    from shared.outbox import run_outbox_relay

    # Start consumers
    tasks.append(asyncio.create_task(
        order_consumers.run_consumer(db_pools["order"], shutdown_event),
        name="order-consumer",
    ))
    tasks.append(asyncio.create_task(
        restaurant_consumers.run_consumer(db_pools["restaurant"], shutdown_event),
        name="restaurant-consumer",
    ))
    tasks.append(asyncio.create_task(
        payment_consumers.run_consumer(db_pools["payment"], shutdown_event),
        name="payment-consumer",
    ))

    # Start outbox relays
    for svc_name in ("order", "restaurant", "payment"):
        tasks.append(asyncio.create_task(
            run_outbox_relay(
                db_pools[svc_name], kafka_producer_fn, shutdown_event,
                poll_interval=0.3,
            ),
            name=f"{svc_name}-outbox-relay",
        ))

    # Give consumers time to subscribe
    await asyncio.sleep(3)

    yield

    # Shutdown
    shutdown_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest_asyncio.fixture(autouse=True)
async def reset_databases(db_pools, service_tasks):
    """Truncate all tables and re-seed before each test."""
    # Truncate order_db
    async with db_pools["order"].acquire() as conn:
        await conn.execute(
            "TRUNCATE orders, outbox_events, processed_events CASCADE"
        )

    # Truncate restaurant_db
    async with db_pools["restaurant"].acquire() as conn:
        await conn.execute(
            "TRUNCATE stock_reservations, outbox_events, processed_events CASCADE"
        )
        await conn.execute("DELETE FROM menu_item_stock")
        await conn.execute("DELETE FROM menu_items")
        await conn.execute("DELETE FROM restaurants")
        await conn.execute(RESTAURANT_SEED_SQL)

    # Truncate payment_db
    async with db_pools["payment"].acquire() as conn:
        await conn.execute(
            "TRUNCATE payments, outbox_events, processed_events CASCADE"
        )

    await asyncio.sleep(0.5)
    yield


# ═══════════════════════════════════════════════════════════════════════════
# Convenience fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def jwt_token():
    """Return a valid JWT token."""
    return make_jwt_token()


@pytest_asyncio.fixture
async def raw_kafka_producer(kafka_bootstrap):
    """Create a raw Kafka producer for direct event publishing."""
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    yield producer
    await producer.stop()
