"""Database connection pool for the Auth service.

Connects to auth_db and runs migrations on startup.
Uses asyncpg's JSONB codec so JSONB columns are automatically
serialized/deserialized as Python dicts.
"""

import json
import os
from pathlib import Path

import asyncpg
import structlog

logger = structlog.get_logger()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/auth_db"
)


async def create_pool() -> asyncpg.Pool:
    """Create the asyncpg connection pool and run migrations."""
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        init=_init_connection,
    )
    await _run_migrations(pool)
    logger.info("database_pool_created", database="auth_db")
    return pool


async def _init_connection(conn: asyncpg.Connection):
    """Set up JSONB codec for each new connection."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def _run_migrations(pool: asyncpg.Pool):
    """Run all SQL migration files in order."""
    migrations_dir = Path(__file__).parent / "migrations"
    if not migrations_dir.exists():
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    async with pool.acquire() as conn:
        for sql_file in migration_files:
            sql = sql_file.read_text()
            await conn.execute(sql)
            logger.info("migration_applied", file=sql_file.name)
