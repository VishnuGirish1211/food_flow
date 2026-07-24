"""Database connection pool for the Restaurant service."""

import json
import os
from pathlib import Path

import asyncpg
import structlog

logger = structlog.get_logger()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/restaurant_db"
)


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        init=_init_connection,
    )
    await _run_migrations(pool)
    logger.info("database_pool_created", database="restaurant_db")
    return pool


async def _init_connection(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def _run_migrations(pool: asyncpg.Pool):
    migrations_dir = Path(__file__).parent / "migrations"
    if not migrations_dir.exists():
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    async with pool.acquire() as conn:
        for sql_file in migration_files:
            sql = sql_file.read_text()
            await conn.execute(sql)
            logger.info("migration_applied", file=sql_file.name)
