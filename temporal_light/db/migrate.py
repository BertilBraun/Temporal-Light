"""Migration entrypoint.

Run with:
    python -m temporal_light.db.migrate

Reads DATABASE_URL from the environment and applies schema.sql idempotently
(all statements use IF NOT EXISTS / IF EXISTS guards).
"""

from __future__ import annotations

import asyncio
import os
import pathlib

import asyncpg

_SCHEMA_PATH = pathlib.Path(__file__).parent / "schema.sql"


async def run_migration(
    database_url: str | None = None,
    *,
    pool: asyncpg.Pool | None = None,
) -> None:
    """Apply schema.sql idempotently.

    When pool is given, borrows a connection from it (no extra TCP connection).
    Otherwise opens a standalone connection using database_url, which defaults
    to os.environ["DATABASE_URL"] (the normal Docker entrypoint path).
    """
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")

    if pool is not None:
        async with pool.acquire() as conn:
            await conn.execute(schema_sql)
        return

    url = database_url if database_url is not None else os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(url)
    try:
        await conn.execute(schema_sql)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migration())
