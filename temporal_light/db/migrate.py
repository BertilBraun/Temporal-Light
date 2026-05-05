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


async def run_migration() -> None:
    database_url = os.environ["DATABASE_URL"]
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(schema_sql)
        print("Migration completed successfully.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migration())
