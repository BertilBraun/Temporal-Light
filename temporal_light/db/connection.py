from __future__ import annotations

import json

import asyncpg

_connection_pool: asyncpg.Pool | None = None


async def _initialize_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=json.loads,
        schema='pg_catalog',
    )


async def initialize_connection_pool(database_url: str) -> None:
    global _connection_pool
    _connection_pool = await asyncpg.create_pool(database_url, init=_initialize_connection)


async def get_connection_pool() -> asyncpg.Pool:
    if _connection_pool is None:
        raise RuntimeError('Connection pool is not initialized. Call initialize_connection_pool() first.')
    return _connection_pool


async def close_connection_pool() -> None:
    global _connection_pool
    if _connection_pool is not None:
        await _connection_pool.close()
        _connection_pool = None
