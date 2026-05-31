from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg

_connection_pool: asyncpg.Pool | None = None


async def _initialize_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=json.loads,
        schema='pg_catalog',
    )


def set_connection_pool(pool: asyncpg.Pool | None) -> asyncpg.Pool | None:
    global _connection_pool
    previous_pool = _connection_pool
    _connection_pool = pool
    return previous_pool


async def initialize_connection_pool(database_url: str) -> None:
    pool = await asyncpg.create_pool(database_url, init=_initialize_connection)
    set_connection_pool(pool)


async def get_connection_pool() -> asyncpg.Pool:
    if _connection_pool is None:
        raise RuntimeError('Connection pool is not initialized. Call initialize_connection_pool() first.')
    return _connection_pool


@asynccontextmanager
async def acquire() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await get_connection_pool()
    async with pool.acquire() as conn:
        yield conn


@asynccontextmanager
async def transaction() -> AsyncGenerator[asyncpg.Connection, None]:
    async with acquire() as conn:
        async with conn.transaction():
            yield conn


async def close_connection_pool() -> None:
    global _connection_pool
    if _connection_pool is not None:
        await _connection_pool.close()
        set_connection_pool(None)
