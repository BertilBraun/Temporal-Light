from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from .. import profiling

_connection_pool: asyncpg.Pool | None = None


class _TimingConnection:
    """Delegating wrapper that records query wall time into the profiling buckets."""

    __slots__ = ('_connection',)

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        with profiling.timed(profiling.current_db_bucket()):
            return await self._connection.execute(*args, **kwargs)

    async def executemany(self, *args: Any, **kwargs: Any) -> Any:
        with profiling.timed(profiling.current_db_bucket()):
            return await self._connection.executemany(*args, **kwargs)

    async def fetch(self, *args: Any, **kwargs: Any) -> Any:
        with profiling.timed(profiling.current_db_bucket()):
            return await self._connection.fetch(*args, **kwargs)

    async def fetchrow(self, *args: Any, **kwargs: Any) -> Any:
        with profiling.timed(profiling.current_db_bucket()):
            return await self._connection.fetchrow(*args, **kwargs)

    async def fetchval(self, *args: Any, **kwargs: Any) -> Any:
        with profiling.timed(profiling.current_db_bucket()):
            return await self._connection.fetchval(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


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
        yield _TimingConnection(conn) if profiling.enabled() else conn


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
