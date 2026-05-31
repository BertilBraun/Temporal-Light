from __future__ import annotations

import pytest

from temporal_light.db import connection


class _FakeTransaction:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> None:
        self.entered = True

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.exited = True


class _FakeConnection:
    def __init__(self) -> None:
        self.transaction_context = _FakeTransaction()

    def transaction(self) -> _FakeTransaction:
        return self.transaction_context


class _FakeAcquire:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> _FakeConnection:
        self.entered = True
        return self.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.exited = True


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConnection()
        self.acquire_context = _FakeAcquire(self.conn)

    def acquire(self) -> _FakeAcquire:
        return self.acquire_context


@pytest.fixture(autouse=True)
def restore_connection_pool():
    previous_pool = connection.set_connection_pool(None)
    try:
        yield
    finally:
        connection.set_connection_pool(previous_pool)


async def test_set_connection_pool_installs_pool_and_returns_previous_pool() -> None:
    first_pool = _FakePool()
    second_pool = _FakePool()

    assert connection.set_connection_pool(first_pool) is None
    assert await connection.get_connection_pool() is first_pool

    previous_pool = connection.set_connection_pool(second_pool)

    assert previous_pool is first_pool
    assert await connection.get_connection_pool() is second_pool


async def test_acquire_yields_connection_from_installed_pool() -> None:
    pool = _FakePool()
    connection.set_connection_pool(pool)

    async with connection.acquire() as conn:
        assert conn is pool.conn
        assert pool.acquire_context.entered is True

    assert pool.acquire_context.exited is True


async def test_transaction_acquires_connection_and_opens_transaction() -> None:
    pool = _FakePool()
    connection.set_connection_pool(pool)

    async with connection.transaction() as conn:
        assert conn is pool.conn
        assert pool.acquire_context.entered is True
        assert pool.conn.transaction_context.entered is True

    assert pool.conn.transaction_context.exited is True
    assert pool.acquire_context.exited is True
