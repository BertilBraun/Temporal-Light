"""Shared fixtures.

Unit tests run with no setup required.
Integration tests require TEST_DATABASE_URL to be set; they are skipped
automatically when it is absent so the suite stays green in environments
without a database.
"""

from __future__ import annotations

import os

import pytest

from temporal_light.db import connection
from temporal_light.db.migrate import run_migration

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if TEST_DATABASE_URL:
        return
    skip = pytest.mark.skip(reason="TEST_DATABASE_URL not set")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip)


@pytest.fixture(scope="session")
async def database_pool():
    """Session-scoped pool; runs migration once for the whole test session."""
    assert TEST_DATABASE_URL, "TEST_DATABASE_URL must be set for integration tests"
    await connection.initialize_connection_pool(TEST_DATABASE_URL)
    await run_migration()
    yield
    await connection.close_connection_pool()


@pytest.fixture
async def clean_database(database_pool: None) -> None:
    """Truncate all tables before each integration test for isolation."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE events, workflows, workers RESTART IDENTITY CASCADE"
        )
