from __future__ import annotations

import os
import sys
from collections.abc import AsyncGenerator

import asyncpg
import pytest

from temporal_light.db import connection
from temporal_light.db.migrate import run_migration

# Make sibling test support modules (e.g. sample_activities) importable here and in
# process-pool subprocesses, which inherit sys.path from this process.
sys.path.insert(0, os.path.dirname(__file__))

TEST_DATABASE_URL = os.environ.get('TEST_DATABASE_URL')
RUN_STRESS_TEST = os.environ.get('RUN_STRESS_TEST')


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    skip_integration = pytest.mark.skip(reason='TEST_DATABASE_URL not set')
    skip_stress = pytest.mark.skip(reason='RUN_STRESS_TEST not set')
    for item in items:
        if item.get_closest_marker('integration') and not TEST_DATABASE_URL:
            item.add_marker(skip_integration)
        if item.get_closest_marker('stress') and not RUN_STRESS_TEST:
            item.add_marker(skip_stress)


@pytest.fixture(scope='session')
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    assert TEST_DATABASE_URL, 'TEST_DATABASE_URL must be set for integration tests'
    pool = await asyncpg.create_pool(
        TEST_DATABASE_URL,
        init=connection._initialize_connection,
    )
    await run_migration(pool=pool)
    # Register this pool as the module-level singleton so production code finds it.
    connection._connection_pool = pool
    yield pool
    await pool.close()


@pytest.fixture
async def clean_database(db_pool: asyncpg.Pool) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('TRUNCATE TABLE events, workflows, workers RESTART IDENTITY CASCADE')
