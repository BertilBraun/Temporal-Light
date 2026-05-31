from __future__ import annotations

import asyncio
import time

import asyncpg

from temporal_light.db import connection
from temporal_light.models import EventType, WorkflowStatus


async def truncate_database() -> None:
    async with connection.acquire() as conn:
        await conn.execute('TRUNCATE TABLE events, workflows, workers RESTART IDENTITY CASCADE')


async def insert_workflow(
    workflow_id: str,
    name: str = 'my_flow',
    status: str = 'running',
    run_at_sql: str = 'NOW()',
) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO workflows (workflow_id, name, status, run_at, created_at, updated_at)
            VALUES ($1, $2, $3, {run_at_sql}, NOW(), NOW())
            """,
            workflow_id,
            name,
            status,
        )


async def set_workflow_run_at_sql(workflow_id: str, run_at_sql: str) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            f'UPDATE workflows SET run_at = {run_at_sql}, updated_at = NOW() WHERE workflow_id = $1',
            workflow_id,
        )


async def unlock_workflow(workflow_id: str) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            """
            UPDATE workflows
            SET locked_by = NULL, locked_until = NULL, status = 'running', run_at = NOW(), updated_at = NOW()
            WHERE workflow_id = $1
            """,
            workflow_id,
        )


async def workflow_status_counts() -> dict[str, int]:
    async with connection.acquire() as conn:
        rows = await conn.fetch('SELECT status, count(*) AS n FROM workflows GROUP BY status')
    return {row['status']: row['n'] for row in rows}


async def count_workflows_by_name(name: str) -> int:
    async with connection.acquire() as conn:
        return await conn.fetchval('SELECT count(*) FROM workflows WHERE name = $1', name)


async def workflows_not_completed() -> list[asyncpg.Record]:
    async with connection.acquire() as conn:
        return await conn.fetch(
            'SELECT workflow_id, status FROM workflows WHERE status != $1',
            WorkflowStatus.COMPLETED.value,
        )


async def steps_completed_more_than_once() -> list[asyncpg.Record]:
    """Steps with more than one COMPLETED event — the signature of double execution."""
    async with connection.acquire() as conn:
        return await conn.fetch(
            """
            SELECT workflow_id, step_index
            FROM events
            WHERE event_type = $1
            GROUP BY workflow_id, step_index
            HAVING count(*) > 1
            """,
            EventType.COMPLETED.value,
        )


async def workflows_with_multiple_terminal_events() -> list[asyncpg.Record]:
    async with connection.acquire() as conn:
        return await conn.fetch(
            """
            SELECT workflow_id
            FROM events
            WHERE event_type IN ($1, $2)
            GROUP BY workflow_id
            HAVING count(*) > 1
            """,
            EventType.WORKFLOW_COMPLETED.value,
            EventType.WORKFLOW_FAILED.value,
        )


async def wait_for_all_terminal(expected_workflow_count: int, timeout_seconds: float) -> dict[str, int]:
    """Poll until every workflow row exists and none are still running or waiting."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        counts = await workflow_status_counts()
        total = sum(counts.values())
        active = counts.get(WorkflowStatus.RUNNING.value, 0) + counts.get(WorkflowStatus.WAITING.value, 0)
        if total >= expected_workflow_count and active == 0:
            return counts
        if time.monotonic() > deadline:
            raise AssertionError(
                f'workflows did not all finish within {timeout_seconds}s: '
                f'{counts} (total={total}/{expected_workflow_count})'
            )
        await asyncio.sleep(0.2)
