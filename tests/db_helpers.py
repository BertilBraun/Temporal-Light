from __future__ import annotations

from temporal_light.db import connection


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
