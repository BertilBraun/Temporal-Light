"""Multi-process stress test — requires TEST_DATABASE_URL and RUN_STRESS_TEST=1.

Launches real worker subprocesses (so cross-process locking, heartbeats, and the
process-pool activity path are exercised for real), starts hundreds of workflows
that sleep, spawn children, and wait for signals, kills a worker mid-flight, then
asserts every result is correct and no workflow was duplicated or double-executed.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import time

import pytest

import stress_workflows
from stress_workflows import compute_expected_total, iter_parent_specs, run_worker
from temporal_light.db import connection, queries
from temporal_light.models import EventType, WorkflowStatus

pytestmark = [pytest.mark.integration, pytest.mark.stress]

_LOCK_SECONDS = 10
_HEARTBEAT_SECONDS = 3
_GLOBAL_TIMEOUT_SECONDS = 300


def _spawn_worker(spawn_context: multiprocessing.context.BaseContext, database_url: str) -> multiprocessing.Process:
    # Workers cannot be daemons: each one owns a ProcessPoolExecutor for activities,
    # and daemonic processes are not allowed to have children.
    worker = spawn_context.Process(
        target=run_worker,
        args=(database_url, _LOCK_SECONDS, _HEARTBEAT_SECONDS),
        daemon=False,
    )
    worker.start()
    return worker


async def _await_all_terminal(expected_workflow_count: int) -> dict[str, int]:
    deadline = time.monotonic() + _GLOBAL_TIMEOUT_SECONDS
    while True:
        async with connection.acquire() as conn:
            rows = await conn.fetch('SELECT status, count(*) AS n FROM workflows GROUP BY status')
        status_counts = {row['status']: row['n'] for row in rows}
        total = sum(status_counts.values())
        active = status_counts.get(WorkflowStatus.RUNNING.value, 0) + status_counts.get(WorkflowStatus.WAITING.value, 0)
        if total >= expected_workflow_count and active == 0:
            return status_counts
        if time.monotonic() > deadline:
            raise AssertionError(
                f'Timed out after {_GLOBAL_TIMEOUT_SECONDS}s. '
                f'status_counts={status_counts}, total={total}/{expected_workflow_count}'
            )
        await asyncio.sleep(1)


async def test_stress_multiprocess_workflows_are_correct_and_unique(clean_database: None) -> None:
    database_url = os.environ['TEST_DATABASE_URL']
    parent_count = int(os.environ.get('STRESS_WORKFLOWS', '200'))
    worker_count = int(os.environ.get('STRESS_WORKERS', '4'))

    parent_specs = list(iter_parent_specs(parent_count))
    expected_child_count = sum(spec.num_children for spec in parent_specs)
    expected_workflow_count = parent_count + expected_child_count

    spawn_context = multiprocessing.get_context('spawn')
    workers = [_spawn_worker(spawn_context, database_url) for _ in range(worker_count)]
    try:
        for spec in parent_specs:
            await queries.create_workflow(spec.parent_id, 'stress_parent', spec.workflow_input())

        # Let workers pick up work, then release every parent. Early signals are
        # durable, so this need not be synchronized with each workflow's wait point.
        await asyncio.sleep(3)
        for spec in parent_specs:
            await queries.write_signal_and_wake_workflow(
                spec.parent_id, stress_workflows.RELEASE_SIGNAL, {'value': spec.signal_value}
            )

        # Chaos: kill a worker mid-flight and replace it. In-flight workflows whose
        # locks lapse must be reclaimed by another worker and still finish correctly.
        await asyncio.sleep(8)
        workers[0].terminate()
        workers[0].join(timeout=10)
        workers[0] = _spawn_worker(spawn_context, database_url)

        await _await_all_terminal(expected_workflow_count)
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.join(timeout=10)

    async with connection.acquire() as conn:
        double_executed = await conn.fetch(
            """
            SELECT workflow_id, step_index
            FROM events
            WHERE event_type = $1
            GROUP BY workflow_id, step_index
            HAVING count(*) > 1
            """,
            EventType.COMPLETED.value,
        )
        assert not double_executed, f'steps executed more than once: {double_executed}'

        actual_child_count = await conn.fetchval("SELECT count(*) FROM workflows WHERE name = 'stress_child'")
        assert actual_child_count == expected_child_count, (
            f'expected {expected_child_count} children, found {actual_child_count} (duplicates or orphans)'
        )

        non_completed = await conn.fetch("SELECT workflow_id, status FROM workflows WHERE status != 'completed'")
        assert not non_completed, f'workflows not completed: {non_completed}'

        multiple_terminals = await conn.fetch(
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
        assert not multiple_terminals, f'workflows with more than one terminal event: {multiple_terminals}'

    for spec in parent_specs:
        history = await queries.load_event_history(spec.parent_id)
        terminal = history[-1]
        assert terminal.event_type == EventType.WORKFLOW_COMPLETED
        assert terminal.payload['result']['total'] == compute_expected_total(spec)
