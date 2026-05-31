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

import pytest

import db_helpers
import stress_workflows
from stress_workflows import compute_expected_total, iter_parent_specs, run_worker
from temporal_light.db import queries
from temporal_light.models import EventType

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

        await db_helpers.wait_for_all_terminal(expected_workflow_count, _GLOBAL_TIMEOUT_SECONDS)
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.join(timeout=10)

    double_executed = await db_helpers.steps_completed_more_than_once()
    assert not double_executed, f'steps executed more than once: {double_executed}'

    actual_child_count = await db_helpers.count_workflows_by_name('stress_child')
    assert actual_child_count == expected_child_count, (
        f'expected {expected_child_count} children, found {actual_child_count} (duplicates or orphans)'
    )

    non_completed = await db_helpers.workflows_not_completed()
    assert not non_completed, f'workflows not completed: {non_completed}'

    multiple_terminals = await db_helpers.workflows_with_multiple_terminal_events()
    assert not multiple_terminals, f'workflows with more than one terminal event: {multiple_terminals}'

    for spec in parent_specs:
        history = await queries.load_event_history(spec.parent_id)
        terminal = history[-1]
        assert terminal.event_type == EventType.WORKFLOW_COMPLETED
        assert terminal.payload['result']['total'] == compute_expected_total(spec)
