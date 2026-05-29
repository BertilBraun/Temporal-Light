"""Integration tests for end-to-end workflow execution — requires TEST_DATABASE_URL."""

from __future__ import annotations

import pytest

from temporal_light.db.queries import (
    claim_next_workflow,
    create_workflow,
    get_workflow,
    load_event_history,
    register_worker,
    write_event,
)
from temporal_light.decorators import activity, workflow
from pydantic import BaseModel

from temporal_light.models import EventType, WorkflowStatus
from temporal_light.children import spawn_child, wait_for_child
from temporal_light.worker.runner import WorkflowRunner

pytestmark = pytest.mark.integration


class PaymentResult(BaseModel):
    transaction_id: str


async def _run(runner: WorkflowRunner, workflow_id: str) -> None:
    """Claim and run a workflow through the runner."""
    await register_worker('test-worker')
    record = await claim_next_workflow('test-worker')
    assert record is not None and record.workflow_id == workflow_id
    await runner.run_workflow(record)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_simple_workflow_completes_with_correct_result(
    clean_database: None,
) -> None:
    @workflow
    async def double_flow(value: int) -> int:
        return value * 2

    await create_workflow('wf-1', 'double_flow', {'value': 7})
    runner = WorkflowRunner({'double_flow': double_flow})
    await _run(runner, 'wf-1')

    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.COMPLETED

    history = await load_event_history('wf-1')
    terminal = history[-1]
    assert terminal.event_type == EventType.WORKFLOW_COMPLETED
    assert terminal.payload['result'] == 14


async def test_workflow_with_activity_writes_scheduled_and_completed_events(
    clean_database: None,
) -> None:
    @activity(retries=0, timeout=5)
    async def add_one(value: int) -> int:
        return value + 1

    @workflow
    async def activity_flow(value: int) -> int:
        return await add_one(value)

    await create_workflow('wf-1', 'activity_flow', {'value': 10})
    runner = WorkflowRunner({'activity_flow': activity_flow})
    await _run(runner, 'wf-1')

    history = await load_event_history('wf-1')
    event_types = [e.event_type for e in history]
    assert EventType.SCHEDULED in event_types
    assert EventType.COMPLETED in event_types
    assert EventType.WORKFLOW_COMPLETED in event_types

    completed = next(e for e in history if e.event_type == EventType.COMPLETED)
    assert completed.payload['result'] == 11
    assert 'duration_seconds' in completed.payload
    assert completed.payload['attempts_total'] == 1


async def test_parent_spawns_multiple_children_once_and_waits_for_results(
    clean_database: None,
) -> None:
    @workflow
    async def child_flow(value: int) -> int:
        return value * 10

    @workflow
    async def parent_flow() -> list[int]:
        first_child_id = await spawn_child('child_flow', value=1)
        second_child_id = await spawn_child('child_flow', value=2)

        first_result = await wait_for_child(first_child_id)
        second_result = await wait_for_child(second_child_id)
        return [first_result, second_result]

    await create_workflow('parent-1', 'parent_flow', {})
    runner = WorkflowRunner({'parent_flow': parent_flow, 'child_flow': child_flow})

    await _run(runner, 'parent-1')
    parent_record = await get_workflow('parent-1')
    assert parent_record is not None
    assert parent_record.status == WorkflowStatus.WAITING

    parent_history = await load_event_history('parent-1')
    child_started_events = [event for event in parent_history if event.event_type == EventType.CHILD_STARTED]
    assert len(child_started_events) == 2
    child_ids = [event.payload['child_id'] for event in child_started_events]
    assert child_ids[0] != child_ids[1]

    for child_id in child_ids:
        child_record = await get_workflow(child_id)
        assert child_record is not None
        await runner.run_workflow(child_record)

    parent_record = await get_workflow('parent-1')
    assert parent_record is not None
    assert parent_record.status == WorkflowStatus.RUNNING

    await _run(runner, 'parent-1')

    parent_history = await load_event_history('parent-1')
    child_started_events_after_replay = [
        event for event in parent_history if event.event_type == EventType.CHILD_STARTED
    ]
    assert len(child_started_events_after_replay) == 2

    parent_record = await get_workflow('parent-1')
    assert parent_record is not None
    assert parent_record.status == WorkflowStatus.COMPLETED

    terminal = parent_history[-1]
    assert terminal.event_type == EventType.WORKFLOW_COMPLETED
    assert terminal.payload['result'] == [10, 20]


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


async def test_activity_is_not_re_executed_on_replay(clean_database: None) -> None:
    execution_count = 0

    @activity(retries=0, timeout=5)
    async def counted_step() -> str:
        nonlocal execution_count
        execution_count += 1
        return 'done'

    @workflow
    async def replay_flow() -> str:
        return await counted_step()

    await create_workflow('wf-1', 'replay_flow', {})
    runner = WorkflowRunner({'replay_flow': replay_flow})

    # First run: executes for real
    await _run(runner, 'wf-1')
    assert execution_count == 1

    # Manually unlock so we can run it again (simulates worker handoff)
    from temporal_light.db import connection

    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE workflows SET locked_by = NULL, locked_until = NULL, '
            "status = 'running', run_at = NOW() WHERE workflow_id = 'wf-1'"
        )

    await _run(runner, 'wf-1')
    # Activity must NOT have been called a second time — result came from history
    assert execution_count == 1


async def test_activity_replay_restores_pydantic_result_from_annotation(clean_database: None) -> None:
    execution_count = 0

    @activity(retries=0, timeout=5)
    async def charge() -> PaymentResult:
        nonlocal execution_count
        execution_count += 1
        return PaymentResult(transaction_id='txn-1')

    @workflow
    async def typed_result_flow() -> str:
        payment_result = await charge()
        return payment_result.transaction_id

    await create_workflow('wf-1', 'typed_result_flow', {})
    await write_event(
        workflow_id='wf-1',
        step_index=0,
        step_name=charge.__qualname__,
        event_type=EventType.COMPLETED,
        payload={
            'result': {'transaction_id': 'txn-1'},
            'duration_seconds': 0.1,
            'attempts_total': 1,
        },
    )
    runner = WorkflowRunner({'typed_result_flow': typed_result_flow})

    record = await get_workflow('wf-1')
    assert record is not None
    await runner.run_workflow(record)

    assert execution_count == 0
    history = await load_event_history('wf-1')
    terminal = history[-1]
    assert terminal.event_type == EventType.WORKFLOW_COMPLETED
    assert terminal.payload['result'] == 'txn-1'


async def test_activity_input_change_is_reported_as_divergence(clean_database: None) -> None:
    @activity(retries=0, timeout=5)
    async def echo(value: int) -> int:
        return value

    @workflow
    async def input_divergence_flow() -> int:
        return await echo(2)

    await create_workflow('wf-1', 'input_divergence_flow', {})
    await write_event(
        workflow_id='wf-1',
        step_index=0,
        step_name=echo.__qualname__,
        event_type=EventType.SCHEDULED,
        payload={'step_name': echo.__qualname__, 'input': {'args': [1], 'kwargs': {}}},
    )

    runner = WorkflowRunner({'input_divergence_flow': input_divergence_flow})
    record = await get_workflow('wf-1')
    assert record is not None
    await runner.run_workflow(record)

    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.FAILED

    history = await load_event_history('wf-1')
    wf_failed = next(e for e in history if e.event_type == EventType.WORKFLOW_FAILED)
    assert 'input arguments changed' in wf_failed.payload['error']
    assert 'non-deterministic workflow code outside an activity' in wf_failed.payload['error']


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------


async def test_activity_retry_increments_failed_event_count(
    clean_database: None,
) -> None:
    attempt_number = 0

    @activity(retries=2, timeout=5, backoff_seconds=0)
    async def flaky_step() -> str:
        nonlocal attempt_number
        attempt_number += 1
        if attempt_number < 2:
            raise RuntimeError('transient failure')
        return 'ok'

    @workflow
    async def retry_flow() -> str:
        return await flaky_step()

    await create_workflow('wf-1', 'retry_flow', {})
    runner = WorkflowRunner({'retry_flow': retry_flow})

    # First run: fails → writes failed event, suspends
    await _run(runner, 'wf-1')
    history = await load_event_history('wf-1')
    failed_events = [e for e in history if e.event_type == EventType.FAILED]
    assert len(failed_events) == 1
    assert failed_events[0].payload['attempt'] == 0

    # Unlock for retry
    from temporal_light.db import connection

    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE workflows SET locked_by = NULL, locked_until = NULL, '
            "status = 'running', run_at = NOW() WHERE workflow_id = 'wf-1'"
        )

    # Second run (replay + retry): succeeds
    await _run(runner, 'wf-1')
    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.COMPLETED

    history = await load_event_history('wf-1')
    completed = next(e for e in history if e.event_type == EventType.COMPLETED)
    assert completed.payload['attempts_total'] == 2


async def test_activity_exhausting_retries_marks_workflow_failed(
    clean_database: None,
) -> None:
    @activity(retries=1, timeout=5, backoff_seconds=0)
    async def always_fails() -> None:
        raise RuntimeError('permanent failure')

    @workflow
    async def failing_flow() -> None:
        await always_fails()

    await create_workflow('wf-1', 'failing_flow', {})
    runner = WorkflowRunner({'failing_flow': failing_flow})

    pool = await _get_pool()
    await _run(runner, 'wf-1')  # attempt 1 → fails, schedules retry
    await _unlock('wf-1', pool)
    await _run(runner, 'wf-1')  # attempt 2 → exhausts retries → FAILED

    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.FAILED

    history = await load_event_history('wf-1')
    assert any(e.event_type == EventType.WORKFLOW_FAILED for e in history)


# ---------------------------------------------------------------------------
# Divergence detection
# ---------------------------------------------------------------------------


async def test_divergence_detected_when_step_name_changes(
    clean_database: None,
) -> None:
    await create_workflow('wf-1', 'diverge_flow', {})

    # Simulate a workflow that scheduled "original_step" but hasn't completed it
    # (the activity was in-flight when the worker died).
    await write_event(
        workflow_id='wf-1',
        step_index=0,
        step_name='original_step',
        event_type=EventType.SCHEDULED,
        payload={'step_name': 'original_step', 'input': {'args': [], 'kwargs': {}}},
    )

    # Deploy new code that calls a differently-named step at the same position.
    @activity(retries=0, timeout=5)
    async def replacement_step() -> str:
        return 'second'

    @workflow
    async def diverge_flow() -> str:
        return await replacement_step()

    runner = WorkflowRunner({'diverge_flow': diverge_flow})
    await _run(runner, 'wf-1')

    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.FAILED

    history = await load_event_history('wf-1')
    wf_failed = next(e for e in history if e.event_type == EventType.WORKFLOW_FAILED)
    assert 'Divergence' in wf_failed.payload['error']


# ---------------------------------------------------------------------------
# Unknown workflow name
# ---------------------------------------------------------------------------


async def test_unknown_workflow_name_marks_workflow_failed(
    clean_database: None,
) -> None:
    await create_workflow('wf-1', 'unknown_flow', {})
    runner = WorkflowRunner({})  # registry is empty
    await _run(runner, 'wf-1')

    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.FAILED


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_pool():
    from temporal_light.db import connection

    return await connection.get_connection_pool()


async def _unlock(workflow_id: str, pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE workflows SET locked_by = NULL, locked_until = NULL, '
            "status = 'running', run_at = NOW() WHERE workflow_id = $1",
            workflow_id,
        )
