"""Integration tests for DB query functions — requires TEST_DATABASE_URL."""

from __future__ import annotations

import pytest

from temporal_light.db.queries import (
    claim_next_workflow,
    create_workflow,
    create_child_workflow,
    get_workflow,
    load_event_history,
    load_events_after,
    write_event,
    write_signal_and_wake_workflow,
)
from temporal_light.models import EventType, WorkflowStatus

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# create_workflow + load_event_history
# ---------------------------------------------------------------------------


async def test_create_workflow_writes_started_event(clean_database: None) -> None:
    await create_workflow('wf-1', 'order_flow', {'order_id': '123', 'amount': 99.0})

    history = await load_event_history('wf-1')
    assert len(history) == 1
    started = history[0]
    assert started.event_type == EventType.STARTED
    assert started.step_index == -1
    assert started.payload['input'] == {'order_id': '123', 'amount': 99.0}


async def test_create_workflow_sets_status_to_running(clean_database: None) -> None:
    await create_workflow('wf-1', 'order_flow', {})
    record = await get_workflow('wf-1')
    assert record is not None
    assert record.status == WorkflowStatus.RUNNING


async def test_get_workflow_returns_none_for_unknown_id(clean_database: None) -> None:
    result = await get_workflow('does-not-exist')
    assert result is None


# ---------------------------------------------------------------------------
# write_event + load_event_history
# ---------------------------------------------------------------------------


async def test_write_event_appends_to_history(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})
    await write_event(
        workflow_id='wf-1',
        step_index=0,
        step_name='charge_payment',
        event_type=EventType.SCHEDULED,
        payload={'step_name': 'charge_payment', 'input': {'args': [], 'kwargs': {}}},
    )

    history = await load_event_history('wf-1')
    assert len(history) == 2  # started + scheduled
    assert history[1].event_type == EventType.SCHEDULED
    assert history[1].step_index == 0


async def test_write_event_payload_is_decoded_as_dict(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})
    await write_event(
        workflow_id='wf-1',
        step_index=0,
        step_name='charge_payment',
        event_type=EventType.COMPLETED,
        payload={'result': {'transaction_id': 'txn-abc'}, 'duration_seconds': 0.5},
    )

    history = await load_event_history('wf-1')
    completed = history[-1]
    assert isinstance(completed.payload, dict)
    assert completed.payload['result']['transaction_id'] == 'txn-abc'
    assert completed.payload['duration_seconds'] == 0.5


async def test_load_events_after_returns_only_newer_events(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})
    history = await load_event_history('wf-1')
    first_event_id = history[0].event_id

    await write_event('wf-1', 0, 'step_a', EventType.SCHEDULED, {'step_name': 'step_a', 'input': {}})
    await write_event('wf-1', 0, 'step_a', EventType.COMPLETED, {'result': None})

    newer = await load_events_after('wf-1', first_event_id)
    assert len(newer) == 2
    assert newer[0].event_type == EventType.SCHEDULED
    assert newer[1].event_type == EventType.COMPLETED


# ---------------------------------------------------------------------------
# claim_next_workflow
# ---------------------------------------------------------------------------


async def test_claim_next_workflow_locks_the_row(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})

    claimed = await claim_next_workflow('worker-a')
    assert claimed is not None
    assert claimed.workflow_id == 'wf-1'
    assert claimed.locked_by == 'worker-a'
    assert claimed.locked_until is not None


async def test_claim_next_workflow_second_worker_gets_nothing(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})
    await claim_next_workflow('worker-a')

    second_claim = await claim_next_workflow('worker-b')
    assert second_claim is None


async def test_claim_next_workflow_returns_none_when_no_workflows(
    clean_database: None,
) -> None:
    result = await claim_next_workflow('worker-a')
    assert result is None


async def test_claim_next_workflow_oldest_run_at_claimed_first(
    clean_database: None,
) -> None:
    await create_workflow('wf-new', 'my_flow', {})
    # Force wf-old to have an older run_at by updating directly
    from temporal_light.db import connection

    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO workflows (workflow_id, name, status, run_at, created_at, updated_at) '
            "VALUES ('wf-old', 'my_flow', 'running', NOW() - INTERVAL '1 hour', NOW(), NOW())"
        )

    claimed = await claim_next_workflow('worker-a')
    assert claimed is not None
    assert claimed.workflow_id == 'wf-old'


# ---------------------------------------------------------------------------
# write_signal_and_wake_workflow
# ---------------------------------------------------------------------------


async def test_write_signal_stores_signal_event(clean_database: None) -> None:
    await create_workflow('wf-1', 'my_flow', {})
    await write_signal_and_wake_workflow('wf-1', 'approval', {'approved': True})

    history = await load_event_history('wf-1')
    signal_events = [e for e in history if e.event_type == EventType.SIGNAL]
    assert len(signal_events) == 1
    assert signal_events[0].payload['signal_type'] == 'approval'
    assert signal_events[0].payload['payload'] == {'approved': True}


async def test_write_signal_makes_workflow_immediately_claimable(
    clean_database: None,
) -> None:

    await create_workflow('wf-1', 'my_flow', {})

    # Push run_at far into the future so it's not claimable
    from temporal_light.db import connection

    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE workflows SET run_at = '9999-01-01' WHERE workflow_id = 'wf-1'")

    not_claimable = await claim_next_workflow('worker-a')
    assert not_claimable is None

    # Signal should reset run_at to NOW()
    await write_signal_and_wake_workflow('wf-1', 'approval', {})
    now_claimable = await claim_next_workflow('worker-a')
    assert now_claimable is not None
    assert now_claimable.workflow_id == 'wf-1'


# ---------------------------------------------------------------------------
# create_child_workflow
# ---------------------------------------------------------------------------


async def test_create_child_workflow_writes_parent_child_started_and_child_started_atomically(
    clean_database: None,
) -> None:
    await create_workflow('parent-1', 'parent_flow', {})

    await create_child_workflow(
        parent_workflow_id='parent-1',
        child_workflow_id='child-1',
        child_workflow_name='child_flow',
        child_workflow_input={'value': 7},
        parent_step_index=0,
    )

    parent_history = await load_event_history('parent-1')
    child_started = parent_history[-1]
    assert child_started.event_type == EventType.CHILD_STARTED
    assert child_started.step_index == 0
    assert child_started.payload == {
        'child_id': 'child-1',
        'workflow_name': 'child_flow',
        'input': {'value': 7},
    }

    child_record = await get_workflow('child-1')
    assert child_record is not None
    assert child_record.name == 'child_flow'
    assert child_record.status == WorkflowStatus.RUNNING

    child_history = await load_event_history('child-1')
    assert child_history[0].event_type == EventType.STARTED
    assert child_history[0].payload == {
        'input': {'value': 7},
        'parent': {'workflow_id': 'parent-1', 'child_id': 'child-1'},
    }


async def test_create_child_workflow_is_idempotent_on_duplicate_child_id(
    clean_database: None,
) -> None:
    await create_workflow('parent-1', 'parent_flow', {})

    first_created = await create_child_workflow(
        parent_workflow_id='parent-1',
        child_workflow_id='child-1',
        child_workflow_name='child_flow',
        child_workflow_input={'value': 7},
        parent_step_index=0,
    )
    second_created = await create_child_workflow(
        parent_workflow_id='parent-1',
        child_workflow_id='child-1',
        child_workflow_name='child_flow',
        child_workflow_input={'value': 7},
        parent_step_index=0,
    )

    assert first_created is True
    assert second_created is False

    parent_history = await load_event_history('parent-1')
    child_started_events = [e for e in parent_history if e.event_type == EventType.CHILD_STARTED]
    assert len(child_started_events) == 1
