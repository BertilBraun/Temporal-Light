"""Unit tests for WorkflowContext — no database required."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from temporal_light.models import EventRecord, EventType
from temporal_light.worker.context import WorkflowContext


def _make_event(
    event_id: int,
    step_index: int,
    event_type: EventType,
    step_name: str = 'some_activity',
    payload: dict | None = None,
) -> EventRecord:
    return EventRecord(
        event_id=event_id,
        workflow_id='wf-test',
        step_index=step_index,
        step_name=step_name,
        event_type=event_type,
        payload=payload or {},
        timestamp=datetime.now(timezone.utc),
    )


def _empty_context() -> WorkflowContext:
    return WorkflowContext(workflow_id='wf-test', event_history=[])


# ---------------------------------------------------------------------------
# next_step_index
# ---------------------------------------------------------------------------


def test_step_counter_starts_at_zero() -> None:
    assert _empty_context().next_step_index() == 0


def test_step_counter_increments_on_each_call() -> None:
    context = _empty_context()
    indices = [context.next_step_index() for _ in range(5)]
    assert indices == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# find_event / find_completed_event / find_scheduled_event
# ---------------------------------------------------------------------------


def test_find_completed_event_returns_matching_event() -> None:
    event = _make_event(1, 0, EventType.COMPLETED)
    context = WorkflowContext(workflow_id='wf-test', event_history=[event])
    assert context.find_completed_event(0) is event


def test_find_completed_event_returns_none_for_wrong_step() -> None:
    event = _make_event(1, 0, EventType.COMPLETED)
    context = WorkflowContext(workflow_id='wf-test', event_history=[event])
    assert context.find_completed_event(1) is None


def test_find_completed_event_returns_none_for_wrong_type() -> None:
    event = _make_event(1, 0, EventType.SCHEDULED)
    context = WorkflowContext(workflow_id='wf-test', event_history=[event])
    assert context.find_completed_event(0) is None


def test_find_scheduled_event_returns_matching_event() -> None:
    event = _make_event(1, 2, EventType.SCHEDULED)
    context = WorkflowContext(workflow_id='wf-test', event_history=[event])
    assert context.find_scheduled_event(2) is event


def test_find_event_returns_first_match_when_multiple_present() -> None:
    first = _make_event(1, 0, EventType.FAILED)
    second = _make_event(2, 0, EventType.FAILED)
    context = WorkflowContext(workflow_id='wf-test', event_history=[first, second])
    assert context.find_event(0, EventType.FAILED) is first


# ---------------------------------------------------------------------------
# find_signal_event
# ---------------------------------------------------------------------------


def test_find_signal_event_returns_received_signal() -> None:
    signal = _make_event(
        1,
        -1,
        EventType.SIGNAL,
        step_name='signal',
        payload={'signal_type': 'approval', 'payload': {'approved': True}},
    )
    context = WorkflowContext(workflow_id='wf-test', event_history=[signal])
    result = context.find_signal_event('approval')
    assert result is signal


def test_find_signal_event_returns_none_for_different_type() -> None:
    signal = _make_event(
        1,
        -1,
        EventType.SIGNAL,
        step_name='signal',
        payload={'signal_type': 'approval', 'payload': {}},
    )
    context = WorkflowContext(workflow_id='wf-test', event_history=[signal])
    assert context.find_signal_event('rejection') is None


def test_find_signal_event_ignores_waiting_marker() -> None:
    waiting_marker = _make_event(
        1,
        -1,
        EventType.SIGNAL,
        step_name='signal',
        payload={'signal_type': 'approval', 'status': 'waiting'},
    )
    context = WorkflowContext(workflow_id='wf-test', event_history=[waiting_marker])
    assert context.find_signal_event('approval') is None


def test_find_signal_event_returns_received_signal_not_waiting_marker() -> None:
    waiting_marker = _make_event(
        1,
        -1,
        EventType.SIGNAL,
        step_name='signal',
        payload={'signal_type': 'approval', 'status': 'waiting'},
    )
    received = _make_event(
        2,
        -1,
        EventType.SIGNAL,
        step_name='signal',
        payload={'signal_type': 'approval', 'payload': {'approved': True}},
    )
    context = WorkflowContext(workflow_id='wf-test', event_history=[waiting_marker, received])
    assert context.find_signal_event('approval') is received


# ---------------------------------------------------------------------------
# count_failed_events
# ---------------------------------------------------------------------------


def test_count_failed_events_returns_zero_with_no_failures() -> None:
    context = WorkflowContext(
        workflow_id='wf-test',
        event_history=[_make_event(1, 0, EventType.SCHEDULED)],
    )
    assert context.count_failed_events(0) == 0


def test_count_failed_events_counts_only_matching_step() -> None:
    history = [
        _make_event(1, 0, EventType.FAILED),
        _make_event(2, 0, EventType.FAILED),
        _make_event(3, 1, EventType.FAILED),  # different step
    ]
    context = WorkflowContext(workflow_id='wf-test', event_history=history)
    assert context.count_failed_events(0) == 2
    assert context.count_failed_events(1) == 1


@pytest.mark.parametrize('failure_count', [0, 1, 2, 3])
def test_count_failed_events_matches_parametrized_counts(failure_count: int) -> None:
    history = [_make_event(i + 1, 0, EventType.FAILED) for i in range(failure_count)]
    context = WorkflowContext(workflow_id='wf-test', event_history=history)
    assert context.count_failed_events(0) == failure_count
