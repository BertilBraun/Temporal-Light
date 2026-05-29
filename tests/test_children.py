"""Unit tests for child workflow helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from temporal_light.children import CHILD_COMPLETED_SIGNAL_TYPE, wait_for_child
from temporal_light.models import EventRecord, EventType
from temporal_light.worker.context import WorkflowContext, _current_workflow_context


def _make_child_signal(child_id: str, result: Any) -> EventRecord:
    return EventRecord(
        event_id=1,
        workflow_id='parent-1',
        step_index=-1,
        step_name='signal',
        event_type=EventType.SIGNAL,
        payload={
            'signal_type': CHILD_COMPLETED_SIGNAL_TYPE,
            'payload': {
                'child_id': child_id,
                'status': 'completed',
                'result': result,
            },
        },
        timestamp=datetime.now(timezone.utc),
    )


async def test_wait_for_child_returns_result_when_completion_wins_waiting_race(monkeypatch) -> None:
    child_signal = _make_child_signal('child-1', {'ok': True})
    context = WorkflowContext(workflow_id='parent-1', event_history=[])
    context_token = _current_workflow_context.set(context)
    calls: list[tuple[str, Any]] = []

    async def fake_write_event(**kwargs: Any) -> None:
        calls.append(('write_event', kwargs))

    history_reads = iter([[], [child_signal]])

    async def fake_load_event_history(workflow_id: str) -> list[EventRecord]:
        calls.append(('load_event_history', workflow_id))
        return next(history_reads)

    async def fake_mark_workflow_waiting_for_child(
        workflow_id: str,
        child_id: str,
    ) -> bool:
        calls.append(('mark_waiting', (workflow_id, child_id)))
        return False

    async def fake_update_workflow_status(**kwargs: Any) -> None:
        calls.append(('update_workflow_status', kwargs))

    monkeypatch.setattr('temporal_light.children.queries.write_event', fake_write_event)
    monkeypatch.setattr('temporal_light.children.queries.load_event_history', fake_load_event_history)
    monkeypatch.setattr('temporal_light.children.queries.update_workflow_status', fake_update_workflow_status)
    monkeypatch.setattr(
        'temporal_light.children.queries.mark_workflow_waiting_for_child',
        fake_mark_workflow_waiting_for_child,
        raising=False,
    )

    try:
        result = await wait_for_child('child-1')
    finally:
        _current_workflow_context.reset(context_token)

    assert result == {'ok': True}
    assert ('mark_waiting', ('parent-1', 'child-1')) in calls
    assert all(call[0] != 'update_workflow_status' for call in calls)
