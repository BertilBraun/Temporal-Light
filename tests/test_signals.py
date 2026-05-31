"""Unit tests for wait_for_signal — no database required."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from temporal_light.models import EventRecord, EventType
from temporal_light.signals import wait_for_signal
from temporal_light.worker.context import WorkflowContext, _current_workflow_context


def _make_received_signal(signal_type: str, payload: Any) -> EventRecord:
    return EventRecord(
        event_id=1,
        workflow_id='wf-1',
        step_index=-1,
        step_name=signal_type,
        event_type=EventType.SIGNAL,
        payload={'signal_type': signal_type, 'payload': payload},
        timestamp=datetime.now(timezone.utc),
    )


async def test_wait_for_signal_returns_payload_when_signal_wins_waiting_race(monkeypatch) -> None:
    received = _make_received_signal('approval', {'approved': True})
    context = WorkflowContext(workflow_id='wf-1', event_history=[])
    context_token = _current_workflow_context.set(context)
    calls: list[str] = []

    async def fake_write_event(**kwargs: Any) -> None:
        calls.append('write_event')

    async def fake_load_event_history(workflow_id: str) -> list[EventRecord]:
        # The only reload happens after the mark refuses to suspend; it must surface
        # the signal that won the race.
        return [received]

    async def fake_mark_workflow_waiting_for_signal(workflow_id: str, signal_type: str) -> bool:
        calls.append('mark_waiting')
        return False

    async def fail_if_called(**kwargs: Any) -> None:
        raise AssertionError('update_workflow_status must not be used to suspend on a won race')

    monkeypatch.setattr('temporal_light.signals.queries.write_event', fake_write_event)
    monkeypatch.setattr('temporal_light.signals.queries.load_event_history', fake_load_event_history)
    monkeypatch.setattr(
        'temporal_light.signals.queries.mark_workflow_waiting_for_signal',
        fake_mark_workflow_waiting_for_signal,
        raising=False,
    )
    monkeypatch.setattr('temporal_light.signals.queries.update_workflow_status', fail_if_called)

    try:
        result = await wait_for_signal('approval')
    finally:
        _current_workflow_context.reset(context_token)

    assert result == {'approved': True}
    assert 'mark_waiting' in calls
