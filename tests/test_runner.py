"""Unit tests for WorkflowRunner behavior."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from temporal_light.models import EventRecord, EventType, WorkflowRecord, WorkflowStatus
from temporal_light.worker.runner import WorkflowRunner


def _workflow_record(workflow_id: str, name: str) -> WorkflowRecord:
    now = datetime.now(timezone.utc)
    return WorkflowRecord(
        workflow_id=workflow_id,
        name=name,
        status=WorkflowStatus.RUNNING,
        run_at=now,
        locked_by='worker-1',
        locked_until=now,
        created_at=now,
        updated_at=now,
    )


def _started_event(workflow_id: str, payload: dict[str, Any]) -> EventRecord:
    return EventRecord(
        event_id=1,
        workflow_id=workflow_id,
        step_index=-1,
        step_name='workflow',
        event_type=EventType.STARTED,
        payload=payload,
        timestamp=datetime.now(timezone.utc),
    )


async def test_child_completion_uses_atomic_terminal_query(monkeypatch) -> None:
    async def child_flow() -> dict[str, bool]:
        return {'ok': True}

    calls: list[tuple[str, Any]] = []

    async def fake_load_event_history(workflow_id: str) -> list[EventRecord]:
        return [
            _started_event(
                workflow_id,
                {
                    'input': {},
                    'parent': {
                        'workflow_id': 'parent-1',
                        'child_id': workflow_id,
                    },
                },
            )
        ]

    async def fake_complete_workflow(
        workflow_id: str,
        result: Any,
        parent_info: dict[str, Any] | None = None,
    ) -> None:
        calls.append(('complete_workflow', (workflow_id, result, parent_info)))

    async def forbidden_call(*args: Any, **kwargs: Any) -> None:
        raise AssertionError('terminal child completion must use complete_workflow()')

    async def fake_release_workflow_lock(workflow_id: str) -> None:
        calls.append(('release_workflow_lock', workflow_id))

    monkeypatch.setattr('temporal_light.worker.runner.queries.load_event_history', fake_load_event_history)
    monkeypatch.setattr(
        'temporal_light.worker.runner.queries.complete_workflow',
        fake_complete_workflow,
        raising=False,
    )
    monkeypatch.setattr('temporal_light.worker.runner.queries.write_event', forbidden_call)
    monkeypatch.setattr('temporal_light.worker.runner.queries.update_workflow_status', forbidden_call)
    monkeypatch.setattr('temporal_light.worker.runner.queries.write_signal_and_wake_workflow', forbidden_call)
    monkeypatch.setattr('temporal_light.worker.runner.queries.release_workflow_lock', fake_release_workflow_lock)

    runner = WorkflowRunner({'child_flow': child_flow})
    await runner.run_workflow(_workflow_record('child-1', 'child_flow'))

    assert calls == [
        (
            'complete_workflow',
            (
                'child-1',
                {'ok': True},
                {'workflow_id': 'parent-1', 'child_id': 'child-1'},
            ),
        ),
        ('release_workflow_lock', 'child-1'),
    ]
