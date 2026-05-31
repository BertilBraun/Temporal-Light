"""Child workflow primitives."""

from __future__ import annotations

from typing import Any

from .db import queries
from .exceptions import ChildWorkflowFailedError, WorkflowSuspended
from .models import CHILD_COMPLETED_SIGNAL_TYPE, EventRecord, EventType
from .worker.context import _current_workflow_context


async def spawn_child(workflow_name: str, *, child_id: str | None = None, **kwargs: Any) -> str:
    """Start a child workflow once and return its workflow id.

    The child id is the idempotency key: create_child_workflow inserts it as the
    workflows primary key, so two concurrent or replayed parent runs that resolve
    the same id collapse to a single child. When child_id is omitted it defaults to
    the parent id and step index; pass a stable business key when the parent's code
    path before this call is not guaranteed deterministic.
    """
    workflow_context = _current_workflow_context.get()
    step_index = workflow_context.next_step_index()

    existing_child_started_event = workflow_context.find_child_started_event(step_index)
    if existing_child_started_event is not None:
        return existing_child_started_event.payload['child_id']

    resolved_child_id = child_id or f'{workflow_context.workflow_id}:{step_index}'
    await queries.create_child_workflow(
        parent_workflow_id=workflow_context.workflow_id,
        child_workflow_id=resolved_child_id,
        child_workflow_name=workflow_name,
        child_workflow_input=kwargs,
        parent_step_index=step_index,
    )
    return resolved_child_id


async def wait_for_child(child_id: str) -> Any:
    """Suspend until child_id completes, then return its result.

    Child completion is delivered as a reserved signal in the parent's event
    history. mark_workflow_waiting_for_child checks for that completion under a row
    lock and refuses to suspend if it already arrived, which closes the race where
    the child completes between loading history and reaching this call.
    """
    workflow_context = _current_workflow_context.get()
    workflow_context.next_step_index()

    child_result_signal = workflow_context.find_child_result_signal(child_id)
    if child_result_signal is not None:
        return _child_signal_result(child_result_signal)

    if not _has_waiting_child_marker(workflow_context.event_history, child_id):
        await queries.write_event(
            workflow_id=workflow_context.workflow_id,
            step_index=-1,
            step_name='child',
            event_type=EventType.SIGNAL,
            payload={
                'signal_type': CHILD_COMPLETED_SIGNAL_TYPE,
                'status': 'waiting',
                'child_id': child_id,
            },
        )

    marked_waiting = await queries.mark_workflow_waiting_for_child(
        workflow_id=workflow_context.workflow_id,
        child_id=child_id,
    )
    if not marked_waiting:
        fresh_history = await queries.load_event_history(workflow_context.workflow_id)
        fresh_result_signal = _find_child_result_signal(fresh_history, child_id)
        if fresh_result_signal is not None:
            return _child_signal_result(fresh_result_signal)

    raise WorkflowSuspended(f"Workflow waiting for child '{child_id}'.")


def _child_signal_result(event: EventRecord) -> Any:
    payload = event.payload.get('payload', {})
    status = payload.get('status')
    if status == 'completed':
        return payload.get('result')
    if status == 'failed':
        raise ChildWorkflowFailedError(
            child_id=payload.get('child_id', '<unknown>'),
            error=payload.get('error', 'Unknown child workflow error'),
        )
    raise ChildWorkflowFailedError(
        child_id=payload.get('child_id', '<unknown>'),
        error=f"Unknown child workflow status '{status}'.",
    )


def _find_child_result_signal(events: list[EventRecord], child_id: str) -> EventRecord | None:
    for event in events:
        if event.step_index != -1 or event.event_type != EventType.SIGNAL or event.payload is None:
            continue
        if event.payload.get('signal_type') != CHILD_COMPLETED_SIGNAL_TYPE:
            continue
        payload = event.payload.get('payload')
        if isinstance(payload, dict) and payload.get('child_id') == child_id:
            return event
    return None


def _has_waiting_child_marker(events: list[EventRecord], child_id: str) -> bool:
    for event in events:
        if event.step_index != -1 or event.event_type != EventType.SIGNAL or event.payload is None:
            continue
        if (
            event.payload.get('signal_type') == CHILD_COMPLETED_SIGNAL_TYPE
            and event.payload.get('status') == 'waiting'
            and event.payload.get('child_id') == child_id
        ):
            return True
    return False
