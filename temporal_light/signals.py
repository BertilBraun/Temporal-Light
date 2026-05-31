"""wait_for_signal() — suspend a workflow until a named signal is received."""

from __future__ import annotations

from typing import Any

from .db import queries
from .exceptions import WorkflowSuspended
from .models import EventRecord, EventType
from .worker.context import _current_workflow_context


async def wait_for_signal(signal_type: str) -> Any:
    """Suspend the workflow until a signal of the given type is received.

    On first encounter: writes a waiting marker event, sets status=WAITING, and
    raises WorkflowSuspended. The API's signal endpoint atomically writes the signal
    event and resets status=running, causing the scheduler to re-claim the workflow.
    On replay: finds the received signal event in history and returns its payload.
    """
    workflow_context = _current_workflow_context.get()
    workflow_context.next_step_index()

    signal_event = workflow_context.find_signal_event(signal_type)
    if signal_event is not None:
        return signal_event.payload.get('payload')

    if workflow_context.find_waiting_for_signal_event(signal_type) is None:
        await queries.write_event(
            workflow_id=workflow_context.workflow_id,
            step_index=-1,
            step_name=signal_type,
            event_type=EventType.SIGNAL,
            payload={'signal_type': signal_type, 'status': 'waiting'},
        )

    marked_waiting = await queries.mark_workflow_waiting_for_signal(
        workflow_id=workflow_context.workflow_id,
        signal_type=signal_type,
    )
    if not marked_waiting:
        fresh_signal = _find_received_signal(
            await queries.load_event_history(workflow_context.workflow_id), signal_type
        )
        if fresh_signal is not None:
            return fresh_signal.payload.get('payload')

    raise WorkflowSuspended(f"Workflow waiting for signal '{signal_type}'.")


def _find_received_signal(events: list[EventRecord], signal_type: str) -> EventRecord | None:
    for event in events:
        if (
            event.step_index == -1
            and event.event_type == EventType.SIGNAL
            and event.payload is not None
            and event.payload.get('signal_type') == signal_type
            and event.payload.get('status') != 'waiting'
        ):
            return event
    return None
