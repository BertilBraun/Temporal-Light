"""wait_for_signal() — suspend a workflow until a named signal is received."""

from __future__ import annotations

from typing import Any

from .db import queries
from .exceptions import WorkflowSuspended
from .models import EventType, WorkflowStatus
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
        # Guard against the race where the signal arrived between loading history
        # and writing the waiting marker: re-read fresh DB history and check again.
        fresh_history = await queries.load_event_history(workflow_context.workflow_id)
        for event in fresh_history:
            if (
                event.step_index == -1
                and event.event_type == EventType.SIGNAL
                and event.payload is not None
                and event.payload.get('signal_type') == signal_type
                and event.payload.get('status') != 'waiting'
            ):
                return event.payload.get('payload')

    await queries.update_workflow_status(
        workflow_id=workflow_context.workflow_id,
        status=WorkflowStatus.WAITING,
    )
    raise WorkflowSuspended(f"Workflow waiting for signal '{signal_type}'.")
