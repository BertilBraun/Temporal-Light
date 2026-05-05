"""sleep() and wait_for_signal() — workflow suspension primitives."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .db import queries
from .exceptions import WorkflowSuspended
from .models import EventType
from .worker.context import _current_workflow_context

_FAR_FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


async def sleep(
    *,
    seconds: float = 0,
    minutes: float = 0,
    hours: float = 0,
    days: float = 0,
) -> None:
    """Suspend the workflow for the given duration.

    On first encounter: writes a sleep event and raises WorkflowSuspended.
    On replay: returns immediately (the scheduler only resumes the workflow
    after run_at <= now, so wakeup_at is always in the past by the time we
    replay through this step).
    """
    workflow_context = _current_workflow_context.get()
    step_index = workflow_context.next_step_index()

    existing_sleep_event = workflow_context.find_event(step_index, EventType.SLEEP)
    if existing_sleep_event is not None:
        wakeup_at_raw: str = existing_sleep_event.payload["wakeup_at"]
        wakeup_at = datetime.fromisoformat(wakeup_at_raw)
        if datetime.now(timezone.utc) >= wakeup_at:
            return
        # Defensive re-suspend: should not occur in practice since the scheduler
        # only picks up workflows when run_at <= now.
        await queries.update_workflow_run_at(
            workflow_id=workflow_context.workflow_id,
            run_at=wakeup_at,
        )
        raise WorkflowSuspended(f"Re-suspended sleep at step {step_index}.")

    total_seconds = seconds + minutes * 60 + hours * 3600 + days * 86400
    wakeup_at = datetime.now(timezone.utc) + timedelta(seconds=total_seconds)

    await queries.write_event(
        workflow_id=workflow_context.workflow_id,
        step_index=step_index,
        step_name="sleep",
        event_type=EventType.SLEEP,
        payload={"wakeup_at": wakeup_at.isoformat()},
    )
    await queries.update_workflow_run_at(
        workflow_id=workflow_context.workflow_id,
        run_at=wakeup_at,
    )
    raise WorkflowSuspended(f"Workflow sleeping until {wakeup_at.isoformat()}.")


async def wait_for_signal(signal_type: str) -> Any:
    """Suspend the workflow until a signal of the given type is received.

    On first encounter: pushes run_at to far-future and raises WorkflowSuspended.
    The API's signal endpoint resets run_at = NOW() when the signal arrives,
    causing the scheduler to re-pick up the workflow.
    On replay: finds the signal event in history and returns its payload.
    """
    workflow_context = _current_workflow_context.get()
    workflow_context.next_step_index()

    signal_event = workflow_context.find_signal_event(signal_type)
    if signal_event is not None:
        return signal_event.payload.get("payload")

    await queries.update_workflow_run_at(
        workflow_id=workflow_context.workflow_id,
        run_at=_FAR_FUTURE,
    )
    raise WorkflowSuspended(
        f"Workflow waiting for signal '{signal_type}'."
    )
