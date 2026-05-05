"""sleep() — suspend a workflow for a fixed duration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .db import queries
from .exceptions import WorkflowSuspended
from .models import EventType
from .worker.context import _current_workflow_context


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


