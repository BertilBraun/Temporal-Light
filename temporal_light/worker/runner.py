"""WorkflowRunner: loads history, sets up context, executes or replays a workflow."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from ..db import queries
from ..exceptions import DivergenceError, WorkflowSuspended
from ..models import EventType, WorkflowRecord, WorkflowStatus
from .context import WorkflowContext, _current_workflow_context


class WorkflowRunner:
    """Executes a single workflow run given a claimed WorkflowRecord.

    workflow_registry maps workflow __qualname__ → the decorated async function.
    """

    def __init__(
        self,
        workflow_registry: dict[str, Callable[..., Coroutine[Any, Any, Any]]],
    ) -> None:
        self.workflow_registry = workflow_registry

    async def run_workflow(self, workflow_record: WorkflowRecord) -> None:
        """Load history, set context, invoke workflow coroutine, handle outcome."""
        event_history = await queries.load_event_history(workflow_record.workflow_id)

        workflow_function = self.workflow_registry.get(workflow_record.name)
        if workflow_function is None:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message=(
                    f"No workflow named '{workflow_record.name}' is registered with this worker."
                ),
            )
            return

        started_event = next(
            (e for e in event_history if e.event_type == EventType.STARTED),
            None,
        )
        if started_event is None:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message="STARTED event is missing — workflow row is corrupt.",
            )
            return

        workflow_input: dict[str, Any] = started_event.payload.get("input", {})

        workflow_context = WorkflowContext(
            workflow_id=workflow_record.workflow_id,
            event_history=event_history,
        )
        context_token = _current_workflow_context.set(workflow_context)

        try:
            result = await workflow_function(**workflow_input)
        except WorkflowSuspended:
            # Normal suspension (retry backoff, sleep, signal wait).
            # run_at was already updated by the executor; just release the lock.
            return
        except DivergenceError as divergence_error:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message=f"Divergence: {divergence_error}",
            )
            return
        except Exception as unexpected_error:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message=(
                    f"Unhandled exception in workflow code: "
                    f"{type(unexpected_error).__name__}: {unexpected_error}"
                ),
            )
            return
        else:
            await queries.write_event(
                workflow_id=workflow_record.workflow_id,
                step_index=-1,
                step_name="workflow",
                event_type=EventType.WORKFLOW_COMPLETED,
                payload={"result": result},
            )
            await queries.update_workflow_status(
                workflow_id=workflow_record.workflow_id,
                status=WorkflowStatus.COMPLETED,
            )
        finally:
            _current_workflow_context.reset(context_token)
            await queries.release_workflow_lock(workflow_record.workflow_id)


async def _fail_workflow(workflow_id: str, error_message: str) -> None:
    await queries.write_event(
        workflow_id=workflow_id,
        step_index=-1,
        step_name="workflow",
        event_type=EventType.WORKFLOW_FAILED,
        payload={"error": error_message},
    )
    await queries.update_workflow_status(
        workflow_id=workflow_id,
        status=WorkflowStatus.FAILED,
    )
    await queries.release_workflow_lock(workflow_id)
