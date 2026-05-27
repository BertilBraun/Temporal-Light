"""WorkflowRunner: loads history, sets up context, executes or replays a workflow."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from ..children import CHILD_COMPLETED_SIGNAL_TYPE
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
                error_message=(f"No workflow named '{workflow_record.name}' is registered with this worker."),
            )
            return

        started_event = next(
            (e for e in event_history if e.event_type == EventType.STARTED),
            None,
        )
        if started_event is None:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message='STARTED event is missing — workflow row is corrupt.',
            )
            return

        workflow_input: dict[str, Any] = started_event.payload.get('input', {})
        parent_info = started_event.payload.get('parent')

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
            await _signal_parent_if_workflow_failed_after_suspension(
                workflow_id=workflow_record.workflow_id,
                parent_info=parent_info,
            )
            return
        except DivergenceError as divergence_error:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message=f'Divergence: {divergence_error}',
                parent_info=parent_info,
            )
            return
        except Exception as unexpected_error:
            await _fail_workflow(
                workflow_id=workflow_record.workflow_id,
                error_message=(
                    f'Unhandled exception in workflow code: {type(unexpected_error).__name__}: {unexpected_error}'
                ),
                parent_info=parent_info,
            )
            return
        else:
            await queries.write_event(
                workflow_id=workflow_record.workflow_id,
                step_index=-1,
                step_name='workflow',
                event_type=EventType.WORKFLOW_COMPLETED,
                payload={'result': result},
            )
            await queries.update_workflow_status(
                workflow_id=workflow_record.workflow_id,
                status=WorkflowStatus.COMPLETED,
            )
            await _signal_parent_if_child_workflow(
                parent_info=parent_info,
                status='completed',
                result=result,
            )
        finally:
            _current_workflow_context.reset(context_token)
            await queries.release_workflow_lock(workflow_record.workflow_id)


async def _fail_workflow(
    workflow_id: str,
    error_message: str,
    parent_info: dict[str, Any] | None = None,
) -> None:
    await queries.write_event(
        workflow_id=workflow_id,
        step_index=-1,
        step_name='workflow',
        event_type=EventType.WORKFLOW_FAILED,
        payload={'error': error_message},
    )
    await queries.update_workflow_status(
        workflow_id=workflow_id,
        status=WorkflowStatus.FAILED,
    )
    await _signal_parent_if_child_workflow(
        parent_info=parent_info,
        status='failed',
        error=error_message,
    )
    await queries.release_workflow_lock(workflow_id)


async def _signal_parent_if_workflow_failed_after_suspension(
    workflow_id: str,
    parent_info: dict[str, Any] | None,
) -> None:
    if parent_info is None:
        return

    workflow_record = await queries.get_workflow(workflow_id)
    if workflow_record is None or workflow_record.status != WorkflowStatus.FAILED:
        return

    fresh_history = await queries.load_event_history(workflow_id)
    failed_event = next(
        (event for event in reversed(fresh_history) if event.event_type == EventType.WORKFLOW_FAILED),
        None,
    )
    error_message = 'Child workflow failed.'
    if failed_event is not None:
        error_message = failed_event.payload.get('error', error_message)

    await _signal_parent_if_child_workflow(
        parent_info=parent_info,
        status='failed',
        error=error_message,
    )


async def _signal_parent_if_child_workflow(
    parent_info: dict[str, Any] | None,
    status: str,
    result: Any = None,
    error: str | None = None,
) -> None:
    if parent_info is None:
        return

    payload: dict[str, Any] = {
        'child_id': parent_info['child_id'],
        'status': status,
    }
    if status == 'completed':
        payload['result'] = result
    else:
        payload['error'] = error

    await queries.write_signal_and_wake_workflow(
        workflow_id=parent_info['workflow_id'],
        signal_type=CHILD_COMPLETED_SIGNAL_TYPE,
        signal_payload=payload,
    )
