"""Activity execution: replay, forward execution, retries, timeout, divergence."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Any, get_type_hints

from ..db import queries
from ..exceptions import DivergenceError, WorkflowSuspended
from ..models import ActivityPolicy, EventType
from ..serialization import from_json_safe, to_json_safe
from .context import WorkflowContext


async def execute_activity(
    workflow_context: WorkflowContext,
    activity_function: Callable[..., Coroutine[Any, Any, Any]],
    activity_policy: ActivityPolicy,
    positional_arguments: tuple[Any, ...],
    keyword_arguments: dict[str, Any],
) -> Any:
    """Core execution path for a single activity invocation.

    Handles, in order:
      1. Replay: return from history if a completed event exists.
      2. Divergence detection: verify step name matches the scheduled event.
      3. Forward execution: run the activity with timeout.
      4. On failure: write failed event, schedule retry or mark workflow failed.
    """
    step_index = workflow_context.next_step_index()
    step_name = activity_function.__qualname__

    # --- Replay path ---
    completed_event = workflow_context.find_completed_event(step_index)
    if completed_event is not None:
        return from_json_safe(
            completed_event.payload['result'],
            _activity_return_annotation(activity_function),
        )

    serializable_arguments = _serialize_arguments(positional_arguments, keyword_arguments)

    # --- Divergence detection ---
    scheduled_event = workflow_context.find_scheduled_event(step_index)
    if scheduled_event is not None:
        recorded_step_name: str = scheduled_event.payload['step_name']
        if recorded_step_name != step_name:
            raise DivergenceError(
                f"Step {step_index}: history recorded '{recorded_step_name}' "
                f"but code now calls '{step_name}'. "
                f'Workflow code changed incompatibly while the workflow was in flight.'
            )
        recorded_arguments = scheduled_event.payload.get('input')
        if recorded_arguments != serializable_arguments:
            raise DivergenceError(
                f"Step {step_index}: history recorded input arguments {recorded_arguments!r} "
                f"but code now calls '{step_name}' with {serializable_arguments!r}. "
                'Activity input arguments changed while the workflow was in flight. '
                'If the workflow code did not change intentionally, this is a strong signal of '
                'non-deterministic workflow code outside an activity.'
            )

    # --- Write scheduled event (idempotent: only on first real execution) ---
    if scheduled_event is None:
        await queries.write_event(
            workflow_id=workflow_context.workflow_id,
            step_index=step_index,
            step_name=step_name,
            event_type=EventType.SCHEDULED,
            payload={'step_name': step_name, 'input': serializable_arguments},
        )

    # --- Determine current attempt number from failed events in history ---
    failed_attempt_count = workflow_context.count_failed_events(step_index)

    # --- Execute ---
    started_at = datetime.now(timezone.utc)
    try:
        result = await asyncio.wait_for(
            activity_function(*positional_arguments, **keyword_arguments),
            timeout=activity_policy.timeout_seconds,
        )
    except Exception as execution_error:
        duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        await _handle_activity_failure(
            workflow_context=workflow_context,
            step_index=step_index,
            step_name=step_name,
            activity_policy=activity_policy,
            failed_attempt_count=failed_attempt_count,
            execution_error=execution_error,
            duration_seconds=duration_seconds,
        )
        raise WorkflowSuspended(f"Activity '{step_name}' failed on attempt {failed_attempt_count}.")

    # --- Success ---
    duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
    await queries.write_event(
        workflow_id=workflow_context.workflow_id,
        step_index=step_index,
        step_name=step_name,
        event_type=EventType.COMPLETED,
        payload={
            'result': to_json_safe(result),
            'duration_seconds': duration_seconds,
            'attempts_total': failed_attempt_count + 1,
        },
    )
    return result


async def _handle_activity_failure(
    workflow_context: WorkflowContext,
    step_index: int,
    step_name: str,
    activity_policy: ActivityPolicy,
    failed_attempt_count: int,
    execution_error: Exception,
    duration_seconds: float,
) -> None:
    error_payload = {
        'error_type': type(execution_error).__name__,
        'error_message': str(execution_error),
        'attempt': failed_attempt_count,
        'duration_seconds': duration_seconds,
    }
    await queries.write_event(
        workflow_id=workflow_context.workflow_id,
        step_index=step_index,
        step_name=step_name,
        event_type=EventType.FAILED,
        payload=error_payload,
    )

    attempts_used = failed_attempt_count + 1
    if attempts_used <= activity_policy.max_retries:
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=activity_policy.backoff_seconds)
        await queries.update_workflow_run_at(
            workflow_id=workflow_context.workflow_id,
            run_at=retry_at,
        )
    else:
        await queries.fail_workflow(
            workflow_id=workflow_context.workflow_id,
            error_message=(
                f"Activity '{step_name}' exhausted all {activity_policy.max_retries} "
                f'retries. Last error: {type(execution_error).__name__}: {execution_error}'
            ),
            parent_info=workflow_context.parent_info,
        )


def _serialize_arguments(
    positional_arguments: tuple[Any, ...],
    keyword_arguments: dict[str, Any],
) -> dict[str, Any]:
    """Convert call arguments into a JSON-safe dict for storage in the event log."""
    return {
        'args': [to_json_safe(argument) for argument in positional_arguments],
        'kwargs': {
            key: to_json_safe(value)
            for key, value in keyword_arguments.items()
        },
    }


def _activity_return_annotation(activity_function: Callable[..., Coroutine[Any, Any, Any]]) -> Any | None:
    return get_type_hints(activity_function).get('return')
