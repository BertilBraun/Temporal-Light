from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine
from typing import Any

from .models import ActivityPolicy


def workflow(function: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
    """Mark an async function as a workflow definition.

    The decorated function is otherwise unchanged — the Worker uses
    __is_workflow__ to validate registrations.
    """
    function.__is_workflow__ = True  # type: ignore[attr-defined]
    return function


def activity(
    retries: int = 0,
    timeout: float = 60.0,
    backoff_seconds: float = 5.0,
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
    """Decorator that marks a function as a durable activity.

    When called inside a workflow context, the wrapper routes through the
    executor (replay check → forward execution → retry scheduling). When called
    outside a workflow context (e.g., in tests), the original function is called
    directly.
    """

    def decorator(
        function: Callable[..., Coroutine[Any, Any, Any]],
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        policy = ActivityPolicy(
            max_retries=retries,
            timeout_seconds=timeout,
            backoff_seconds=backoff_seconds,
        )

        @functools.wraps(function)
        async def wrapper(*positional_arguments: Any, **keyword_arguments: Any) -> Any:
            from .worker.context import _current_workflow_context

            try:
                workflow_context = _current_workflow_context.get()
            except LookupError:
                return await function(*positional_arguments, **keyword_arguments)

            from .worker.executor import execute_activity

            return await execute_activity(
                workflow_context=workflow_context,
                activity_function=function,
                activity_policy=policy,
                positional_arguments=positional_arguments,
                keyword_arguments=keyword_arguments,
            )

        wrapper.__activity_policy__ = policy  # type: ignore[attr-defined]
        wrapper.__is_activity__ = True  # type: ignore[attr-defined]
        return wrapper

    return decorator
