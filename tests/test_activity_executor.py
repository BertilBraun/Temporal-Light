from __future__ import annotations

from concurrent.futures import Executor, Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

import pytest

from sample_activities import add_one_in_subprocess, raise_unpickleable_error
from temporal_light.worker.context import WorkflowContext, _current_workflow_context
from temporal_light.worker.executor import (
    ActivitySubprocessError,
    _execute_activity_in_subprocess,
    _run_activity_body,
)


class MarkableBrokenExecutor(Executor):
    def __init__(self) -> None:
        self.marked_broken = False

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Future[Any]:
        future: Future[Any] = Future()
        future.set_exception(BrokenProcessPool('pool died'))
        return future

    def mark_broken(self) -> None:
        self.marked_broken = True


@pytest.mark.asyncio
async def test_activity_body_marks_executor_broken_after_broken_process_pool() -> None:
    activity_executor = MarkableBrokenExecutor()

    with pytest.raises(BrokenProcessPool):
        await _run_activity_body(
            activity_executor=activity_executor,
            activity_function=add_one_in_subprocess,
            positional_arguments=(41,),
            keyword_arguments={},
            timeout_seconds=5,
        )

    assert activity_executor.marked_broken is True


@pytest.mark.asyncio
async def test_activity_body_wraps_unpickleable_child_exception() -> None:
    with ProcessPoolExecutor(max_workers=1) as activity_executor:
        with pytest.raises(ActivitySubprocessError) as error_info:
            await _run_activity_body(
                activity_executor=activity_executor,
                activity_function=raise_unpickleable_error,
                positional_arguments=(),
                keyword_arguments={},
                timeout_seconds=5,
            )

        assert 'UnpickleableActivityError' in str(error_info.value)
        assert activity_executor.submit(sum, [1, 2]).result(timeout=5) == 3


def test_subprocess_entrypoint_bypasses_activity_wrapper_when_context_is_inherited() -> None:
    context_token = _current_workflow_context.set(WorkflowContext(workflow_id='wf-1', event_history=[]))
    try:
        result, _ = _execute_activity_in_subprocess(
            'sample_activities',
            'add_one_in_subprocess',
            (41,),
            {},
        )
    finally:
        _current_workflow_context.reset(context_token)

    assert result['result'] == 42
