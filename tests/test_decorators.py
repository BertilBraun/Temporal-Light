"""Unit tests for @workflow and @activity decorators — no database required."""

from __future__ import annotations

import pytest

from temporal_light.decorators import activity, workflow
from temporal_light.models import ActivityPolicy


# ---------------------------------------------------------------------------
# @workflow
# ---------------------------------------------------------------------------


def test_workflow_preserves_function_name() -> None:
    @workflow
    async def my_workflow() -> None:
        pass

    assert my_workflow.__name__ == 'my_workflow'


def test_workflow_sets_is_workflow_flag() -> None:
    @workflow
    async def my_workflow() -> None:
        pass

    assert my_workflow.__is_workflow__ is True


# ---------------------------------------------------------------------------
# @activity — policy storage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'retries, timeout, backoff_seconds',
    [
        (0, 60.0, 5.0),
        (3, 30.0, 2.0),
        (10, 600.0, 60.0),
    ],
)
def test_activity_stores_policy_on_wrapper(retries: int, timeout: float, backoff_seconds: float) -> None:
    @activity(retries=retries, timeout=timeout, backoff_seconds=backoff_seconds)
    async def my_activity() -> None:
        pass

    policy: ActivityPolicy = my_activity.__activity_policy__
    assert policy.max_retries == retries
    assert policy.timeout_seconds == timeout
    assert policy.backoff_seconds == backoff_seconds


def test_activity_preserves_function_name() -> None:
    @activity(retries=1, timeout=10)
    async def process_payment() -> None:
        pass

    assert process_payment.__name__ == 'process_payment'
    assert process_payment.__qualname__.endswith('process_payment')


def test_activity_sets_is_activity_flag() -> None:
    @activity()
    async def my_activity() -> None:
        pass

    assert my_activity.__is_activity__ is True


# ---------------------------------------------------------------------------
# @activity — outside workflow context calls function directly
# ---------------------------------------------------------------------------


async def test_activity_outside_workflow_context_calls_function_directly() -> None:
    call_log: list[tuple] = []

    @activity(retries=2, timeout=10)
    async def tracked_activity(value: int, label: str) -> str:
        call_log.append((value, label))
        return f'{label}-{value}'

    result = await tracked_activity(42, 'test')
    assert result == 'test-42'
    assert call_log == [(42, 'test')]


async def test_activity_outside_workflow_context_propagates_exceptions() -> None:
    @activity(retries=3, timeout=10)
    async def failing_activity() -> None:
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        await failing_activity()


async def test_activity_outside_workflow_context_receives_keyword_arguments() -> None:
    received: dict = {}

    @activity()
    async def kwarg_activity(*, name: str, count: int) -> None:
        received['name'] = name
        received['count'] = count

    await kwarg_activity(name='hello', count=3)
    assert received == {'name': 'hello', 'count': 3}
