from __future__ import annotations

import asyncio
from typing import Any

import pytest

from temporal_light.client import Client, WorkflowHandle


async def test_result_raises_timeout_error_and_cancels_stream() -> None:
    handle = WorkflowHandle(Client('http://testserver'), workflow_id='wf-timeout')
    stream_started = asyncio.Event()
    stream_cancelled = asyncio.Event()

    async def never_terminal() -> Any:
        stream_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            stream_cancelled.set()
            raise

    handle._stream_until_terminal = never_terminal  # type: ignore[method-assign]

    with pytest.raises(TimeoutError):
        await handle.result(timeout=0.01)

    assert stream_started.is_set()
    assert stream_cancelled.is_set()
