from __future__ import annotations

import os
from concurrent.futures.process import BrokenProcessPool

import pytest

from temporal_light.worker.activity_pool import RecoveringProcessPoolExecutor


def _exit_abruptly() -> None:
    os._exit(3)


def _add(left: int, right: int) -> int:
    return left + right


def test_recovering_process_pool_replaces_broken_pool() -> None:
    activity_executor = RecoveringProcessPoolExecutor(max_workers=1)
    try:
        with pytest.raises(BrokenProcessPool):
            activity_executor.submit(_exit_abruptly).result(timeout=10)

        activity_executor.mark_broken()

        assert activity_executor.submit(_add, 2, 3).result(timeout=10) == 5
    finally:
        activity_executor.shutdown(wait=False, cancel_futures=True)
