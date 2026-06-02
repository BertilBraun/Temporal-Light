from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any


class RecoveringProcessPoolExecutor:
    def __init__(self, max_workers: int) -> None:
        self._max_workers = max_workers
        self._lock = threading.Lock()
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        with self._lock:
            try:
                return self._executor.submit(fn, *args, **kwargs)
            except BrokenProcessPool:
                self._replace_pool()
                return self._executor.submit(fn, *args, **kwargs)

    def mark_broken(self) -> None:
        with self._lock:
            self._replace_pool()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def _replace_pool(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._executor = ProcessPoolExecutor(max_workers=self._max_workers)
