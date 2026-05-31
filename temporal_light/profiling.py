"""Opt-in wall-time accounting, split into named buckets.

Enabled only when TEMPORAL_LIGHT_PROFILE=1, so the hot path pays nothing in
production: timed() is a bare yield and the decorators/wrappers that consult
enabled() skip their work entirely.

DB time is attributed to an actor (engine / claim / heartbeat / driver) via a
context variable so a single-process profiling harness can separate the worker's
own database work from the test driver's bookkeeping.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections import defaultdict
from collections.abc import Iterator
from contextvars import ContextVar

_ENABLED = os.environ.get('TEMPORAL_LIGHT_PROFILE') == '1'
_totals: dict[str, float] = defaultdict(float)
_counts: dict[str, int] = defaultdict(int)
_db_actor: ContextVar[str] = ContextVar('temporal_light_db_actor', default='engine')


def enabled() -> bool:
    return _ENABLED


@contextlib.contextmanager
def timed(bucket: str) -> Iterator[None]:
    if not _ENABLED:
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        record(bucket, time.perf_counter() - start)


def record(bucket: str, value: float, count: int = 1) -> None:
    if not _ENABLED:
        return
    _totals[bucket] += value
    _counts[bucket] += count


@contextlib.contextmanager
def db_actor(actor: str) -> Iterator[None]:
    if not _ENABLED:
        yield
        return
    token = _db_actor.set(actor)
    try:
        yield
    finally:
        _db_actor.reset(token)


def current_db_bucket() -> str:
    actor = _db_actor.get()
    return 'db' if actor == 'engine' else f'db.{actor}'


def snapshot() -> dict[str, tuple[float, int]]:
    return {bucket: (_totals[bucket], _counts[bucket]) for bucket in sorted(_totals)}


def reset() -> None:
    _totals.clear()
    _counts.clear()
