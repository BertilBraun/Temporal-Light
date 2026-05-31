"""Runtime timing knobs, read from the environment at call time.

Defaults match production. They are deliberately not tuned below ~10s for the
lock: a lock shorter than the realistic stall from normal computational overhead
would let a healthy worker's lock lapse, producing false reclaim/duplication.
"""

from __future__ import annotations

import os

_DEFAULT_LOCK_DURATION_SECONDS = 30
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10


def lock_duration_seconds() -> int:
    return int(os.environ.get('TEMPORAL_LIGHT_LOCK_DURATION_SECONDS', _DEFAULT_LOCK_DURATION_SECONDS))


def heartbeat_interval_seconds() -> int:
    return int(os.environ.get('TEMPORAL_LIGHT_HEARTBEAT_INTERVAL_SECONDS', _DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
