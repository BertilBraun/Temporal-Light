"""Module-level activities for tests that dispatch to a real process pool.

Process-pool dispatch pickles the activity by module + qualname, so the body
must live at module scope (functions defined inside a test are not importable
by the subprocess).
"""

from __future__ import annotations

import os

from temporal_light.decorators import activity


@activity(retries=0, timeout=30)
async def add_one_in_subprocess(value: int) -> dict[str, int]:
    return {'result': value + 1, 'pid': os.getpid()}
