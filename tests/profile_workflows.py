"""Deterministic, fast workflows for the profiling harness.

Module-level so the activity process pool can import them. The activity body is
intentionally trivial: with cheap user code, the report makes the engine's
fixed per-step cost (DB round-trips, replay, serialization) stand out clearly.
"""

from __future__ import annotations

from temporal_light import activity, spawn_child, wait_for_child, wait_for_signal, workflow

RELEASE_SIGNAL = 'release'


@activity(retries=0, timeout=30)
async def increment(value: int) -> int:
    return value + 1


@workflow
async def profile_child(value: int) -> dict[str, int]:
    return {'value': await increment(value)}


@workflow
async def profile_parent(seed: int, num_children: int, signal_value: int) -> dict[str, int]:
    child_ids: list[str] = []
    for child_index in range(num_children):
        child_ids.append(await spawn_child('profile_child', value=seed + child_index))

    signal_payload = await wait_for_signal(RELEASE_SIGNAL)
    total = signal_payload['value']
    for child_id in child_ids:
        total += (await wait_for_child(child_id))['value']
    return {'total': total}


def expected_total(seed: int, num_children: int, signal_value: int) -> int:
    return signal_value + sum((seed + child_index) + 1 for child_index in range(num_children))
