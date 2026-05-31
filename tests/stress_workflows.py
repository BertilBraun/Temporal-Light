"""Workflows, activities, and a worker entrypoint for the multi-process stress test.

Everything here is module-level so spawn-based worker subprocesses (and their
activity process pools) can import it by name. Result values are pure functions
of inputs so the driver can verify every workflow's output independently;
compute_expected_total is the single source of truth shared with stress_parent.
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Iterator

from temporal_light import Worker, activity, sleep, spawn_child, wait_for_child, wait_for_signal, workflow

LONG_SLEEP_SECONDS = int(os.environ.get('STRESS_LONG_SLEEP_SECONDS', '60'))
SLOW_ACTIVITY_SECONDS = int(os.environ.get('STRESS_SLOW_ACTIVITY_SECONDS', '15'))
RELEASE_SIGNAL = 'release'

SHAPE_PLAIN = 'plain'
SHAPE_SLEEP = 'sleep'
SHAPE_SLOW = 'slow'


@activity(retries=12, timeout=30, backoff_seconds=0.5)
async def flaky_increment(value: int) -> int:
    if random.random() < 0.25:
        raise RuntimeError('transient failure to exercise retries')
    return value + 1


@activity(retries=0, timeout=120)
async def slow_blocking(value: int) -> int:
    # Blocks its subprocess past the lock duration: the heartbeat must keep the
    # lock alive while this runs, and the step must not be executed twice.
    time.sleep(SLOW_ACTIVITY_SECONDS)
    return value * 2


@workflow
async def stress_child(value: int) -> dict[str, int]:
    incremented = await flaky_increment(value)
    return {'value': incremented}


@workflow
async def stress_parent(seed: int, num_children: int, signal_value: int, shape: str) -> dict[str, int]:
    total = 0
    child_ids: list[str] = []
    for child_index in range(num_children):
        child_ids.append(await spawn_child('stress_child', value=seed + child_index))

    if shape == SHAPE_SLOW:
        total += await slow_blocking(seed)
    if shape == SHAPE_SLEEP:
        await sleep(seconds=LONG_SLEEP_SECONDS)

    signal_payload = await wait_for_signal(RELEASE_SIGNAL)
    total += signal_payload['value']

    for child_id in child_ids:
        child_result = await wait_for_child(child_id)
        total += child_result['value']

    return {'total': total}


@dataclass(frozen=True)
class ParentSpec:
    parent_id: str
    seed: int
    num_children: int
    signal_value: int
    shape: str

    def workflow_input(self) -> dict[str, Any]:
        return {
            'seed': self.seed,
            'num_children': self.num_children,
            'signal_value': self.signal_value,
            'shape': self.shape,
        }


def compute_expected_total(spec: ParentSpec) -> int:
    children_total = sum((spec.seed + child_index) + 1 for child_index in range(spec.num_children))
    total = children_total + spec.signal_value
    if spec.shape == SHAPE_SLOW:
        total += spec.seed * 2
    return total


def iter_parent_specs(count: int) -> Iterator[ParentSpec]:
    for index in range(count):
        bucket = index % 10
        if bucket < 7:
            shape = SHAPE_PLAIN
        elif bucket < 9:
            shape = SHAPE_SLEEP
        else:
            shape = SHAPE_SLOW
        yield ParentSpec(
            parent_id=f'stress-parent-{index}',
            seed=index * 100,
            num_children=1 + (index % 4),
            signal_value=index,
            shape=shape,
        )


def run_worker(database_url: str, lock_seconds: int, heartbeat_seconds: int) -> None:
    """Worker subprocess entrypoint: tighten the lock/heartbeat, then run forever."""
    os.environ['TEMPORAL_LIGHT_LOCK_DURATION_SECONDS'] = str(lock_seconds)
    os.environ['TEMPORAL_LIGHT_HEARTBEAT_INTERVAL_SECONDS'] = str(heartbeat_seconds)
    logging.basicConfig(level=logging.WARNING)
    Worker(
        workflow_functions=[stress_parent, stress_child],
        activity_functions=[flaky_increment, slow_blocking],
        database_url=database_url,
        worker_concurrency=4,
        activity_pool_size=2,
    ).run()
