"""Profiling harness: where does workflow time go — user code vs engine?

Run with both env vars set, and -s to see the report:

    TEMPORAL_LIGHT_PROFILE=1 TEST_DATABASE_URL=... pytest tests/test_profile.py -s

One in-process worker at concurrency 1 (so timing buckets reflect real serial
time, not overlapping wall time) drives a deterministic workload of children,
signals, and replays, with activities in a real process pool. Skipped unless
profiling is enabled.
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import pytest

import db_helpers
import profile_workflows
from temporal_light import profiling
from temporal_light.db import queries
from temporal_light.models import EventType
from temporal_light.worker.runner import WorkflowRunner
from temporal_light.worker.scheduler import run_scheduler_loop

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class ProfileSpec:
    parent_id: str
    seed: int
    num_children: int
    signal_value: int


def _print_report(snapshot: dict[str, tuple[float, int]], parent_count: int, child_count: int) -> None:
    def seconds(bucket: str) -> float:
        return snapshot.get(bucket, (0.0, 0))[0]

    def count(bucket: str) -> int:
        return snapshot.get(bucket, (0.0, 0))[1]

    run = seconds('run')
    db_in_run = seconds('db')
    serialize = seconds('serialize')
    activity_total = seconds('activity.total')
    activity_user = seconds('activity.user')
    dispatch = activity_total - activity_user
    framework = run - db_in_run - serialize - activity_total
    scheduling = seconds('db.claim') + seconds('db.heartbeat')

    active = run + scheduling
    user = activity_user
    engine = active - user

    runs = count('run')
    workflows = parent_count + child_count
    db_ops = count('db') + count('db.claim') + count('db.heartbeat')

    def line(label: str, value: float, ops: int | None = None) -> str:
        share = (value / active * 100) if active else 0.0
        detail = f'{ops:8d}{value / ops * 1000:9.2f}ms' if ops else ''
        return f'  {label:<30}{value:8.3f}s{share:6.1f}%{detail}'

    print('\n=================== profiling: user vs engine ===================')
    print(f'  workflows={workflows} (parents={parent_count}, children={child_count})')
    if runs:
        print(f'  workflow runs={runs} -> {runs / workflows:.2f} runs/workflow ({runs - workflows} replays)')
        print(f'  avg history rows replayed per run={seconds("history_rows") / runs:.1f}')
        print(f'  db round-trips: {db_ops} total, {db_ops / runs:.1f} per run')
    print(f'  {"":<30}{"total":>8}{"share":>7}{"ops":>8}{"avg":>11}')
    print('  ---------------------------------------------------------------')
    print(line('USER (activity bodies)', user, count('activity.user')))
    print(line('ENGINE', engine))
    print('  ---- engine breakdown ----')
    print(line('db i/o (in-run)', db_in_run, count('db')))
    print(line('serialization', serialize, count('serialize')))
    print(line('activity dispatch (ipc)', dispatch, count('activity.total')))
    print(line('framework / replay cpu', framework))
    print(line('scheduling: claim', seconds('db.claim'), count('db.claim')))
    print(line('scheduling: heartbeat', seconds('db.heartbeat'), count('db.heartbeat')))
    print(f'  total active worker time      {active:8.3f}s  (idle polling excluded)')
    print('=================================================================')


async def test_profile_user_vs_engine_time(clean_database: None) -> None:
    if not profiling.enabled():
        pytest.skip('set TEMPORAL_LIGHT_PROFILE=1 to run the profiling harness')

    parent_count = int(os.environ.get('PROFILE_WORKFLOWS', '40'))
    specs = [
        ProfileSpec(
            parent_id=f'profile-parent-{index}',
            seed=index * 100,
            num_children=1 + (index % 2),
            signal_value=index,
        )
        for index in range(parent_count)
    ]
    child_count = sum(spec.num_children for spec in specs)
    expected_total_workflows = parent_count + child_count

    profiling.reset()
    activity_executor = ProcessPoolExecutor(max_workers=4)
    runner = WorkflowRunner(
        {'profile_parent': profile_workflows.profile_parent, 'profile_child': profile_workflows.profile_child},
        activity_executor=activity_executor,
    )
    concurrency = int(os.environ.get('PROFILE_CONCURRENCY', '1'))
    scheduler_task = asyncio.create_task(run_scheduler_loop(runner, 'profile-worker', worker_concurrency=concurrency))
    try:
        with profiling.db_actor('driver'):
            for spec in specs:
                await queries.create_workflow(
                    spec.parent_id,
                    'profile_parent',
                    {'seed': spec.seed, 'num_children': spec.num_children, 'signal_value': spec.signal_value},
                )
            for spec in specs:
                await queries.write_signal_and_wake_workflow(
                    spec.parent_id, profile_workflows.RELEASE_SIGNAL, {'value': spec.signal_value}
                )
        await db_helpers.wait_for_all_terminal(expected_total_workflows, timeout_seconds=120)
    finally:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        activity_executor.shutdown(wait=False, cancel_futures=True)

    _print_report(profiling.snapshot(), parent_count, child_count)

    for spec in specs:
        history = await queries.load_event_history(spec.parent_id)
        terminal = history[-1]
        assert terminal.event_type == EventType.WORKFLOW_COMPLETED
        assert terminal.payload['result']['total'] == profile_workflows.expected_total(
            spec.seed, spec.num_children, spec.signal_value
        )
