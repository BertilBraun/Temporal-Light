"""End-to-end stress test driven entirely through the Client and HTTP API.

Requires TEST_DATABASE_URL and RUN_STRESS_TEST=1. Unlike test_stress.py (which
talks to the database directly), this starts a real uvicorn API process and uses
the Client to start workflows, send signals, and stream results back over SSE —
exercising the full system including the API's LISTEN/NOTIFY → SSE path. A worker
is killed mid-run to confirm crash recovery survives the round trip.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

import db_helpers
import stress_workflows
from stress_workflows import compute_expected_total, iter_parent_specs, run_worker
from temporal_light import Client, WorkflowHandle

pytestmark = [pytest.mark.integration, pytest.mark.stress]

_LOCK_SECONDS = 10
_HEARTBEAT_SECONDS = 3
_RESULT_TIMEOUT_SECONDS = 150


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def _spawn_worker(spawn_context: multiprocessing.context.BaseContext, database_url: str) -> multiprocessing.Process:
    worker = spawn_context.Process(
        target=run_worker,
        args=(database_url, _LOCK_SECONDS, _HEARTBEAT_SECONDS),
        daemon=False,
    )
    worker.start()
    return worker


def _start_api(database_url: str, port: int) -> subprocess.Popen[bytes]:
    environment = {**os.environ, 'DATABASE_URL': database_url}
    return subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'temporal_light.api.server:app', '--host', '127.0.0.1', '--port', str(port)],
        env=environment,
    )


async def _wait_for_api(base_url: str) -> None:
    deadline = time.monotonic() + 30
    async with httpx.AsyncClient() as http_client:
        while time.monotonic() < deadline:
            try:
                response = await http_client.get(f'{base_url}/workflows?limit=1')
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.3)
    raise AssertionError('API did not become ready within 30s')


async def test_stress_end_to_end_through_client(clean_database: None) -> None:
    database_url = os.environ['TEST_DATABASE_URL']
    parent_count = int(os.environ.get('E2E_WORKFLOWS', '80'))
    worker_count = int(os.environ.get('E2E_WORKERS', '3'))

    # Keep the long paths short enough for a snappy end-to-end run; workers inherit
    # these via spawn. Slow activity still exceeds the lock so the heartbeat matters.
    os.environ.setdefault('STRESS_LONG_SLEEP_SECONDS', '5')
    os.environ.setdefault('STRESS_SLOW_ACTIVITY_SECONDS', '12')

    parent_specs = list(iter_parent_specs(parent_count))
    expected_child_count = sum(spec.num_children for spec in parent_specs)

    port = _free_port()
    base_url = f'http://127.0.0.1:{port}'
    spawn_context = multiprocessing.get_context('spawn')

    api_process = _start_api(database_url, port)
    workers = [_spawn_worker(spawn_context, database_url) for _ in range(worker_count)]
    client = Client(base_url)
    request_semaphore = asyncio.Semaphore(20)
    result_semaphore = asyncio.Semaphore(50)

    async def start_parent(spec: stress_workflows.ParentSpec) -> WorkflowHandle:
        async with request_semaphore:
            return await client.start(
                'stress_parent',
                seed=spec.seed,
                num_children=spec.num_children,
                signal_value=spec.signal_value,
                shape=spec.shape,
            )

    async def collect_result(handle: WorkflowHandle, spec: stress_workflows.ParentSpec) -> None:
        async with result_semaphore:
            result = await handle.result(timeout=_RESULT_TIMEOUT_SECONDS)
        assert result['total'] == compute_expected_total(spec), f'wrong result for {handle.workflow_id}'

    async def kill_and_replace_a_worker() -> None:
        await asyncio.sleep(8)
        workers[0].terminate()
        workers[0].join(timeout=10)
        workers[0] = _spawn_worker(spawn_context, database_url)

    try:
        await _wait_for_api(base_url)

        handles = await asyncio.gather(*(start_parent(spec) for spec in parent_specs))
        await asyncio.gather(
            *(
                client.signal(handle.workflow_id, stress_workflows.RELEASE_SIGNAL, {'value': spec.signal_value})
                for handle, spec in zip(handles, parent_specs)
            )
        )

        chaos_task = asyncio.create_task(kill_and_replace_a_worker())
        await asyncio.gather(*(collect_result(handle, spec) for handle, spec in zip(handles, parent_specs)))
        await chaos_task
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.join(timeout=10)
        api_process.terminate()
        try:
            api_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            api_process.kill()

    double_executed = await db_helpers.steps_completed_more_than_once()
    assert not double_executed, f'steps executed more than once: {double_executed}'

    actual_child_count = await db_helpers.count_workflows_by_name('stress_child')
    assert actual_child_count == expected_child_count, (
        f'expected {expected_child_count} children, found {actual_child_count}'
    )

    non_completed = await db_helpers.workflows_not_completed()
    assert not non_completed, f'workflows not completed: {non_completed}'
