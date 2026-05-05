"""Scheduler loop: claim, execute, heartbeat, repeat."""

from __future__ import annotations

import asyncio
import logging

from ..db import queries
from ..models import WorkflowRecord
from .runner import WorkflowRunner

logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL_SECONDS = 10
_LOCK_EXTENSION_SECONDS = 30
_IDLE_POLL_INTERVAL_SECONDS = 1
_AT_CAPACITY_YIELD_SECONDS = 0.1


async def run_scheduler_loop(
    workflow_runner: WorkflowRunner,
    worker_identifier: str,
    worker_concurrency: int,
) -> None:
    """Main loop: claim workflows and run them as concurrent asyncio Tasks.

    Stops only on cancellation (e.g. KeyboardInterrupt in Worker.run()).
    """
    await queries.register_worker(worker_identifier)

    active_tasks: set[asyncio.Task[None]] = set()
    worker_heartbeat_task = asyncio.create_task(
        _worker_heartbeat_loop(worker_identifier),
        name=f"worker-heartbeat-{worker_identifier}",
    )

    try:
        had_work = False
        while True:
            if not had_work:
                await asyncio.sleep(_IDLE_POLL_INTERVAL_SECONDS)

            # Reap finished tasks.
            active_tasks = {task for task in active_tasks if not task.done()}

            if len(active_tasks) >= worker_concurrency:
                had_work = False
                await asyncio.sleep(_AT_CAPACITY_YIELD_SECONDS)
                continue

            workflow_record = await queries.claim_next_workflow(worker_identifier)
            if workflow_record is None:
                had_work = False
                continue

            had_work = True
            task = asyncio.create_task(
                _run_workflow_with_heartbeat(workflow_runner, workflow_record),
                name=f"workflow-{workflow_record.workflow_id}",
            )
            active_tasks.add(task)

    finally:
        worker_heartbeat_task.cancel()
        try:
            await worker_heartbeat_task
        except asyncio.CancelledError:
            pass

        # Allow in-flight tasks to finish before the process exits.
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)


async def _run_workflow_with_heartbeat(
    workflow_runner: WorkflowRunner,
    workflow_record: WorkflowRecord,
) -> None:
    """Run a workflow alongside a heartbeat task that keeps its lock alive."""
    heartbeat_task = asyncio.create_task(
        _workflow_lock_heartbeat_loop(workflow_record.workflow_id),
        name=f"heartbeat-{workflow_record.workflow_id}",
    )
    try:
        await workflow_runner.run_workflow(workflow_record)
    except Exception:
        logger.exception(
            "Unexpected error running workflow %s", workflow_record.workflow_id
        )
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


async def _workflow_lock_heartbeat_loop(workflow_id: str) -> None:
    """Extend the workflow lock every HEARTBEAT_INTERVAL_SECONDS seconds."""
    while True:
        await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
        try:
            await queries.extend_workflow_lock(workflow_id)
        except Exception:
            logger.exception("Failed to extend lock for workflow %s", workflow_id)


async def _worker_heartbeat_loop(worker_identifier: str) -> None:
    """Keep the worker's last_seen timestamp current in the workers table."""
    while True:
        await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
        try:
            await queries.update_worker_heartbeat(worker_identifier)
        except Exception:
            logger.exception(
                "Failed to update heartbeat for worker %s", worker_identifier
            )
