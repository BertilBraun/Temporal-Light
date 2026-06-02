"""Worker: entry point that ties the connection pool, runner, and scheduler together."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from ..db import connection
from .activity_pool import RecoveringProcessPoolExecutor
from .runner import WorkflowRunner
from .scheduler import run_scheduler_loop

logger = logging.getLogger(__name__)


class Worker:
    """Connects to Postgres, discovers claimable workflows, and executes them.

    Usage::

        Worker(
            workflows=[order_flow],
            activities=[process_payment, send_receipt],
            database_url=os.environ['DATABASE_URL'],
        ).run()

    workflow_functions: list of functions decorated with @workflow.
    activity_functions: list of functions decorated with @activity.
        Activities do not need to be registered to execute — they are invoked
        directly by workflow code via the @activity decorator. The list here
        is used for validation only (Phase 1: stored for future validation).
    database_url: asyncpg-compatible connection string.
    worker_concurrency: maximum number of workflows to run simultaneously.
    """

    def __init__(
        self,
        workflow_functions: list[Callable[..., Coroutine[Any, Any, Any]]],
        activity_functions: list[Callable[..., Coroutine[Any, Any, Any]]],
        database_url: str,
        worker_concurrency: int = 4,
        activity_pool_size: int | None = None,
    ) -> None:
        self.workflow_registry: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {
            func.__qualname__: func for func in workflow_functions
        }
        self.activity_registry: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {
            func.__qualname__: func for func in activity_functions
        }
        self.database_url = database_url
        self.worker_concurrency = worker_concurrency
        self.activity_pool_size = activity_pool_size or worker_concurrency

    def run(self) -> None:
        """Block the calling thread, running the asyncio event loop until interrupted."""
        try:
            asyncio.run(self._run_async())
        except KeyboardInterrupt:
            logger.info('Worker shutting down.')

    async def _run_async(self) -> None:
        await connection.initialize_connection_pool(self.database_url)
        activity_executor = RecoveringProcessPoolExecutor(max_workers=self.activity_pool_size)
        try:
            worker_identifier = str(uuid.uuid4())
            logger.info('Worker starting. id=%s', worker_identifier)
            runner = WorkflowRunner(self.workflow_registry, activity_executor=activity_executor)
            await run_scheduler_loop(
                workflow_runner=runner,
                worker_identifier=worker_identifier,
                worker_concurrency=self.worker_concurrency,
            )
        finally:
            activity_executor.shutdown(wait=False, cancel_futures=True)
            await connection.close_connection_pool()
