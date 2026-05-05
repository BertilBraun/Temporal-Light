from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from ..models import EventRecord, EventType, WorkflowRecord, WorkflowStatus
from . import connection


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


async def register_worker(worker_identifier: str) -> None:
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO workers (worker_id, last_seen)
            VALUES ($1, NOW())
            ON CONFLICT (worker_id) DO UPDATE SET last_seen = NOW()
            """,
            worker_identifier,
        )


async def update_worker_heartbeat(worker_identifier: str) -> None:
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workers SET last_seen = NOW() WHERE worker_id = $1",
            worker_identifier,
        )


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


async def create_workflow(
    workflow_id: str,
    workflow_name: str,
    workflow_input: dict[str, Any],
) -> None:
    """Insert a new workflow row and its STARTED event atomically."""
    pool = await connection.get_connection_pool()
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO workflows
                    (workflow_id, name, status, run_at, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $4, $4)
                """,
                workflow_id,
                workflow_name,
                WorkflowStatus.RUNNING.value,
                now,
            )
            await conn.execute(
                """
                INSERT INTO events
                    (workflow_id, step_index, step_name, event_type, payload, timestamp)
                VALUES ($1, -1, 'workflow', $2, $3::jsonb, $4)
                """,
                workflow_id,
                EventType.STARTED.value,
                json.dumps({"input": workflow_input}),
                now,
            )


async def get_workflow(workflow_id: str) -> WorkflowRecord | None:
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM workflows WHERE workflow_id = $1",
            workflow_id,
        )
    if row is None:
        return None
    return _row_to_workflow_record(row)


async def claim_next_workflow(worker_identifier: str) -> WorkflowRecord | None:
    """Atomically claim the oldest eligible workflow for this worker.

    Uses FOR UPDATE SKIP LOCKED so multiple workers never claim the same row.
    Returns None if no claimable workflow exists.
    """
    pool = await connection.get_connection_pool()
    lock_duration_seconds = 30
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT *
                FROM workflows
                WHERE run_at <= NOW()
                  AND status = 'running'
                  AND (locked_by IS NULL OR locked_until < NOW())
                ORDER BY run_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            if row is None:
                return None

            locked_until = datetime.now(timezone.utc) + timedelta(
                seconds=lock_duration_seconds
            )
            await conn.execute(
                """
                UPDATE workflows
                SET locked_by = $1, locked_until = $2, updated_at = NOW()
                WHERE workflow_id = $3
                """,
                worker_identifier,
                locked_until,
                row["workflow_id"],
            )

            return WorkflowRecord(
                workflow_id=row["workflow_id"],
                name=row["name"],
                status=WorkflowStatus(row["status"]),
                run_at=row["run_at"],
                locked_by=worker_identifier,
                locked_until=locked_until,
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )


async def update_workflow_status(
    workflow_id: str,
    status: WorkflowStatus,
) -> None:
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflows SET status = $1, updated_at = NOW() WHERE workflow_id = $2",
            status.value,
            workflow_id,
        )


async def update_workflow_run_at(workflow_id: str, run_at: datetime) -> None:
    """Reschedule a workflow to be picked up at a future time (retry backoff, sleep)."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflows SET run_at = $1, updated_at = NOW() WHERE workflow_id = $2",
            run_at,
            workflow_id,
        )


async def release_workflow_lock(workflow_id: str) -> None:
    """Clear the lock columns so another worker (or the same one) can claim this workflow."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE workflows
            SET locked_by = NULL, locked_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1
            """,
            workflow_id,
        )


async def extend_workflow_lock(workflow_id: str) -> None:
    """Push locked_until forward by 30 seconds (heartbeat)."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE workflows
            SET locked_until = NOW() + INTERVAL '30 seconds', updated_at = NOW()
            WHERE workflow_id = $1
            """,
            workflow_id,
        )


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


async def load_event_history(workflow_id: str) -> list[EventRecord]:
    """Return all events for a workflow ordered by insertion id (causal order)."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE workflow_id = $1 ORDER BY id",
            workflow_id,
        )
    return [_row_to_event_record(row) for row in rows]


async def load_events_after(workflow_id: str, after_event_id: int) -> list[EventRecord]:
    """Return events inserted after after_event_id (used for SSE tailing)."""
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE workflow_id = $1 AND id > $2 ORDER BY id",
            workflow_id,
            after_event_id,
        )
    return [_row_to_event_record(row) for row in rows]


async def write_event(
    workflow_id: str,
    step_index: int,
    step_name: str,
    event_type: EventType,
    payload: dict[str, Any],
) -> None:
    """Append a single event to the log and notify the API's SSE listeners.

    payload must be JSON-serializable — validated implicitly by json.dumps.
    """
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            """,
            workflow_id,
            step_index,
            step_name,
            event_type.value,
            json.dumps(payload),
        )
        await conn.execute(
            "SELECT pg_notify('workflow_events', $1)",
            workflow_id,
        )


async def write_signal_and_wake_workflow(
    workflow_id: str,
    signal_type: str,
    signal_payload: Any,
) -> None:
    """Write a signal event and immediately make the workflow claimable.

    Atomic: the signal event and the run_at reset happen in one transaction
    so the scheduler cannot claim the workflow before the signal is visible
    in the event log.
    """
    pool = await connection.get_connection_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO events
                    (workflow_id, step_index, step_name, event_type, payload, timestamp)
                VALUES ($1, -1, 'signal', $2, $3::jsonb, NOW())
                """,
                workflow_id,
                EventType.SIGNAL.value,
                json.dumps({"signal_type": signal_type, "payload": signal_payload}),
            )
            await conn.execute(
                "UPDATE workflows SET run_at = NOW(), updated_at = NOW() WHERE workflow_id = $1",
                workflow_id,
            )
            await conn.execute(
                "SELECT pg_notify('workflow_events', $1)",
                workflow_id,
            )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _row_to_workflow_record(row: asyncpg.Record) -> WorkflowRecord:
    return WorkflowRecord(
        workflow_id=row["workflow_id"],
        name=row["name"],
        status=WorkflowStatus(row["status"]),
        run_at=row["run_at"],
        locked_by=row["locked_by"],
        locked_until=row["locked_until"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_event_record(row: asyncpg.Record) -> EventRecord:
    return EventRecord(
        event_id=row["id"],
        workflow_id=row["workflow_id"],
        step_index=row["step_index"],
        step_name=row["step_name"],
        event_type=EventType(row["event_type"]),
        payload=row["payload"],
        timestamp=row["timestamp"],
    )
