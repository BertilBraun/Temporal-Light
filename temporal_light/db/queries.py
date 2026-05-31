from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from .. import config
from ..models import CHILD_COMPLETED_SIGNAL_TYPE, EventRecord, EventType, WorkflowRecord, WorkflowStatus
from . import connection


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


async def register_worker(worker_identifier: str) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO workers (worker_id, last_seen)
            VALUES ($1, NOW())
            ON CONFLICT (worker_id) DO UPDATE SET last_seen = NOW()
            """,
            worker_identifier,
        )


async def update_worker_heartbeat(worker_identifier: str) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            'UPDATE workers SET last_seen = NOW() WHERE worker_id = $1',
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
    async with connection.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO workflows
                (workflow_id, name, status, run_at, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW(), NOW())
            """,
            workflow_id,
            workflow_name,
            WorkflowStatus.RUNNING.value,
        )
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, -1, 'workflow', $2, $3::jsonb, NOW())
            """,
            workflow_id,
            EventType.STARTED.value,
            {'input': workflow_input},
        )


async def create_child_workflow(
    parent_workflow_id: str,
    child_workflow_id: str,
    child_workflow_name: str,
    child_workflow_input: dict[str, Any],
    parent_step_index: int,
) -> bool:
    """Create a child workflow and record the parent child_started event atomically.

    Idempotent on child_workflow_id: a concurrent or replayed call with the same id
    is a no-op that returns False, so the parent never gets a duplicate child or a
    second child_started event for the same logical step.
    """
    async with connection.transaction() as conn:
        inserted_child_id = await conn.fetchval(
            """
            INSERT INTO workflows
                (workflow_id, name, status, run_at, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW(), NOW())
            ON CONFLICT (workflow_id) DO NOTHING
            RETURNING workflow_id
            """,
            child_workflow_id,
            child_workflow_name,
            WorkflowStatus.RUNNING.value,
        )
        if inserted_child_id is None:
            return False
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, -1, 'workflow', $2, $3::jsonb, NOW())
            """,
            child_workflow_id,
            EventType.STARTED.value,
            {
                'input': child_workflow_input,
                'parent': {
                    'workflow_id': parent_workflow_id,
                    'child_id': child_workflow_id,
                },
            },
        )
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            """,
            parent_workflow_id,
            parent_step_index,
            child_workflow_name,
            EventType.CHILD_STARTED.value,
            {
                'child_id': child_workflow_id,
                'workflow_name': child_workflow_name,
                'input': child_workflow_input,
            },
        )
        await conn.execute(
            "SELECT pg_notify('workflow_events', $1)",
            parent_workflow_id,
        )
    return True


async def get_workflow(workflow_id: str) -> WorkflowRecord | None:
    async with connection.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM workflows WHERE workflow_id = $1',
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
    # The worker row is registered at startup and refreshed by the heartbeat loop,
    # so there is no need to upsert it on every claim.
    lock_duration_seconds = config.lock_duration_seconds()
    async with connection.transaction() as conn:
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

        locked_until = datetime.now(timezone.utc) + timedelta(seconds=lock_duration_seconds)
        await conn.execute(
            """
            UPDATE workflows
            SET locked_by = $1, locked_until = $2, updated_at = NOW()
            WHERE workflow_id = $3
            """,
            worker_identifier,
            locked_until,
            row['workflow_id'],
        )

        return WorkflowRecord(
            workflow_id=row['workflow_id'],
            name=row['name'],
            status=WorkflowStatus(row['status']),
            run_at=row['run_at'],
            locked_by=worker_identifier,
            locked_until=locked_until,
            created_at=row['created_at'],
            updated_at=row['updated_at'],
        )


async def update_workflow_status(
    workflow_id: str,
    status: WorkflowStatus,
) -> None:
    async with connection.acquire() as conn:
        await conn.execute(
            'UPDATE workflows SET status = $1, updated_at = NOW() WHERE workflow_id = $2',
            status.value,
            workflow_id,
        )


async def mark_workflow_waiting_for_child(workflow_id: str, child_id: str) -> bool:
    """Mark parent waiting unless the child completion signal is already durable.

    Returns True when the parent was marked waiting. Returns False when the
    child completion signal already exists, which means the caller must replay
    history and continue instead of suspending.

    The workflows row is locked FOR UPDATE before the events table is read in a
    separate statement, so the existence check sees a fresh post-lock snapshot.
    A single UPDATE with a NOT EXISTS subquery would not: under READ COMMITTED,
    after blocking on the row lock it re-runs the subquery against the original
    statement snapshot and misses a completion the waker just committed, losing
    the wakeup.
    """
    async with connection.transaction() as conn:
        await conn.execute('SELECT 1 FROM workflows WHERE workflow_id = $1 FOR UPDATE', workflow_id)
        completion_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM events
                WHERE workflow_id = $1
                  AND event_type = $2
                  AND payload->>'signal_type' = $3
                  AND payload->'payload'->>'child_id' = $4
            )
            """,
            workflow_id,
            EventType.SIGNAL.value,
            CHILD_COMPLETED_SIGNAL_TYPE,
            child_id,
        )
        if completion_exists:
            return False
        await conn.execute(
            """
            UPDATE workflows
            SET status = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
            WHERE workflow_id = $2
            """,
            WorkflowStatus.WAITING.value,
            workflow_id,
        )
    return True


async def mark_workflow_waiting_for_signal(workflow_id: str, signal_type: str) -> bool:
    """Mark a workflow waiting unless the signal it wants has already been received.

    Returns True when the workflow was marked waiting. Returns False when a non-waiting
    signal of this type already exists, meaning the caller must replay and continue
    instead of suspending — this closes the lost-wakeup race where the signal arrives
    between the caller's history read and its suspension.

    Locks the workflows row FOR UPDATE before reading events in a separate statement;
    see mark_workflow_waiting_for_child for why a NOT EXISTS subquery is not safe here.
    """
    async with connection.transaction() as conn:
        await conn.execute('SELECT 1 FROM workflows WHERE workflow_id = $1 FOR UPDATE', workflow_id)
        signal_received = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM events
                WHERE workflow_id = $1
                  AND event_type = $2
                  AND payload->>'signal_type' = $3
                  AND payload->>'status' IS DISTINCT FROM 'waiting'
            )
            """,
            workflow_id,
            EventType.SIGNAL.value,
            signal_type,
        )
        if signal_received:
            return False
        await conn.execute(
            """
            UPDATE workflows
            SET status = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
            WHERE workflow_id = $2
            """,
            WorkflowStatus.WAITING.value,
            workflow_id,
        )
    return True


async def update_workflow_run_at(workflow_id: str, run_at: datetime) -> None:
    """Reschedule a suspended workflow and release its lock in one statement.

    Both callers (sleep, retry backoff) are suspending, so clearing the lock here
    lets a later wake claim the workflow immediately instead of paying a separate
    release round-trip and waiting out the lock.
    """
    async with connection.acquire() as conn:
        await conn.execute(
            """
            UPDATE workflows
            SET run_at = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
            WHERE workflow_id = $2
            """,
            run_at,
            workflow_id,
        )


async def complete_workflow(
    workflow_id: str,
    result: Any,
    parent_info: dict[str, Any] | None = None,
) -> None:
    """Record workflow completion and wake the parent child join atomically."""
    await _write_terminal_workflow_event(
        workflow_id=workflow_id,
        event_type=EventType.WORKFLOW_COMPLETED,
        payload={'result': result},
        status=WorkflowStatus.COMPLETED,
        parent_info=parent_info,
        child_status='completed',
        child_result=result,
    )


async def fail_workflow(
    workflow_id: str,
    error_message: str,
    parent_info: dict[str, Any] | None = None,
) -> None:
    """Record workflow failure and wake the parent child join atomically."""
    await _write_terminal_workflow_event(
        workflow_id=workflow_id,
        event_type=EventType.WORKFLOW_FAILED,
        payload={'error': error_message},
        status=WorkflowStatus.FAILED,
        parent_info=parent_info,
        child_status='failed',
        child_error=error_message,
    )


async def extend_workflow_lock(workflow_id: str) -> None:
    """Push locked_until forward by the configured lock duration (heartbeat)."""
    async with connection.acquire() as conn:
        await conn.execute(
            """
            UPDATE workflows
            SET locked_until = NOW() + make_interval(secs => $2), updated_at = NOW()
            WHERE workflow_id = $1
            """,
            workflow_id,
            config.lock_duration_seconds(),
        )


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


async def load_event_history(workflow_id: str) -> list[EventRecord]:
    """Return all events for a workflow ordered by insertion id (causal order)."""
    async with connection.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM events WHERE workflow_id = $1 ORDER BY id',
            workflow_id,
        )
    return [_row_to_event_record(row) for row in rows]


async def load_events_after(workflow_id: str, after_event_id: int) -> list[EventRecord]:
    """Return events inserted after after_event_id (used for SSE tailing)."""
    async with connection.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM events WHERE workflow_id = $1 AND id > $2 ORDER BY id',
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
    """Append a single event to the log and notify the API's SSE listeners."""
    async with connection.acquire() as conn:
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
            payload,
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
    async with connection.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, -1, 'signal', $2, $3::jsonb, NOW())
            """,
            workflow_id,
            EventType.SIGNAL.value,
            {'signal_type': signal_type, 'payload': signal_payload},
        )
        await conn.execute(
            """
            UPDATE workflows
            SET run_at = NOW(), status = 'running', updated_at = NOW()
            WHERE workflow_id = $1
            """,
            workflow_id,
        )
        await conn.execute(
            "SELECT pg_notify('workflow_events', $1)",
            workflow_id,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _write_terminal_workflow_event(
    workflow_id: str,
    event_type: EventType,
    payload: dict[str, Any],
    status: WorkflowStatus,
    parent_info: dict[str, Any] | None,
    child_status: str,
    child_result: Any = None,
    child_error: str | None = None,
) -> None:
    async with connection.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events
                (workflow_id, step_index, step_name, event_type, payload, timestamp)
            VALUES ($1, -1, 'workflow', $2, $3::jsonb, NOW())
            """,
            workflow_id,
            event_type.value,
            payload,
        )
        await conn.execute(
            """
            UPDATE workflows
            SET status = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
            WHERE workflow_id = $2
            """,
            status.value,
            workflow_id,
        )

        if parent_info is not None:
            child_signal_payload: dict[str, Any] = {
                'child_id': parent_info['child_id'],
                'status': child_status,
            }
            if child_status == 'completed':
                child_signal_payload['result'] = child_result
            else:
                child_signal_payload['error'] = child_error

            await conn.execute(
                """
                INSERT INTO events
                    (workflow_id, step_index, step_name, event_type, payload, timestamp)
                VALUES ($1, -1, 'signal', $2, $3::jsonb, NOW())
                """,
                parent_info['workflow_id'],
                EventType.SIGNAL.value,
                {
                    'signal_type': CHILD_COMPLETED_SIGNAL_TYPE,
                    'payload': child_signal_payload,
                },
            )
            await conn.execute(
                """
                UPDATE workflows
                SET run_at = NOW(), status = 'running', updated_at = NOW()
                WHERE workflow_id = $1
                """,
                parent_info['workflow_id'],
            )
            await conn.execute(
                "SELECT pg_notify('workflow_events', $1)",
                parent_info['workflow_id'],
            )

        await conn.execute(
            "SELECT pg_notify('workflow_events', $1)",
            workflow_id,
        )


def _row_to_workflow_record(row: asyncpg.Record) -> WorkflowRecord:
    return WorkflowRecord(
        workflow_id=row['workflow_id'],
        name=row['name'],
        status=WorkflowStatus(row['status']),
        run_at=row['run_at'],
        locked_by=row['locked_by'],
        locked_until=row['locked_until'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
    )


def _row_to_event_record(row: asyncpg.Record) -> EventRecord:
    return EventRecord(
        event_id=row['id'],
        workflow_id=row['workflow_id'],
        step_index=row['step_index'],
        step_name=row['step_name'],
        event_type=EventType(row['event_type']),
        payload=row['payload'],
        timestamp=row['timestamp'],
    )
