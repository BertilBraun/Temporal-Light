from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class WorkflowStatus(Enum):
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"


class EventType(Enum):
    STARTED = "started"
    SCHEDULED = "scheduled"
    COMPLETED = "completed"
    FAILED = "failed"
    SLEEP = "sleep"
    SIGNAL = "signal"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"


@dataclass(frozen=True)
class ActivityPolicy:
    max_retries: int
    timeout_seconds: float
    backoff_seconds: float


@dataclass(frozen=True)
class WorkflowRecord:
    workflow_id: str
    name: str
    status: WorkflowStatus
    run_at: datetime
    locked_by: str | None
    locked_until: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class EventRecord:
    """A single event row read from the database.

    payload is Any because it is raw decoded JSON — this is the serialization
    boundary. Callers must access known fields by key and handle missing keys.
    """

    event_id: int
    workflow_id: str
    step_index: int
    step_name: str
    event_type: EventType
    payload: Any
    timestamp: datetime
