"""SSE streaming: LISTEN/NOTIFY dispatcher and per-stream event generator."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from ..models import EventRecord, EventType

logger = logging.getLogger(__name__)

_TERMINAL_EVENT_TYPES = frozenset(
    {EventType.WORKFLOW_COMPLETED, EventType.WORKFLOW_FAILED}
)

# One shared LISTEN connection for the entire API process.
_notify_connection: asyncpg.Connection | None = None

# workflow_id → list of queues, one per active SSE subscriber.
_subscriber_queues: dict[str, list[asyncio.Queue[None]]] = defaultdict(list)


async def initialize_notify_listener(database_url: str) -> None:
    global _notify_connection
    _notify_connection = await asyncpg.connect(database_url)
    await _notify_connection.add_listener("workflow_events", _on_notify)


async def close_notify_listener() -> None:
    global _notify_connection
    if _notify_connection is not None:
        await _notify_connection.close()
        _notify_connection = None


def _on_notify(
    connection: asyncpg.Connection,
    pid: int,
    channel: str,
    workflow_id: str,
) -> None:
    queues = _subscriber_queues.get(workflow_id)
    if queues:
        for queue in queues:
            queue.put_nowait(None)


async def stream_workflow_events(
    workflow_id: str,
    existing_events: list[EventRecord],
) -> AsyncIterator[str]:
    """Yield SSE-formatted lines for a workflow stream.

    Replays existing_events first (already fetched by the endpoint before
    opening this generator), then tails live via NOTIFY until a terminal event
    is seen.
    """
    last_event_id = 0

    for event in existing_events:
        yield _format_sse(event)
        last_event_id = event.event_id
        if event.event_type in _TERMINAL_EVENT_TYPES:
            return

    notification_queue: asyncio.Queue[None] = asyncio.Queue()
    _subscriber_queues[workflow_id].append(notification_queue)

    try:
        while True:
            await notification_queue.get()

            from ..db import queries

            new_events = await queries.load_events_after(workflow_id, last_event_id)
            for event in new_events:
                yield _format_sse(event)
                last_event_id = event.event_id
                if event.event_type in _TERMINAL_EVENT_TYPES:
                    return
    finally:
        queues = _subscriber_queues.get(workflow_id, [])
        try:
            queues.remove(notification_queue)
        except ValueError:
            pass
        if not queues:
            _subscriber_queues.pop(workflow_id, None)


def _format_sse(event: EventRecord) -> str:
    data = _event_to_sse_payload(event)
    return f"data: {json.dumps(data)}\n\n"


def _event_to_sse_payload(event: EventRecord) -> dict[str, Any]:
    base: dict[str, Any] = {
        "type": event.event_type.value,
        "step_index": event.step_index,
        "step_name": event.step_name,
        "timestamp": event.timestamp.isoformat(),
    }
    if event.payload:
        base.update(event.payload)
    return base
