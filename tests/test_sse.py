"""Unit tests for SSE formatting — no database required."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from temporal_light.api.sse import _event_to_sse_payload, _format_sse
from temporal_light.models import EventRecord, EventType


def _make_event(
    event_type: EventType,
    step_index: int = 0,
    step_name: str = "some_activity",
    payload: dict | None = None,
) -> EventRecord:
    return EventRecord(
        event_id=1,
        workflow_id="wf-test",
        step_index=step_index,
        step_name=step_name,
        event_type=event_type,
        payload=payload or {},
        timestamp=datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# _format_sse
# ---------------------------------------------------------------------------


def test_format_sse_starts_with_data_prefix() -> None:
    event = _make_event(EventType.SCHEDULED)
    line = _format_sse(event)
    assert line.startswith("data: ")


def test_format_sse_ends_with_double_newline() -> None:
    event = _make_event(EventType.COMPLETED, payload={"result": 42})
    line = _format_sse(event)
    assert line.endswith("\n\n")


def test_format_sse_body_is_valid_json() -> None:
    event = _make_event(EventType.COMPLETED, payload={"result": {"key": "value"}})
    line = _format_sse(event)
    json_part = line[len("data: "):].strip()
    parsed = json.loads(json_part)
    assert isinstance(parsed, dict)


# ---------------------------------------------------------------------------
# _event_to_sse_payload
# ---------------------------------------------------------------------------


def test_sse_payload_includes_event_type() -> None:
    event = _make_event(EventType.SCHEDULED)
    payload = _event_to_sse_payload(event)
    assert payload["type"] == "scheduled"


def test_sse_payload_includes_step_index() -> None:
    event = _make_event(EventType.COMPLETED, step_index=3)
    payload = _event_to_sse_payload(event)
    assert payload["step_index"] == 3


def test_sse_payload_includes_step_name() -> None:
    event = _make_event(EventType.COMPLETED, step_name="charge_payment")
    payload = _event_to_sse_payload(event)
    assert payload["step_name"] == "charge_payment"


def test_sse_payload_includes_timestamp_as_iso_string() -> None:
    event = _make_event(EventType.COMPLETED)
    payload = _event_to_sse_payload(event)
    assert payload["timestamp"] == "2026-05-05T12:00:00+00:00"


def test_sse_payload_merges_event_payload_fields() -> None:
    event = _make_event(
        EventType.COMPLETED,
        payload={"result": {"tx_id": "abc"}, "duration_seconds": 1.5},
    )
    payload = _event_to_sse_payload(event)
    assert payload["result"] == {"tx_id": "abc"}
    assert payload["duration_seconds"] == 1.5


def test_sse_payload_with_empty_event_payload_has_no_extra_keys() -> None:
    event = _make_event(EventType.SCHEDULED, payload={})
    payload = _event_to_sse_payload(event)
    assert set(payload.keys()) == {"type", "step_index", "step_name", "timestamp"}


def test_sse_payload_workflow_completed_uses_correct_type_string() -> None:
    event = _make_event(EventType.WORKFLOW_COMPLETED, step_index=-1, step_name="workflow")
    payload = _event_to_sse_payload(event)
    assert payload["type"] == "workflow_completed"
