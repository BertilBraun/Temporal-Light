from __future__ import annotations

from concurrent.futures import Executor
from contextvars import ContextVar
from dataclasses import dataclass, field

from typing import Any

from ..models import CHILD_COMPLETED_SIGNAL_TYPE, EventRecord, EventType


@dataclass
class WorkflowContext:
    """Holds all in-memory state for one workflow execution.

    Loaded once from the DB before the workflow coroutine starts and shared
    via _current_workflow_context so decorators and sleep() can read it without
    an explicit parameter.

    step_counter is the only mutable field: it increments each time an activity
    or suspension point is encountered, giving every step a deterministic index
    that matches the event log.
    """

    workflow_id: str
    event_history: list[EventRecord]
    parent_info: dict[str, Any] | None = None
    step_counter: int = field(default=0)
    activity_executor: Executor | None = None

    def next_step_index(self) -> int:
        index = self.step_counter
        self.step_counter += 1
        return index

    def find_event(self, step_index: int, event_type: EventType) -> EventRecord | None:
        for event in self.event_history:
            if event.step_index == step_index and event.event_type == event_type:
                return event
        return None

    def find_completed_event(self, step_index: int) -> EventRecord | None:
        return self.find_event(step_index, EventType.COMPLETED)

    def find_scheduled_event(self, step_index: int) -> EventRecord | None:
        return self.find_event(step_index, EventType.SCHEDULED)

    def find_child_started_event(self, step_index: int) -> EventRecord | None:
        return self.find_event(step_index, EventType.CHILD_STARTED)

    def find_child_result_signal(self, child_id: str) -> EventRecord | None:
        for event in self.event_history:
            if event.step_index != -1 or event.event_type != EventType.SIGNAL or event.payload is None:
                continue
            if event.payload.get('signal_type') != CHILD_COMPLETED_SIGNAL_TYPE:
                continue
            signal_payload = event.payload.get('payload')
            if isinstance(signal_payload, dict) and signal_payload.get('child_id') == child_id:
                return event
        return None

    def find_signal_event(self, signal_type: str) -> EventRecord | None:
        """Return the first received signal event matching signal_type, or None."""
        for event in self.event_history:
            if (
                event.step_index == -1
                and event.event_type == EventType.SIGNAL
                and event.payload is not None
                and event.payload.get('signal_type') == signal_type
                and event.payload.get('status') != 'waiting'
            ):
                return event
        return None

    def find_waiting_for_signal_event(self, signal_type: str) -> EventRecord | None:
        """Return the waiting marker event for signal_type if one was already written."""
        for event in self.event_history:
            if (
                event.step_index == -1
                and event.event_type == EventType.SIGNAL
                and event.payload is not None
                and event.payload.get('signal_type') == signal_type
                and event.payload.get('status') == 'waiting'
            ):
                return event
        return None

    def count_failed_events(self, step_index: int) -> int:
        return sum(
            1 for event in self.event_history if event.step_index == step_index and event.event_type == EventType.FAILED
        )


_current_workflow_context: ContextVar[WorkflowContext] = ContextVar('current_workflow_context')
