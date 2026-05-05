from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field

from ..models import EventRecord, EventType


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
    step_counter: int = field(default=0)

    def next_step_index(self) -> int:
        index = self.step_counter
        self.step_counter += 1
        return index

    def find_completed_event(self, step_index: int) -> EventRecord | None:
        for event in self.event_history:
            if (
                event.step_index == step_index
                and event.event_type == EventType.COMPLETED
            ):
                return event
        return None

    def find_scheduled_event(self, step_index: int) -> EventRecord | None:
        for event in self.event_history:
            if (
                event.step_index == step_index
                and event.event_type == EventType.SCHEDULED
            ):
                return event
        return None

    def count_failed_events(self, step_index: int) -> int:
        return sum(
            1
            for event in self.event_history
            if event.step_index == step_index and event.event_type == EventType.FAILED
        )


_current_workflow_context: ContextVar[WorkflowContext] = ContextVar(
    "current_workflow_context"
)
