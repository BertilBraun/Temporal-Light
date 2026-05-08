# Future Work

Items below are non-trivial engineering investments beyond the current MVP. They are ordered roughly by implementation dependency, not priority.

---

## Workflow versioning

Deploy new code without killing in-flight workflows. At a branch point the workflow calls a versioning API that records a version marker in the event log. On replay, the engine reads the marker and routes old instances down the old code path and new instances down the new one. Requires version marker events in the schema and threading version state through `WorkflowContext`.

## Workflow cancellation

Graceful cancellation, not row deletion. A `CancellationError` is delivered into the workflow coroutine at its next `await` point, giving it a chance to run compensation logic (refund, rollback, notify). Workflows suspended on `sleep()` or `wait_for_signal()` must be woken specifically to receive it. Requires a cancellation flag in the `workflows` table and handling in the executor and suspension paths.

## Activity-level heartbeating

The current heartbeat is workflow-scoped (the lock). A long-running activity holds the workflow lock for its full duration — if it hangs silently, the lock expires and a second worker picks up the workflow and runs the activity concurrently.

The fix is transparent: the executor launches a background thread that periodically writes an activity-level heartbeat to the DB for the duration of the activity call. When the activity finishes (success, failure, or exception), the thread is stopped. The activity author sees nothing. Requires a per-step deadline column, a heartbeat thread in the executor, and an expiry check in the scheduler that distinguishes "workflow lock expired" from "activity heartbeat expired".

## Child workflows

Spawn a sub-workflow from within a running workflow. Unlocks fan-out patterns, sagas, and pipeline orchestration. Requires the engine to track parent/child relationships in the schema, propagate cancellation from parent to children, and handle replay correctly when the parent re-runs but the child already completed (treat the child's final result as a cached event, same as an activity).

## Horizontal scale profiling

Characterise where the `FOR UPDATE SKIP LOCKED` claim queue breaks down under load: measure claim latency, queue depth, and Postgres lock contention as concurrent workflow count and worker count scale up. Identify the practical ceiling, document it, and decide whether partition-based claiming or an external dispatch layer (Redis streams, NATS) is warranted.

## Observability

Emit structured JSON log events at every significant transition (workflow claimed, activity scheduled, activity completed, activity failed, workflow completed/failed) with consistent fields: `workflow_id`, `worker_id`, `step_name`, `event_type`, `duration_ms`. The operator's existing monitoring stack (Datadog, Loki, CloudWatch, etc.) can then parse, aggregate, and alert without the engine prescribing a specific tool. Key signals to ensure are always present:

- Queue depth (unclaimed runnable workflows)
- Claim latency (time from `run_at` to claim)
- Activity duration by name
- Retry and failure rates by activity name

## Test SDK

A test harness that runs workflows without a real database. Key capabilities:

- **Time-skipping**: `sleep(days=30)` completes instantly in tests
- **Activity mocking**: substitute a fake implementation for a specific activity
- **Failure injection**: force an activity to fail on attempt N
- **Event sequence assertions**: assert the exact order of SCHEDULED / COMPLETED / FAILED events

This is the highest-leverage adoption driver — teams need to be able to test their workflows locally without Docker.