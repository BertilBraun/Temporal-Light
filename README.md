# Temporal-Light

A lightweight, self-hosted durable workflow engine. Write plain async Python functions, decorate them, and the engine guarantees they run to completion — surviving crashes, worker restarts, and process migrations — by replaying an append-only event log stored in Postgres.

No external coordinator, no proprietary infrastructure. One `docker compose up` and you have a fully functional system.

---

## Dashboard

![Temporal-Light Dashboard](documentation/dashboard.png)

The built-in dashboard (served at `GET /`) shows all workflows with live event streams. Click any workflow to open its SSE feed: each step appears as it completes, with duration, attempt count, and result or error detail.

---

## Architecture

```mermaid
flowchart TB
    PG[(Postgres\nworkflows / events / workers)]

    PG -->|LISTEN/NOTIFY| API
    W -->|claim / write events| PG

    subgraph Workers
        W[Worker\n─────────────\npoll DB\nreplay history\nexecute steps\nwrite events]
    end

    subgraph API Server
        API[API Server\n─────────────\nstart workflow\nsend signal\nSSE stream\ndashboard]
    end

    API -->|SSE / HTTP| SDK[Client SDK\nany language]
```

**Four independent components:**

| Component | Role |
|---|---|
| **Postgres** | Single source of truth. `workflows` table tracks status and scheduling. `events` table is the append-only execution log. |
| **Workers** | Stateless Python processes. Poll Postgres, claim workflows with `FOR UPDATE SKIP LOCKED`, replay history, execute forward, write events. |
| **API** | Thin HTTP bridge (FastAPI). Starts workflows, sends signals, streams events to clients via SSE using Postgres `LISTEN/NOTIFY`. |
| **Client** | Any HTTP client. The Python SDK provides `Client`, `WorkflowHandle`, and `WorkflowFailedError`. |

Workers and the API are the same Docker image with different `command` entries. No separate base image.

---

## Execution Model

### Event log as the only durable state

Every meaningful step writes one or more rows to the `events` table. Coroutine state is **never** persisted — when a workflow resumes, the engine re-creates the coroutine from scratch and replays history:

- Steps with a `completed` event in history return their recorded result instantly.
- The first step with no `completed` event is the frontier — execution continues from there for real.

This means worker handoff and crash recovery are identical operations: load history, replay, continue.

### Activity execution flow

```
await some_activity(arg1, arg2)
  │
  ├─ completed event exists? ──yes──► return recorded result  (replay)
  │
  ├─ scheduled event exists with different name? ──► DivergenceError
  │
  ├─ write scheduled event
  │
  ├─ asyncio.wait_for(fn(), timeout=policy.timeout_seconds)
  │     ├─ success ──► write completed event (with duration + attempt count)
  │     └─ failure ──► write failed event
  │           ├─ attempts < max_retries ──► set run_at = now + backoff, raise _WorkflowSuspended
  │           └─ attempts >= max_retries ──► mark workflow FAILED
```

### Sleep and signals

Both suspend execution by raising an internal `_WorkflowSuspended` exception (caught by the runner, never visible to user code):

- **`sleep()`** — writes a `sleep` event with `wakeup_at`, sets `run_at = wakeup_at`. The scheduler picks the workflow up when `run_at <= now`.
- **`wait_for_signal(signal_type)`** — scans history for a matching `signal` event. If absent, sets `run_at` to year 9999 and suspends. The API's signal endpoint writes the signal event and atomically resets `run_at = NOW()` in the same transaction.

### Child workflows

Child workflows let a parent fan out independent workflow executions and join them later:

- **`spawn_child(workflow_name, **kwargs)`** writes a parent `child_started` event, creates the child workflow, and returns the child workflow id. On replay it reads the existing `child_started` event, so a resumed parent does not create duplicate children.
- **`wait_for_child(child_id)`** scans the parent's event log for the reserved child completion signal. If absent, it marks the parent waiting and suspends. When the child completes or fails, the runner writes that reserved signal to the parent and wakes it atomically.

Child workflow inputs and results must be JSON-serializable, just like workflow starts and activity results.

### Multi-worker coordination

```sql
SELECT * FROM workflows
WHERE run_at <= NOW() AND status = 'running'
  AND (locked_by IS NULL OR locked_until < NOW())
ORDER BY run_at LIMIT 1
FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` ensures multiple workers never claim the same workflow. Lock lease is 30 seconds, extended every 10 seconds via heartbeat. Expired leases are re-claimed automatically.

---

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Replay trigger | Crash recovery / worker handoff only | Normal execution runs straight through — no overhead |
| Suspension mechanism | Internal `_WorkflowSuspended` exception | Sleep, signals, and child joins stop cleanly; activities run in place |
| Workflow reference | String name | Client lives in a separate codebase; function refs not possible |
| Serialization | JSON only | Safe across code changes; fails fast if not serializable |
| Scaling | Stateless workers + Postgres locking | No external coordinator needed |
| Code changes mid-flight | Fail with divergence error | Explicit is better than silent mismatch |
| Signal storage | `events` table (`event_type = 'signal'`) | Preserves ordering; child completion also uses a reserved signal |
| JSONB decoding | asyncpg codec registered on pool init | asyncpg returns JSONB as strings by default |

---

## Programming Model

### Activities

```python
from temporal_light import activity

@activity(retries=3, timeout=30, backoff_seconds=5)
async def charge_payment(order_id: str, amount: float) -> dict:
    # Must be idempotent — may be called multiple times on retry.
    response = await payment_gateway.charge(order_id, amount)
    return {"transaction_id": response.tx_id}
```

- `retries`: maximum retry attempts after the first failure
- `timeout`: seconds before the attempt is considered failed (`asyncio.wait_for`)
- `backoff_seconds`: fixed delay between attempts (written to DB, not an in-process sleep)
- Inputs and outputs must be JSON-serializable

### Workflows

```python
from temporal_light import workflow, sleep, spawn_child, wait_for_child, wait_for_signal

@workflow
async def order_flow(order_id: str, amount: float) -> dict:
    payment = await charge_payment(order_id, amount)
    risk_child_id = await spawn_child("risk_check_flow", order_id=order_id, amount=amount)

    if amount > 500:
        await sleep(minutes=1)
        approval = await wait_for_signal("approval")
        if not approval.get("approved"):
            await cancel_order(order_id, reason=approval.get("reason"))
            return {"status": "cancelled"}

    risk = await wait_for_child(risk_child_id)
    if risk["risk"] == "high":
        await cancel_order(order_id, reason="Risk check failed")
        return {"status": "cancelled", "risk": risk}

    await send_receipt(order_id, payment["transaction_id"])
    return {"status": "completed", "transaction_id": payment["transaction_id"], "risk": risk}


@workflow
async def risk_check_flow(order_id: str, amount: float) -> dict:
    await sleep(seconds=1)
    return {"order_id": order_id, "risk": "high" if amount >= 5000 else "low"}
```

- No direct I/O, system time, or randomness (not enforced in MVP — detected via divergence)
- `sleep`, `wait_for_signal`, `spawn_child`, and `wait_for_child` are imported from `temporal_light`
- No explicit context parameter — propagated via `contextvars.ContextVar`

### Worker entry point

```python
import os
from temporal_light import Worker
from flows import order_flow, risk_check_flow, charge_payment, send_receipt, cancel_order

Worker(
    workflow_functions=[order_flow, risk_check_flow],
    activity_functions=[charge_payment, send_receipt, cancel_order],
    database_url=os.environ["DATABASE_URL"],
    worker_concurrency=int(os.environ.get("WORKER_CONCURRENCY", "4")),
).run()
```

### Client SDK

```python
from temporal_light import Client, WorkflowFailedError

client = Client("http://api:8080")

# Start a workflow — client never imports workflow functions
handle = await client.start("order_flow", order_id="123", amount=750.0)

# Poll status without blocking
status = await handle.status()

# Stream events via SSE until completion
result = await handle.result()

# Send a signal to a waiting workflow
await client.signal(handle.workflow_id, "approval", payload={"approved": True})
```

`handle.result()` opens an SSE connection and blocks until a `workflow_completed` or `workflow_failed` event is received. `client.start()` alone opens no connections.

---

## Running

### Quick start (Docker)

```bash
cp .env.example .env        # edit credentials if needed
docker compose up --build
```

The `migrate` service runs first and applies the schema idempotently. Then `api` and `worker` start. The dashboard is at `http://localhost:8080`.

### Development (hot reload)

`docker compose up` auto-merges `docker-compose.override.yml`:
- API uses `uvicorn --reload`
- Worker uses `watchfiles` — restarts on any Python file change

No image rebuild needed on code changes.

### Production (no override)

```bash
docker compose -f docker-compose.yml up --build -d
```

### Running the example client

```bash
# With Docker running:
python example/run_client.py

# Or against a custom API:
API_URL=http://my-server:8080 python example/run_client.py
```

The example demonstrates a small order, a large order approved via signal, a large order rejected via signal, a child risk-check workflow, and a non-blocking status poll.

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Dashboard UI |
| `POST` | `/workflows` | Start a workflow |
| `GET` | `/workflows` | List workflows (`?status=running&name=order_flow&limit=50`) |
| `GET` | `/workflows/{id}` | Get workflow status |
| `GET` | `/workflows/{id}/stream` | SSE event stream |
| `POST` | `/workflows/{id}/signals` | Send a named signal |

### SSE stream format

```
data: {"type": "started",    "step_index": -1, "step_name": "workflow", ...}
data: {"type": "scheduled",  "step_index": 0,  "step_name": "charge_payment", ...}
data: {"type": "failed",     "step_index": 0,  "error_message": "...", "attempt": 0, "duration_seconds": 1.2}
data: {"type": "scheduled",  "step_index": 0,  "step_name": "charge_payment", ...}
data: {"type": "completed",  "step_index": 0,  "result": {...}, "duration_seconds": 0.5, "attempts_total": 2}
data: {"type": "sleep",      "step_index": 1,  "wakeup_at": "2026-05-05T12:01:00+00:00"}
data: {"type": "child_started", "step_index": 2, "child_id": "child-wf-id", "workflow_name": "risk_check_flow", ...}
data: {"type": "signal",     "step_index": -1, "signal_type": "__child_completed__", "payload": {"child_id": "child-wf-id", "status": "completed", "result": {...}}}
data: {"type": "signal",     "step_index": -1, "signal_type": "approval", "payload": {"approved": true}}
data: {"type": "workflow_completed", "step_index": -1, "result": {...}}
```

On connect, all existing events are replayed immediately. New events are pushed via Postgres `LISTEN/NOTIFY` — no polling.

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | asyncpg connection string | required |
| `API_PORT` | API server port | `8080` |
| `WORKER_CONCURRENCY` | Max concurrent workflows per worker process | `4` |
| `WORKER_REPLICAS` | Number of worker container instances | `1` |
| `POSTGRES_USER` | Postgres username | required |
| `POSTGRES_PASSWORD` | Postgres password | required |

---

## Testing

```bash
pip install -e ".[dev]"

# Unit tests (no database required)
pytest tests/test_context.py tests/test_decorators.py tests/test_sse.py -v

# Integration tests (requires Postgres)
TEST_DATABASE_URL=postgresql://tl:changeme@localhost/temporal_light pytest -v

# All tests
TEST_DATABASE_URL=postgresql://tl:changeme@localhost/temporal_light pytest -v
```

Integration tests are skipped automatically when `TEST_DATABASE_URL` is not set. All integration tests use `@pytest.mark.integration` and truncate the database between runs.

---

## Known Limitations

**No signal timeout.** A workflow calling `wait_for_signal` will wait indefinitely if the signal never arrives. A `timeout` parameter to `wait_for_signal` would address this.

**Client SSE reconnection is best-effort.** If the API restarts while `handle.result()` is streaming, the client retries up to 3 times with exponential backoff (1 s, 2 s, 4 s). If all retries fail, `WorkflowFailedError` is raised. For production use, add an `after_event_id` cursor so reconnects resume from the last seen event rather than replaying from the beginning.

**Fixed backoff only.** The retry model supports `backoff_seconds` as a fixed delay. Exponential backoff can be added without changing the event log structure.

**No API authentication.** Explicitly out of scope. In production, place the API behind a reverse proxy with auth (e.g. nginx + mTLS, or an API gateway).

**Exactly-once across external systems.** Activities may be called more than once on retry. They must be idempotent by design.

---

## Roadmap

Planned engineering investments beyond the MVP are documented in [FUTURE.md](FUTURE.md).
