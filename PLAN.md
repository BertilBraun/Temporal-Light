# Temporal-Light — Implementation Plan

## 0. Scope

**In-scope**

* Linear workflows (no DAGs, no parallelism within a workflow)
* Multiple worker processes (horizontally scalable, stateless)
* Durable execution via append-only event log
* Retries with per-activity policy
* Manual signals (e.g. approval)
* Sleep / time-based suspension
* Basic observability (status, step history, retry counts, durations)
* SSE-based result streaming for clients
* Docker-based deployment with dev hot-reload

**Out-of-scope (explicitly)**

* Exactly-once guarantees across arbitrary external systems
* Complex scheduling / cron
* Dynamic code evolution / seamless upgrades
* API authentication
* Client-side function references (clients identify workflows by name string)

---

## 1. System Architecture

Four independent systems:

```text
                    ┌──────────────────────────────────┐
                    │            Postgres               │
                    │  workflows / events / workers     │
                    └───┬──────────────────────┬────────┘
                        │                      │
              claim/write│              LISTEN/ │NOTIFY
              events     │                      │
                         │                      │
               ┌─────────▼──────┐    ┌──────────▼──────┐
               │   Worker(s)    │    │    API Server    │
               │                │    │                  │
               │ - poll DB      │    │ - start workflow │
               │ - replay       │    │ - send signal    │
               │ - execute      │    │ - SSE stream     │
               │ - write events │    │                  │
               └────────────────┘    └────────┬─────────┘
                                              │ SSE / HTTP
                                     ┌────────▼─────────┐
                                     │   Client SDK     │
                                     │                  │
                                     │  start("work",…) │
                                     │  handle.result() │
                                     │  signal(id, …)   │
                                     └──────────────────┘
```

| System | Code it needs | Talks to |
| --- | --- | --- |
| Postgres | — | — |
| API | `temporal_light` only | Postgres |
| Worker | `temporal_light` + user's flows | Postgres |
| Client | `temporal_light` client SDK | API only |

Workers are the only consumers of user workflow/activity code. The API is a pure DB↔HTTP bridge. Clients are external and identify workflows by string name — they never import workflow functions.

---

## 2. Programming Model

### 2.1 Activity

```python
@activity(retries=2, timeout=600)
async def process_payment(order_id, amount):
    ...
    return {"tx_id": "abc"}
```

* `retries`: max retry attempts on failure
* `timeout`: seconds before the attempt is considered failed
* Must be idempotent — responsibility of the implementation
* Inputs and outputs must be JSON-serializable

### 2.2 Workflow

```python
@workflow
async def order_flow(order_id, amount):
    result = await process_payment(order_id, amount)
    await sleep(hours=24)
    await send_receipt(order_id, result["tx_id"])
    return result
```

* No `ctx` parameter — context is propagated via `contextvars.ContextVar`
* No direct I/O, DB access, system time, or randomness (not enforced in MVP, detected via divergence)
* `sleep` is imported from `temporal_light`
* Inputs and outputs must be JSON-serializable

### 2.3 Worker entry point

```python
# worker_entry.py
import os
from temporal_light import Worker
from flows import order_flow, process_payment, send_receipt

Worker(
    workflows=[order_flow],
    activities=[process_payment, send_receipt],
    database_url=os.environ["DATABASE_URL"],
).run()
```

### 2.4 Client usage

```python
from temporal_light import Client

client = Client("http://api:8080")

# Start a workflow by name — client has no import of the workflow function
handle = await client.start("order_flow", order_id="123", amount=99.0)

# Stream events via SSE until completion
result = await handle.result()

# Send a signal
await client.signal(handle.id, "approval", payload={"approved": True})

# Poll status
status = await handle.status()
```

---

## 3. Execution Model

### 3.1 Event history loading

When a worker picks up a workflow, before executing any user code, it loads the complete event history for that workflow from the database into the `WorkflowContext`. This is a single query, not per-step. All subsequent step lookups are in-memory scans over this list — no per-step DB reads during replay.

### 3.2 Activity execution (detailed)

When `await some_activity(arg1, arg2)` is called inside a workflow:

1. Read `WorkflowContext` from the context var — contains `workflow_id` and the pre-loaded event history
2. Determine `step_index` by incrementing the context's step counter
3. Scan the in-memory event history for a `completed` event at this `step_index`
   * **Found** → return `event.payload.result` immediately, increment step counter, continue — this is the replay path
4. Check for divergence: assert `step_name` matches the recorded `scheduled` event if one exists (crash mid-schedule recovery)
5. No completed event found — execute for real:
   * Write `scheduled` event to DB with serialized inputs
   * Run `asyncio.wait_for(fn(arg1, arg2), timeout=policy.timeout_seconds)`
   * **Success** → write `completed` event with result, return result, continue
   * **Failure** (exception or timeout):
      * Count existing `failed` events for this `step_index` to determine `attempt_number`
      * If `attempt_number < policy.max_retries`:
        * Write `failed` event with `{"error": "...", "attempt": attempt_number}`
        * Set `workflows.run_at = now() + backoff_seconds`
        * Raise `_WorkflowSuspended` — workflow stops, worker is freed
      * If `attempt_number >= policy.max_retries`:
        * Write final `failed` event
        * Mark `workflows.status = WorkflowStatus.FAILED`
        * Raise `_WorkflowSuspended`

The worker is never blocked waiting for a retry delay. It writes the scheduled wakeup time to the DB and moves on. The scheduler picks the workflow up again when `run_at <= now()`.

### 3.3 Replay (crash recovery / worker handoff)

On restart or when a different worker claims a workflow:

* Load the full event history from DB into `WorkflowContext`
* Re-create the workflow coroutine from scratch and run it
* Steps with a `completed` event return from history instantly (step 3.2.3 above)
* First step without a `completed` event is the frontier — execute for real from step 3.2.5

History is the only persistent state. Coroutine state is never saved across runs.

### 3.3 Suspension (sleep and signals)

`sleep` and `wait_for_signal` must genuinely stop execution because coroutine state cannot persist across restarts or worker boundaries:

1. Record the suspension event (with `wakeup_at` or signal type)
2. Set `workflows.run_at` to the appropriate future time
3. Raise internal `_WorkflowSuspended` (caught by the runner, never visible to user code)
4. Scheduler picks up the workflow when `run_at <= now`
5. Workflow re-runs, replays all prior steps, reaches the suspension step
   * Sleep: if `now >= wakeup_at` → continue; else re-suspend
   * Signal: if signal row exists → continue; else re-suspend

### 3.4 Context propagation

```python
_current_ctx: ContextVar[WorkflowContext] = ContextVar("wf_ctx")
```

Set by the runner before invoking the workflow coroutine. Read by the `@activity` decorator and `sleep`/`wait_for_signal` to know which workflow they belong to.

### 3.5 Divergence detection

On replay, each step asserts `(step_index, step_name)` matches the recorded event. If the code has changed the step sequence, the workflow is marked `failed` immediately with a clear error.

---

## 4. Data Model

```sql
CREATE TABLE workers (
    worker_id    TEXT PRIMARY KEY,
    last_seen    TIMESTAMPTZ NOT NULL
);

CREATE TABLE workflows (
    workflow_id  TEXT PRIMARY KEY,       -- uuid
    name         TEXT NOT NULL,          -- workflow __qualname__
    status       TEXT NOT NULL,          -- running | completed | failed
    run_at       TIMESTAMPTZ NOT NULL,   -- scheduler picks up when run_at <= now()
    locked_by    TEXT REFERENCES workers(worker_id),
    locked_until TIMESTAMPTZ,            -- heartbeat-based lease
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE events (
    id           BIGSERIAL PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(workflow_id),
    step_index   INTEGER NOT NULL,       -- -1 for workflow-level events and signals
    step_name    TEXT NOT NULL,          -- activity __qualname__, "sleep", "signal", "workflow"
    event_type   TEXT NOT NULL,          -- started | scheduled | completed | failed | sleep | signal
    payload      JSONB,                  -- inputs, outputs, errors, wakeup_at, etc.
    timestamp    TIMESTAMPTZ NOT NULL,
    UNIQUE (workflow_id, step_index, event_type)
);

CREATE INDEX ON workflows (run_at) WHERE status = 'running';
```

**Event conventions:**

| event_type | step_index | payload |
| --- | --- | --- |
| `started` | -1 | `{"input": {...}}` |
| `scheduled` | N | `{"step_name": "...", "input": [...]}` |
| `completed` | N | `{"result": ...}` |
| `failed` | N | `{"error": "...", "attempt": N}` |
| `sleep` | N | `{"wakeup_at": "..."}` |
| `signal` | -1 | `{"signal_type": "...", "payload": {...}}` |
| `workflow_completed` | -1 | `{"result": ...}` |
| `workflow_failed` | -1 | `{"error": "..."}` |

Signals are event-log entries, not a separate table. Signal ordering relative to other events is naturally preserved.

---

## 5. Multi-Worker Coordination

### 5.1 Workflow claiming

```sql
BEGIN;
SELECT * FROM workflows
WHERE run_at <= NOW()
  AND status = 'running'
  AND (locked_by IS NULL OR locked_until < NOW())
ORDER BY run_at
LIMIT 1
FOR UPDATE SKIP LOCKED;
-- UPDATE workflows SET locked_by = $worker_id, locked_until = NOW() + 30s
COMMIT;
```

`SKIP LOCKED` ensures workers claim different workflows atomically. No retry loops, no contention.

### 5.2 Heartbeating

While a workflow is executing, the owning worker updates `locked_until = now() + 30s` every 10 seconds. If the worker crashes, `locked_until` expires and another worker can claim the workflow (and replay from history).

### 5.3 Worker concurrency

Each worker runs up to `WORKER_CONCURRENCY` workflows simultaneously as asyncio Tasks. The scheduler loop only claims a new workflow when there is capacity. Timeout is implemented with `asyncio.wait_for` — no threads needed.

---

## 6. Scheduler Loop

```python
async def scheduler_loop(worker_concurrency: int) -> None:
    active_tasks: set[asyncio.Task] = set()
    had_work = False

    while True:
        if not had_work:
            await asyncio.sleep(1)

        slots_available = worker_concurrency - len(active_tasks)
        if slots_available == 0:
            had_work = False
            await asyncio.sleep(0.1)
            continue

        workflow = await db.claim_next_workflow()
        if workflow is None:
            had_work = False
            continue

        had_work = True
        task = asyncio.create_task(run_and_release(workflow))
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)
```

* Only claims a new workflow when `active_tasks` is below `WORKER_CONCURRENCY`
* Poll immediately when work was found and capacity remains
* Sleep 1 second only when idle (no claimable workflows)
* Short 100ms yield when at capacity, to let running tasks complete without tight-looping

---

## 7. Retry Model

```python
@activity(retries=3, timeout=60, backoff_seconds=5)
```

* On each failure, a `failed` event is written with the attempt number — retry count is always derived from the event log, never from in-memory state
* Retry is scheduled non-blocking: `workflows.run_at = now() + backoff_seconds`, then `_WorkflowSuspended` is raised — the worker is freed immediately
* The scheduler picks the workflow up again naturally when `run_at <= now()`
* After `max_retries` failed attempts, the workflow is marked `WorkflowStatus.FAILED` — no further scheduling

Backoff strategy for MVP: fixed interval (`backoff_seconds`). Exponential backoff can be added later without changing the event log structure.

---

## 8. API Surface

### HTTP endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/workflows` | Start a workflow |
| `GET` | `/workflows/{id}` | Get workflow status |
| `GET` | `/workflows/{id}/stream` | SSE stream of events |
| `POST` | `/workflows/{id}/signals` | Send a signal |
| `GET` | `/workflows` | List workflows (observability) |

### SSE stream format

```text
GET /workflows/{id}/stream
Content-Type: text/event-stream

data: {"type": "scheduled", "step_index": 0, "step_name": "process_payment"}
data: {"type": "completed", "step_index": 0, "result": {"tx_id": "abc"}}
data: {"type": "sleep", "step_index": 1, "wakeup_at": "2026-05-01T00:00:00Z"}
data: {"type": "workflow_completed", "result": {"tx_id": "abc"}}
```

On connect, the API replays all existing events from the DB, then tails live via Postgres `LISTEN/NOTIFY`. Workers `NOTIFY workflow_events` after every event write. No polling in the API.

---

## 9. Serialization

* JSON only — inputs, outputs, errors
* Raise clearly at the boundary if a value is not JSON-serializable
* Errors stored as stringified exception: `{"type": "ValueError", "message": "..."}`

---

## 10. Project Layout

```text
Temporal-Light/
  temporal_light/
    __init__.py            # public exports: Worker, Client, workflow, activity, sleep
    api/
      __init__.py
      server.py            # FastAPI app, SSE, endpoints
    worker/
      __init__.py
      runner.py            # WorkflowRunner: replay + forward execution
      executor.py          # activity execution, retries, timeout
      scheduler.py         # scheduler loop, claim, heartbeat
      context.py           # WorkflowContext, ContextVar
    db/
      __init__.py
      connection.py        # asyncpg pool
      queries.py           # all DB operations
      migrate.py           # schema creation (run once on startup)
      schema.sql
    decorators.py          # @workflow, @activity
    client.py              # Client SDK
    sleep.py               # sleep(), wait_for_signal()

  example/
    flows.py               # example @workflow and @activity definitions
    worker_entry.py        # Worker([...], [...]).run()

  docker-compose.yml           # prod
  docker-compose.override.yml  # dev: volume mounts + hot reload
  Dockerfile
  requirements.txt
```

---

## 11. Docker Setup

### Dockerfile

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -e .
COPY . .
```

### docker-compose.yml (prod)

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: temporal_light
      POSTGRES_USER: tl
      POSTGRES_PASSWORD: tl
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tl"]
      interval: 5s
      retries: 5

  migrate:
    build: .
    command: python -m temporal_light.db.migrate
    environment:
      DATABASE_URL: postgresql://tl:tl@postgres/temporal_light
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    command: python -m temporal_light.api
    environment:
      DATABASE_URL: postgresql://tl:tl@postgres/temporal_light
    ports:
      - "8080:8080"
    depends_on:
      migrate:
        condition: service_completed_successfully

  worker:
    build: .
    command: python worker_entry.py
    environment:
      DATABASE_URL: postgresql://tl:tl@postgres/temporal_light
    depends_on:
      migrate:
        condition: service_completed_successfully
    deploy:
      replicas: 2

volumes:
  pgdata:
```

### docker-compose.override.yml (dev — auto-merged)

```yaml
services:
  api:
    command: uvicorn temporal_light.api.server:app --reload --host 0.0.0.0 --port 8080
    volumes:
      - .:/app

  worker:
    command: watchfiles "python worker_entry.py" .
    volumes:
      - .:/app
```

---

## 12. Implementation Phases

### Phase 1 — Core engine

* Postgres schema + migration service
* `@workflow` and `@activity` decorators
* `WorkflowContext` + context var propagation
* Forward execution (run in place)
* Replay from event log
* Retries + timeout
* Divergence detection
* Scheduler loop with `FOR UPDATE SKIP LOCKED`
* Heartbeating

### Phase 2 — Suspension + API

* `sleep()` and `wait_for_signal()`
* API server (start, status, signal endpoints)
* SSE streaming via `LISTEN/NOTIFY`
* Client SDK

### Phase 3 — Observability + DX

* `GET /workflows` list with filtering
* Retry counts and durations in event stream
* Worker hot-reload setup (`watchfiles`, `uvicorn --reload`)
* Docker compose dev/prod split
* Example project

---

## 13. Code Style

These rules apply across the entire codebase.

### 13.1 Naming

* No abbreviations — `workflow_identifier` not `wf_id`, `step_index` not `idx`, `database_url` not `db_url`
* Names must be self-documenting at the call site

### 13.2 Type annotations

* Every function parameter, return value, and class field must be annotated
* No `Any` except at explicit serialization boundaries (DB read, JSON parse)
* Use `Optional[X]` / `X | None` rather than leaving types implicit

### 13.3 No stringly-typed boundaries

No `dict[str, Any]` passed between functions. All structured data crossing a function boundary must use a dataclass or enum.

**Bad:**

```python
def handle_event(event: dict) -> None:
    if event["type"] == "completed":
        result = event["payload"]["result"]
```

**Good:**

```python
@dataclass(frozen=True)
class CompletedEvent:
    step_index: int
    step_name: str
    result: Any
    timestamp: datetime

def handle_event(event: WorkflowEvent) -> None:
    if isinstance(event, CompletedEvent):
        result = event.result
```

Strings are only acceptable at the serialization boundary (writing to / reading from DB or JSON). Internally everything is typed.

### 13.4 Enums for categorical values

```python
class WorkflowStatus(Enum):
    RUNNING = "running"
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
```

Enums serialize to their string value at the DB/wire boundary, but all internal code works with enum members.

---

## 14. Deployment Model

No image registry. To deploy, clone the repo, add your workflow code, and build locally.

### 14.1 Workflow

```bash
git clone https://github.com/you/temporal-light
cd temporal-light
cp .env.example .env        # fill in credentials and config
docker compose up --build -d
```

That's it. The client only needs the host address and `API_PORT`.

### 14.2 Project layout (with user code)

User workflow code lives inside the cloned repo:

```text
temporal-light/
  temporal_light/        # framework — don't touch
  example/
    flows.py             # user's @workflow and @activity definitions
    worker_entry.py      # Worker([...]).run()
  docker-compose.yml
  docker-compose.override.yml
  Dockerfile
  .env.example
  .env                   # gitignored — actual credentials
```

### 14.3 Single Dockerfile for all services

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -e .
COPY . .
```

The same image is built once. `api` and `worker` services use different `command` values — no separate base image or multi-stage complexity.

### 14.4 docker-compose.yml (prod)

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: temporal_light
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      retries: 5

  migrate:
    build: .
    command: python -m temporal_light.db.migrate
    environment:
      DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/temporal_light
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    command: python -m temporal_light.api
    environment:
      DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/temporal_light
      API_PORT: ${API_PORT:-8080}
    ports:
      - "${API_PORT:-8080}:${API_PORT:-8080}"
    depends_on:
      migrate:
        condition: service_completed_successfully

  worker:
    build: .
    command: python example/worker_entry.py
    environment:
      DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/temporal_light
      WORKER_CONCURRENCY: ${WORKER_CONCURRENCY:-4}
    depends_on:
      migrate:
        condition: service_completed_successfully
    deploy:
      replicas: ${WORKER_REPLICAS:-1}

volumes:
  pgdata:
```

### 14.5 docker-compose.override.yml (dev — auto-merged)

```yaml
services:
  api:
    command: uvicorn temporal_light.api.server:app --reload --host 0.0.0.0 --port 8080
    volumes:
      - .:/app

  worker:
    command: watchfiles "python example/worker_entry.py" .
    volumes:
      - .:/app
```

In development, `docker compose up` auto-merges the override — hot reload with no image rebuilds on file change. For production, `docker compose -f docker-compose.yml up --build -d` skips the override.

### 14.6 .env.example

```bash
POSTGRES_USER=tl
POSTGRES_PASSWORD=changeme
API_PORT=8080
WORKER_CONCURRENCY=4
WORKER_REPLICAS=1
```

### 14.7 Environment variables

| Variable | Description | Default |
| --- | --- | --- |
| `DATABASE_URL` | Postgres connection string | required |
| `API_PORT` | Port the API server listens on | `8080` |
| `WORKER_CONCURRENCY` | Max concurrent workflows per worker process | `4` |
| `WORKER_REPLICAS` | Number of worker container instances | `1` |
| `POSTGRES_USER` | Postgres username | required |
| `POSTGRES_PASSWORD` | Postgres password | required |

---

## 15. Multi-Language Clients

The API is plain HTTP + SSE. Any language can be a client.

```text
Python worker code  ←→  Postgres  ←→  API  ←→  TypeScript client (shop frontend)
                                            ←→  Python client (backend service)
                                            ←→  Go client (data pipeline)
```

Example TypeScript client call:

```typescript
const client = new TemporalLightClient("https://api.myapp.com:8080");
const handle = await client.start("order_flow", { order_id: "123", amount: 99.0 });
const result = await handle.result();
```

The workflow execution is always Python (where the `@workflow` and `@activity` code lives). The client language is irrelevant — it only speaks HTTP to the API.

---

## 16. Key Trade-offs

| Decision | Choice | Rationale |
| --- | --- | --- |
| Replay trigger | Crash recovery / worker handoff only | Simpler than always-replay; normal path runs straight through |
| Suspension mechanism | Internal `_WorkflowSuspended` exception | Only sleep/signals need to stop; activities run in place |
| Client workflow reference | String name | Client lives in a separate codebase; function refs not possible |
| Serialization | JSON only | Safer than pickle across code changes |
| Scaling | Stateless workers + Postgres locking | No external coordinator needed |
| Code changes mid-flight | Fail workflow with divergence error | Simpler than versioning; explicit is better |
| Signal storage | Events table (event_type='signal') | Preserves ordering relative to other events; no separate table |
| Schema management | Dedicated migrate service in compose | Clean separation; runs once before api/worker start |
