CREATE TABLE IF NOT EXISTS workers (
    worker_id    TEXT PRIMARY KEY,
    last_seen    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS workflows (
    workflow_id  TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,
    parent_id    TEXT REFERENCES workflows(workflow_id),
    run_at       TIMESTAMPTZ NOT NULL,
    locked_by    TEXT REFERENCES workers(worker_id),
    locked_until TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS parent_id TEXT REFERENCES workflows(workflow_id);

-- No UNIQUE constraint on (workflow_id, step_index, event_type): the retry
-- model writes multiple 'failed' events at the same step_index, which would
-- violate such a constraint.
CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(workflow_id),
    step_index   INTEGER NOT NULL,
    step_name    TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB,
    timestamp    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS workflows_run_at_status_idx
    ON workflows (run_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS events_workflow_id_idx
    ON events (workflow_id);
