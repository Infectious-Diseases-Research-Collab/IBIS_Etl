-- Metrics: one row per top-level pipeline invocation (ibis.py -a / -p
-- <stage> / sms.py <command>), and one row per individual stage execution
-- within it. Populated by modules/metrics.py, called from both ibis.py's
-- run_pipeline() and sms.py's command dispatch — no stage code changes.
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id           SERIAL PRIMARY KEY,
    invocation   TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    success      BOOLEAN,
    rows_written INTEGER,
    error_count  INTEGER
);

CREATE TABLE IF NOT EXISTS ops.stage_runs (
    id              SERIAL PRIMARY KEY,
    pipeline_run_id INTEGER NOT NULL REFERENCES ops.pipeline_runs(id),
    stage_name      TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    success         BOOLEAN NOT NULL,
    rows_written    INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    errors          TEXT[]
);
