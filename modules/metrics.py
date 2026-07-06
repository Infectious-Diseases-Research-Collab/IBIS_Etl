from __future__ import annotations

from datetime import datetime

from sqlalchemy import text


def start_pipeline_run(engine, invocation: str) -> int:
    """Insert a new ops.pipeline_runs row for a top-level invocation
    (e.g. '-a', '-p store_ibis', 'sms --check-delivery'). Returns its id,
    to be passed to record_stage_run/finish_pipeline_run for this run."""
    with engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO ops.pipeline_runs (invocation) VALUES (:invocation) RETURNING id"
        ), {'invocation': invocation})
        return result.scalar()


def record_stage_run(
    engine,
    pipeline_run_id: int,
    stage_name: str,
    started_at: datetime,
    *,
    success: bool,
    rows_written: int = 0,
    errors: list[str] | None = None,
) -> None:
    """Insert one ops.stage_runs row for a single stage/command execution
    that already completed. started_at is measured by the caller around its
    existing .run() call — no new timing logic inside stages themselves."""
    errors = errors or []
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ops.stage_runs
                (pipeline_run_id, stage_name, started_at, success, rows_written, error_count, errors)
            VALUES
                (:pipeline_run_id, :stage_name, :started_at, :success, :rows_written, :error_count, :errors)
        """), {
            'pipeline_run_id': pipeline_run_id,
            'stage_name': stage_name,
            'started_at': started_at,
            'success': success,
            'rows_written': rows_written,
            'error_count': len(errors),
            'errors': errors,
        })


def finish_pipeline_run(
    engine, pipeline_run_id: int, *, success: bool, rows_written: int = 0, error_count: int = 0
) -> None:
    """Update an ops.pipeline_runs row once the whole invocation completes."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ops.pipeline_runs
            SET finished_at = now(), success = :success,
                rows_written = :rows_written, error_count = :error_count
            WHERE id = :id
        """), {
            'id': pipeline_run_id,
            'success': success,
            'rows_written': rows_written,
            'error_count': error_count,
        })
