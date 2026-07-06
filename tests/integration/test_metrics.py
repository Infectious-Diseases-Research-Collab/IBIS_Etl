import pytest
from sqlalchemy import text

from modules.metrics import finish_pipeline_run, record_stage_run, start_pipeline_run

pytestmark = pytest.mark.integration


def test_full_metrics_sequence_against_real_postgres(clean_engine):
    from datetime import datetime, timezone

    run_id = start_pipeline_run(clean_engine, '-a')
    assert isinstance(run_id, int)

    record_stage_run(
        clean_engine, run_id, 'transform_ibis', datetime.now(timezone.utc),
        success=True, rows_written=10, errors=None,
    )
    record_stage_run(
        clean_engine, run_id, 'send_sms', datetime.now(timezone.utc),
        success=False, rows_written=0, errors=['boom'],
    )
    finish_pipeline_run(clean_engine, run_id, success=False, rows_written=10, error_count=1)

    with clean_engine.connect() as conn:
        pipeline_row = conn.execute(text(
            'SELECT invocation, success, rows_written, error_count, finished_at '
            'FROM ops.pipeline_runs WHERE id = :id'
        ), {'id': run_id}).fetchone()
        stage_rows = conn.execute(text(
            'SELECT stage_name, success, rows_written, error_count, errors '
            'FROM ops.stage_runs WHERE pipeline_run_id = :id ORDER BY id'
        ), {'id': run_id}).fetchall()

    assert pipeline_row.invocation == '-a'
    assert pipeline_row.success is False
    assert pipeline_row.rows_written == 10
    assert pipeline_row.error_count == 1
    assert pipeline_row.finished_at is not None

    assert len(stage_rows) == 2
    assert stage_rows[0].stage_name == 'transform_ibis'
    assert stage_rows[0].success is True
    assert stage_rows[1].stage_name == 'send_sms'
    assert stage_rows[1].success is False
    assert stage_rows[1].errors == ['boom']
