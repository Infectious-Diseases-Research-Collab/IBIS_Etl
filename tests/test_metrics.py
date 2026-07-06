from datetime import datetime, timezone
from unittest.mock import MagicMock

from modules.metrics import finish_pipeline_run, record_stage_run, start_pipeline_run


def test_start_pipeline_run_inserts_row_and_returns_id():
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 7
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    run_id = start_pipeline_run(engine, '-a')

    assert run_id == 7
    sql = str(conn.execute.call_args[0][0])
    assert 'INSERT INTO ops.pipeline_runs' in sql
    params = conn.execute.call_args[0][1]
    assert params['invocation'] == '-a'


def test_record_stage_run_inserts_row_with_success_and_counts():
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    record_stage_run(
        engine, pipeline_run_id=7, stage_name='transform_ibis', started_at=started_at,
        success=True, rows_written=42, errors=None,
    )

    sql = str(conn.execute.call_args[0][0])
    assert 'INSERT INTO ops.stage_runs' in sql
    params = conn.execute.call_args[0][1]
    assert params['pipeline_run_id'] == 7
    assert params['stage_name'] == 'transform_ibis'
    assert params['success'] is True
    assert params['rows_written'] == 42
    assert params['error_count'] == 0
    assert params['errors'] == []


def test_record_stage_run_captures_errors_and_count():
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    record_stage_run(
        engine, pipeline_run_id=7, stage_name='send_sms',
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        success=False, rows_written=0, errors=['boom', 'also boom'],
    )

    params = conn.execute.call_args[0][1]
    assert params['success'] is False
    assert params['error_count'] == 2
    assert params['errors'] == ['boom', 'also boom']


def test_finish_pipeline_run_updates_row():
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    finish_pipeline_run(engine, pipeline_run_id=7, success=True, rows_written=100, error_count=0)

    sql = str(conn.execute.call_args[0][0])
    assert 'UPDATE ops.pipeline_runs' in sql
    params = conn.execute.call_args[0][1]
    assert params['id'] == 7
    assert params['success'] is True
    assert params['rows_written'] == 100
    assert params['error_count'] == 0
