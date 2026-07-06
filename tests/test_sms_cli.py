from __future__ import annotations

from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

import sms
from sms import _run
from stages.base import StageResult


def _args(**overrides):
    defaults = dict(
        sync=False, dry_run=False, weekly_report=False, init_db=False,
        check_delivery=False, resend=False, subjid=None, week=None,
        actor=None, note=None, verbose=False,
    )
    defaults.update(overrides)
    return Namespace(**defaults)


def test_check_delivery_delegates_to_fetch_dlr_stage():
    """--check-delivery must run the FetchDlr stage (not duplicate its logic
    inline) so the "fail only when every checked row errored" rule and the
    metadata it returns are the single source of truth."""
    config = MagicMock()
    engine = MagicMock()

    with patch('sms.FetchDlr') as MockFetchDlr:
        MockFetchDlr.return_value.run.return_value = StageResult(
            success=True, rows_written=3,
            metadata={'checked': 3, 'updated': 3, 'pending': 0, 'errors': [], 'flagged': 0},
        )
        with pytest.raises(SystemExit) as exc_info:
            _run(_args(check_delivery=True), config, engine)

    MockFetchDlr.assert_called_once_with(config=config, engine=engine)
    MockFetchDlr.return_value.run.assert_called_once()
    assert exc_info.value.code == 0


def test_check_delivery_exits_nonzero_when_stage_fails():
    config = MagicMock()
    engine = MagicMock()

    with patch('sms.FetchDlr') as MockFetchDlr:
        MockFetchDlr.return_value.run.return_value = StageResult(
            success=False, rows_written=0,
            metadata={'checked': 2, 'updated': 0, 'pending': 0, 'errors': [{'log_id': 1}, {'log_id': 2}], 'flagged': 0},
            errors=['all 2 checked row(s) errored'],
        )
        with pytest.raises(SystemExit) as exc_info:
            _run(_args(check_delivery=True), config, engine)

    assert exc_info.value.code == 1


def test_run_records_metrics_around_default_send(monkeypatch):
    """The default (no-flag) command path — SmsProcessor.run() — must be
    wrapped in start/record/finish just like every other sms.py command."""
    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(sms, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 1)[1])
    monkeypatch.setattr(sms, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(sms, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))
    monkeypatch.setattr(sms, 'init_schemas', lambda engine: None)
    monkeypatch.setattr(sms, 'run_migrations', lambda engine: None)

    fake_result = MagicMock(sent=3, failed=0, skipped=1, failures=[])
    monkeypatch.setattr(sms.SmsProcessor, 'run', lambda self: fake_result)

    config = MagicMock()
    engine = MagicMock()

    with pytest.raises(SystemExit):
        _run(_args(), config, engine)

    assert calls['start'] == ['sms']
    assert len(calls['record']) == 1
    assert len(calls['finish']) == 1

    (record_args, record_kwargs) = calls['record'][0]
    assert record_kwargs['success'] is True
    assert record_kwargs['rows_written'] == 3
    assert record_kwargs['errors'] == []

    (finish_args, finish_kwargs) = calls['finish'][0]
    assert finish_kwargs['success'] is True
    assert finish_kwargs['rows_written'] == 3
    assert finish_kwargs['error_count'] == 0


def test_resend_validation_failure_records_failed_stage_run(monkeypatch):
    """--resend with missing required flags exits before ever calling
    processor.resend(...) — the recorded stage_run must still reflect that
    failure (success=False, the validation error message), not a stale
    default from before the validation check ran."""
    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(sms, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 1)[1])
    monkeypatch.setattr(sms, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(sms, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))
    monkeypatch.setattr(sms, 'init_schemas', lambda engine: None)
    monkeypatch.setattr(sms, 'run_migrations', lambda engine: None)

    config = MagicMock()
    engine = MagicMock()

    with pytest.raises(SystemExit) as exc_info:
        _run(_args(resend=True), config, engine)

    assert exc_info.value.code == 1
    assert len(calls['record']) == 1
    assert len(calls['finish']) == 1

    (record_args, record_kwargs) = calls['record'][0]
    assert record_kwargs['success'] is False
    assert record_kwargs['errors'] == ['--resend requires --subjid, --week, --actor']

    (finish_args, finish_kwargs) = calls['finish'][0]
    assert finish_kwargs['success'] is False
    assert finish_kwargs['error_count'] == 1


def test_construction_time_exception_on_default_path_records_correct_stage_name(monkeypatch):
    """If SmsProcessor(...) construction itself raises on the default
    (no-flag) send path — before stage_name is ever reassigned deep inside
    that branch — the recorded stage_name must still say 'sms_send', not the
    unrelated pre-try default ('sms_init_db')."""
    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(sms, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 1)[1])
    monkeypatch.setattr(sms, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(sms, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))
    monkeypatch.setattr(sms, 'init_schemas', lambda engine: None)
    monkeypatch.setattr(sms, 'run_migrations', lambda engine: None)

    def boom(self, *, config, engine):
        raise RuntimeError('construction blew up')

    monkeypatch.setattr(sms.SmsProcessor, '__init__', boom)

    config = MagicMock()
    engine = MagicMock()

    with pytest.raises(RuntimeError, match='construction blew up'):
        _run(_args(), config, engine)

    assert len(calls['record']) == 1
    (record_args, record_kwargs) = calls['record'][0]
    # record_stage_run(engine, pipeline_run_id, stage_name, started_at, ...)
    assert record_args[2] == 'sms_send'
    assert record_kwargs['success'] is False


def test_unexpected_exception_records_failed_metrics_and_still_propagates(monkeypatch):
    """A genuinely unexpected exception (e.g. a bug or a network error) raised
    inside a command branch — as opposed to an intentional sys.exit(...) —
    must still result in exactly one record_stage_run/finish_pipeline_run
    call with success=False and the exception message in errors, and the
    exception must still propagate afterward (sms.py's exit code/crash
    behavior is unchanged; only the recorded metrics are corrected)."""
    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(sms, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 1)[1])
    monkeypatch.setattr(sms, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(sms, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))
    monkeypatch.setattr(sms, 'init_schemas', lambda engine: None)
    monkeypatch.setattr(sms, 'run_migrations', lambda engine: None)

    def boom(self):
        raise RuntimeError('unexpected DB error')

    monkeypatch.setattr(sms.SmsProcessor, 'run', boom)

    config = MagicMock()
    engine = MagicMock()

    with pytest.raises(RuntimeError, match='unexpected DB error'):
        _run(_args(), config, engine)

    assert len(calls['record']) == 1
    assert len(calls['finish']) == 1

    (record_args, record_kwargs) = calls['record'][0]
    assert record_kwargs['success'] is False
    assert record_kwargs['errors'] == ['unexpected DB error']

    (finish_args, finish_kwargs) = calls['finish'][0]
    assert finish_kwargs['success'] is False
    assert finish_kwargs['error_count'] == 1
