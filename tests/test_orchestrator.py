import pytest
from ibis import topological_sort, build_run_list

_STAGE_DEPS = {
    'ftp_to_extracted': [],
    'mdb_to_bronze':    ['ftp_to_extracted'],
    'bronze_to_silver': ['mdb_to_bronze'],
    'transform_ibis':   ['bronze_to_silver'],
    'measures_ibis':    ['transform_ibis'],
    'promote_ibis':     ['measures_ibis'],
    'store_ibis':       ['promote_ibis'],
}

def test_topological_sort_full_order():
    order = topological_sort(_STAGE_DEPS)
    assert order == [
        'ftp_to_extracted', 'mdb_to_bronze', 'bronze_to_silver', 'transform_ibis',
        'measures_ibis', 'promote_ibis', 'store_ibis',
    ]

def test_topological_sort_single_stage():
    order = topological_sort({'only': []})
    assert order == ['only']

def test_build_run_list_all():
    stages = build_run_list(_STAGE_DEPS, run_all=True)
    assert stages == [
        'ftp_to_extracted', 'mdb_to_bronze', 'bronze_to_silver', 'transform_ibis',
        'measures_ibis', 'promote_ibis', 'store_ibis',
    ]

def test_build_run_list_single_stage():
    stages = build_run_list(_STAGE_DEPS, run_all=False, pipeline='transform_ibis')
    assert stages == ['transform_ibis']

def test_build_run_list_unknown_stage_raises():
    with pytest.raises(SystemExit):
        build_run_list(_STAGE_DEPS, run_all=False, pipeline='nonexistent')

from unittest.mock import MagicMock, patch
from stages.base import StageResult
from ibis import run_pipeline, STAGE_CLASSES


def test_run_pipeline_skips_downstream_on_failure():
    """A failing stage causes its downstream stages to be skipped."""
    config = MagicMock()
    engine = MagicMock()

    call_log = []

    # Patch MdbToBronze.run to fail
    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['boom'])):
        # Patch BronzeToSilver.run — should NOT be called
        with patch.object(STAGE_CLASSES['bronze_to_silver'], 'run') as mock_silver:
            with patch('ibis.send_pipeline_report'):
                with patch('sys.exit'):
                    run_pipeline(['mdb_to_bronze', 'bronze_to_silver'], config, engine)

    mock_silver.assert_not_called()


def test_run_pipeline_wraps_unexpected_exception():
    """An unhandled exception from a stage is caught and wrapped, not propagated."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      side_effect=RuntimeError('unexpected crash')):
        with patch('ibis.send_pipeline_report'):
            with patch('sys.exit') as mock_exit:
                run_pipeline(['mdb_to_bronze'], config, engine)

    mock_exit.assert_called_once_with(1)


def test_run_pipeline_exits_1_on_failure():
    """run_pipeline calls sys.exit(1) when any stage fails."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['fail'])):
        with patch('ibis.send_pipeline_report'):
            with patch('sys.exit') as mock_exit:
                run_pipeline(['mdb_to_bronze'], config, engine)

    mock_exit.assert_called_once_with(1)


def test_run_pipeline_calls_notifier_on_failure():
    """send_pipeline_report is called after a failed run."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['boom'])):
        with patch('ibis.send_pipeline_report') as mock_notify:
            with patch('sys.exit'):
                run_pipeline(['mdb_to_bronze'], config, engine)

    mock_notify.assert_called_once()
    call_kwargs = mock_notify.call_args.kwargs
    assert 'mdb_to_bronze' in call_kwargs['results']
    assert call_kwargs['stages'] == ['mdb_to_bronze']


def test_maybe_init_readonly_role_skips_when_not_configured():
    from ibis import _maybe_init_readonly_role

    config = MagicMock()
    config.get.return_value = {}  # no readonly_password_secret_file
    engine = MagicMock()

    with patch('ibis.init_readonly_role') as mock_init:
        _maybe_init_readonly_role(config, engine)

    mock_init.assert_not_called()


def test_maybe_init_readonly_role_reads_secret_and_provisions(tmp_path):
    from ibis import _maybe_init_readonly_role

    secret_file = tmp_path / 'readonly_password'
    secret_file.write_text('s3cr3t\n')

    config = MagicMock()
    config.get.return_value = {'readonly_password_secret_file': str(secret_file)}
    engine = MagicMock()

    with patch('ibis.init_readonly_role') as mock_init:
        _maybe_init_readonly_role(config, engine)

    mock_init.assert_called_once_with(engine, 's3cr3t')


def test_run_pipeline_calls_notifier_on_success():
    """send_pipeline_report is called even on a clean run (it decides internally)."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=True, rows_written=10)):
        with patch('ibis.send_pipeline_report') as mock_notify:
            run_pipeline(['mdb_to_bronze'], config, engine)

    mock_notify.assert_called_once()


def test_run_pipeline_passes_full_rebuild_to_stages_that_accept_it():
    from unittest.mock import MagicMock, patch
    from stages.base import StageResult
    from ibis import run_pipeline, STAGE_CLASSES

    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['bronze_to_silver'], 'run',
                      return_value=StageResult(success=True)) as mock_run:
        with patch('ibis.send_pipeline_report'):
            run_pipeline(['bronze_to_silver'], config, engine, full_rebuild=True)

    mock_run.assert_called_once_with(full_rebuild=True)


def test_run_pipeline_full_rebuild_ignored_by_stages_without_the_kwarg():
    from unittest.mock import MagicMock, patch
    from stages.base import StageResult
    from ibis import run_pipeline, STAGE_CLASSES

    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=True)) as mock_run:
        with patch('ibis.send_pipeline_report'):
            run_pipeline(['mdb_to_bronze'], config, engine, full_rebuild=True)

    mock_run.assert_called_once_with()


def test_run_pipeline_records_metrics_for_each_stage(monkeypatch):
    """Every stage executed must produce exactly one record_stage_run call,
    and the whole invocation must start and finish exactly once."""
    import ibis as ibis_module

    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(ibis_module, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 42)[1])
    monkeypatch.setattr(ibis_module, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(ibis_module, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))

    config = MagicMock()
    engine = MagicMock()

    class FakeStage:
        dependencies: list[str] = []
        def __init__(self, config, engine):
            pass
        def run(self):
            return StageResult(success=True, rows_written=5)

    monkeypatch.setitem(ibis_module.STAGE_CLASSES, 'fake_stage', FakeStage)
    monkeypatch.setattr(ibis_module, 'send_pipeline_report', lambda **kw: None)

    ibis_module.run_pipeline(['fake_stage'], config, engine, invocation='-p fake_stage')

    assert calls['start'] == ['-p fake_stage']
    assert len(calls['record']) == 1
    _, record_kwargs = calls['record'][0]
    assert record_kwargs['success'] is True
    assert record_kwargs['rows_written'] == 5
    assert len(calls['finish']) == 1
    _, finish_kwargs = calls['finish'][0]
    assert finish_kwargs['success'] is True
    assert finish_kwargs['rows_written'] == 5


def test_run_pipeline_records_failure_and_skips_downstream_without_spurious_record(monkeypatch):
    """finish_pipeline_run's success must reflect ANY failed stage, and a stage
    skipped due to an upstream failure must NOT get its own record_stage_run call."""
    import ibis as ibis_module

    calls = {'start': [], 'record': [], 'finish': []}
    monkeypatch.setattr(ibis_module, 'start_pipeline_run', lambda engine, invocation: (calls['start'].append(invocation), 99)[1])
    monkeypatch.setattr(ibis_module, 'record_stage_run', lambda *a, **kw: calls['record'].append((a, kw)))
    monkeypatch.setattr(ibis_module, 'finish_pipeline_run', lambda *a, **kw: calls['finish'].append((a, kw)))
    monkeypatch.setattr(ibis_module, 'send_pipeline_report', lambda **kw: None)

    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['boom'])):
        with patch('sys.exit'):
            ibis_module.run_pipeline(
                ['mdb_to_bronze', 'bronze_to_silver'], config, engine, invocation='-a',
            )

    assert calls['start'] == ['-a']
    # Only the executed stage (mdb_to_bronze) gets a record; bronze_to_silver
    # is skipped due to the upstream failure and must not be recorded.
    assert len(calls['record']) == 1
    record_args, record_kwargs = calls['record'][0]
    assert record_args[2] == 'mdb_to_bronze'
    assert record_kwargs['success'] is False
    assert len(calls['finish']) == 1
    _, finish_kwargs = calls['finish'][0]
    assert finish_kwargs['success'] is False
