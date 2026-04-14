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
            with patch('sys.exit'):
                run_pipeline(['mdb_to_bronze', 'bronze_to_silver'], config, engine)

    mock_silver.assert_not_called()


def test_run_pipeline_wraps_unexpected_exception():
    """An unhandled exception from a stage is caught and wrapped, not propagated."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      side_effect=RuntimeError('unexpected crash')):
        with patch('sys.exit') as mock_exit:
            run_pipeline(['mdb_to_bronze'], config, engine)

    mock_exit.assert_called_once_with(1)


def test_run_pipeline_exits_1_on_failure():
    """run_pipeline calls sys.exit(1) when any stage fails."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['fail'])):
        with patch('sys.exit') as mock_exit:
            run_pipeline(['mdb_to_bronze'], config, engine)

    mock_exit.assert_called_once_with(1)
