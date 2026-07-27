import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from stages.measures_ibis import MeasuresIbis


def test_measures_ibis_writes_validator_report(monkeypatch):
    silver_df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'countrycode': [2, 2],
        'tabletnum': [221, 221],
        'screening_id': ['KE001', 'KE002'],
        'starttime': [None, None],
        'stoptime': [None, None],
        'client_sex': [1, 1],
        'health_facility': ['HF1', 'HF1'],
        'country': ['kenya', 'kenya'],
    })

    report_df = pd.DataFrame([{
        'check': 'missing_required',
        'severity': 'WARNING',
        'field': 'starttime',
        'record_count': 2,
        'detail': 'starttime missing',
        'affected_subjids': '',
    }])

    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'country_code_map': {'kenya': 2}},
    }.get(key, default)

    written = {}

    def fake_to_sql(df_self, name, eng=None, schema=None, if_exists='append', index=True):
        written[f"{schema}.{name}"] = True

    with patch('pandas.DataFrame.to_sql', fake_to_sql):
        with patch('stages.measures_ibis.pd.read_sql', return_value=silver_df):
            with patch('stages.measures_ibis.DataValidator') as MockValidator:
                MockValidator.return_value.validate.return_value = report_df
                with patch('stages.measures_ibis.SQL_MEASURES_DIR', '/nonexistent'):
                    mock_sql_path = MagicMock()
                    mock_sql_path.read_text.return_value = 'SELECT 1;'
                    mock_sql_path.name = 'test.sql'
                    with patch('stages.measures_ibis._load_sql_files', return_value=[mock_sql_path]):
                        stage = MeasuresIbis(config=config, engine=engine)
                        result = stage.run()

    assert result.success
    assert 'gold_ibis.ds_validation_report' in written


def test_measures_ibis_reports_formatted_error_on_sql_failure():
    """
    A SQL error while running sql/measures/*.sql must produce the formatted
    "SQL error in '<file>'" message in result.errors (and still report
    rows_written for the validation report already written) — not be
    silently discarded by the collect-then-raise pattern this replaced.
    """
    silver_df = pd.DataFrame({
        'uniqueid': ['a'],
        'countrycode': [2],
        'tabletnum': [221],
        'screening_id': ['KE001'],
        'starttime': [None],
        'stoptime': [None],
        'client_sex': [1],
        'health_facility': ['HF1'],
        'country': ['kenya'],
    })
    report_df = pd.DataFrame([{
        'check': 'missing_required', 'severity': 'WARNING', 'field': 'starttime',
        'record_count': 1, 'detail': 'starttime missing', 'affected_subjids': '',
    }])

    engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception('relation does not exist')
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'country_code_map': {'kenya': 2}},
    }.get(key, default)

    def fake_to_sql(df_self, name, eng=None, schema=None, if_exists='append', index=True):
        pass

    mock_sql_path = MagicMock()
    mock_sql_path.read_text.return_value = 'SELECT 1;'
    mock_sql_path.name = 'qc_checks.sql'

    with patch('pandas.DataFrame.to_sql', fake_to_sql):
        with patch('stages.measures_ibis.pd.read_sql', return_value=silver_df):
            with patch('stages.measures_ibis.DataValidator') as MockValidator:
                MockValidator.return_value.validate.return_value = report_df
                with patch('stages.measures_ibis._load_sql_files', return_value=[mock_sql_path]):
                    stage = MeasuresIbis(config=config, engine=engine)
                    result = stage.run()

    assert not result.success
    assert result.rows_written == len(report_df)
    assert len(result.errors) == 1
    assert "SQL error in 'qc_checks.sql'" in result.errors[0]
    assert 'relation does not exist' in result.errors[0]


def test_measures_ibis_warns_on_missing_country_code():
    """When a country has no entry in country_code_map, a warning is logged and validation continues."""
    silver_df = pd.DataFrame({
        'uniqueid': ['a'],
        'countrycode': [9],
        'tabletnum': [100],
        'screening_id': ['XX001'],
        'starttime': [None],
        'stoptime': [None],
        'client_sex': [1],
        'health_facility': ['HF1'],
        'country': ['unknown_country'],
    })

    report_df = pd.DataFrame([{
        'check': 'test_check',
        'severity': 'WARNING',
        'field': 'countrycode',
        'record_count': 1,
        'detail': 'unknown country',
        'affected_subjids': '',
    }])

    engine = MagicMock()
    config = MagicMock()
    # country_code_map is empty — 'unknown_country' will not be found
    config.get.side_effect = lambda key, default=None: {
        'trial': {'country_code_map': {}},
    }.get(key, default)

    written = {}

    def fake_to_sql(df_self, name, eng=None, schema=None, if_exists='append', index=True):
        written[f"{schema}.{name}"] = True

    with patch('pandas.DataFrame.to_sql', fake_to_sql):
        with patch('stages.measures_ibis.pd.read_sql', return_value=silver_df):
            with patch('stages.measures_ibis.DataValidator') as MockValidator:
                MockValidator.return_value.validate.return_value = report_df
                with patch('stages.measures_ibis._load_sql_files', return_value=[]):
                    stage = MeasuresIbis(config=config, engine=engine)
                    result = stage.run()

    # Validation ran (MockValidator was called) and report was written
    MockValidator.return_value.validate.assert_called()
    call_kwargs = MockValidator.return_value.validate.call_args
    # country_code should be None (not found in map)
    assert call_kwargs.kwargs.get('country_code') is None or call_kwargs[1].get('country_code') is None
    assert 'gold_ibis.ds_validation_report' in written


def test_measures_ibis_runs_followup_stale_check_and_appends_report():
    baseline_df = pd.DataFrame({
        'uniqueid': ['a'],
        'countrycode': [2],
        'tabletnum': ['221'],
        'screening_id': ['KE001'],
        'starttime': [None],
        'stoptime': [None],
        'client_sex': [1],
        'health_facility': ['HF1'],
        'country': ['kenya'],
    })
    followup_df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'tabletnum': ['221', '221'],
        'country': ['kenya', 'kenya'],
    })
    baseline_report = pd.DataFrame([{
        'check': 'missing_required', 'severity': 'WARNING', 'field': 'starttime',
        'record_count': 1, 'detail': 'starttime missing', 'affected_subjids': '',
        'country': 'kenya', 'site': '', 'affected_tablets': '',
    }])
    followup_stale_report = pd.DataFrame([{
        'check': 'stale_record_missing_from_tablet', 'severity': 'WARNING', 'field': 'uniqueid',
        'record_count': 1, 'detail': 'stale', 'affected_subjids': 'b', 'affected_tablets': '221',
        'country': 'kenya', 'site': '',
    }])

    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'country_code_map': {'kenya': 2}},
    }.get(key, default)

    written = {}
    captured = {}

    def fake_to_sql(df_self, name, eng=None, schema=None, if_exists='append', index=True):
        written[f"{schema}.{name}"] = True
        if name == 'ds_validation_report':
            captured['df'] = df_self

    def fake_read_sql(sql, conn_or_engine, **kwargs):
        sql_str = str(sql)
        if 'silver_ibis.followup' in sql_str:
            return followup_df
        return baseline_df

    with patch('pandas.DataFrame.to_sql', fake_to_sql):
        with patch('stages.measures_ibis.pd.read_sql', side_effect=fake_read_sql):
            with patch('stages.measures_ibis.find_stale_uniqueids', return_value={'b'}):
                with patch('stages.measures_ibis.DataValidator') as MockValidator:
                    MockValidator.return_value.validate.return_value = baseline_report
                    MockValidator.return_value.validate_stale_records.return_value = followup_stale_report
                    with patch('stages.measures_ibis.SQL_MEASURES_DIR', '/nonexistent'):
                        mock_sql_path = MagicMock()
                        mock_sql_path.read_text.return_value = 'SELECT 1;'
                        mock_sql_path.name = 'test.sql'
                        with patch('stages.measures_ibis._load_sql_files', return_value=[mock_sql_path]):
                            stage = MeasuresIbis(config=config, engine=engine)
                            result = stage.run()

    assert result.success
    MockValidator.return_value.validate_stale_records.assert_called()
    assert 'gold_ibis.ds_validation_report' in written
    assert 'stale_record_missing_from_tablet' in captured['df']['check'].values


def test_measures_ibis_writes_baseline_report_even_if_followup_read_fails():
    """
    If reading silver_ibis.followup raises (transient DB error, table
    renamed, connection drop), the already-computed baseline validation
    report must still be written and the failure must surface via
    result.errors rather than propagating out of run() uncaught.
    """
    baseline_df = pd.DataFrame({
        'uniqueid': ['a'],
        'countrycode': [2],
        'tabletnum': ['221'],
        'screening_id': ['KE001'],
        'starttime': [None],
        'stoptime': [None],
        'client_sex': [1],
        'health_facility': ['HF1'],
        'country': ['kenya'],
    })
    baseline_report = pd.DataFrame([{
        'check': 'missing_required', 'severity': 'WARNING', 'field': 'starttime',
        'record_count': 1, 'detail': 'starttime missing', 'affected_subjids': '',
        'country': 'kenya', 'site': '', 'affected_tablets': '',
    }])

    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'country_code_map': {'kenya': 2}},
    }.get(key, default)

    written = {}
    captured = {}

    def fake_to_sql(df_self, name, eng=None, schema=None, if_exists='append', index=True):
        written[f"{schema}.{name}"] = True
        if name == 'ds_validation_report':
            captured['df'] = df_self

    def fake_read_sql(sql, conn_or_engine, **kwargs):
        sql_str = str(sql)
        if 'silver_ibis.followup' in sql_str:
            raise Exception('relation "silver_ibis.followup" does not exist')
        return baseline_df

    with patch('pandas.DataFrame.to_sql', fake_to_sql):
        with patch('stages.measures_ibis.pd.read_sql', side_effect=fake_read_sql):
            with patch('stages.measures_ibis.find_stale_uniqueids', return_value=set()):
                with patch('stages.measures_ibis.DataValidator') as MockValidator:
                    MockValidator.return_value.validate.return_value = baseline_report
                    with patch('stages.measures_ibis.SQL_MEASURES_DIR', '/nonexistent'):
                        mock_sql_path = MagicMock()
                        mock_sql_path.read_text.return_value = 'SELECT 1;'
                        mock_sql_path.name = 'test.sql'
                        with patch('stages.measures_ibis._load_sql_files', return_value=[mock_sql_path]):
                            stage = MeasuresIbis(config=config, engine=engine)
                            result = stage.run()

    assert not result.success
    assert any('silver_ibis.followup' in e for e in result.errors)
    assert 'gold_ibis.ds_validation_report' in written
    assert 'missing_required' in captured['df']['check'].values
