import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from stages.bronze_to_silver import BronzeToSilver


def _make_config(dedup_key='uniqueid'):
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'dedup_key': dedup_key, 'country_code_map': {'kenya': 2, 'uganda': 1}},
    }.get(key, default)
    return config


def _make_conn_with_meta(meta_rows):
    """meta_rows: list of run_uuid strings considered 'new' (unpromoted)."""
    conn = MagicMock()

    def fake_execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if 'FROM bronze_ibis.meta' in sql and 'promoted_to_silver_at IS NULL' in sql:
            result.fetchall.return_value = [(u,) for u in meta_rows]
        else:
            result.scalar.return_value = 1  # constraint checks etc: treat as already present
        return result

    conn.execute.side_effect = fake_execute
    return conn


def test_no_new_files_skips_processing_entirely():
    engine = MagicMock()
    conn = _make_conn_with_meta([])  # nothing unpromoted
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with patch('stages.bronze_to_silver.pd.read_sql') as mock_read_sql:
        stage = BronzeToSilver(config=_make_config(), engine=engine)
        result = stage.run()

    mock_read_sql.assert_not_called()
    assert result.success
    assert result.rows_written == 0


def test_new_files_are_cleaned_and_written_then_marked_promoted():
    engine = MagicMock()
    conn = _make_conn_with_meta(['run-1'])
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    bronze_df = pd.DataFrame({
        'uniqueid': ['a'], 'countrycode': [2], 'country': ['kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01']), 'run_uuid': ['run-1'],
    })
    followup_df = pd.DataFrame(columns=bronze_df.columns)

    def fake_read_sql(clause, conn_arg, params=None):
        return followup_df if 'followup' in str(clause) else bronze_df

    with patch('stages.bronze_to_silver.pd.read_sql', side_effect=fake_read_sql), \
         patch('stages.bronze_to_silver.append_history') as mock_append, \
         patch('stages.bronze_to_silver.ensure_current_table') as mock_ensure, \
         patch('stages.bronze_to_silver.upsert_latest') as mock_upsert:
        stage = BronzeToSilver(config=_make_config(), engine=engine)
        result = stage.run()

    assert result.success
    assert result.rows_written == 1
    mock_append.assert_called_once()
    mock_ensure.assert_called_once()
    mock_upsert.assert_called_once()

    update_calls = [
        c for c in conn.execute.call_args_list
        if 'UPDATE bronze_ibis.meta SET promoted_to_silver_at' in str(c.args[0])
        and c.args[1].get('tn') == 'baseline'
    ]
    assert len(update_calls) == 1
    # Bound params, not just SQL text — this is what actually determines
    # which files get marked done, so it must be checked directly.
    assert update_calls[0].args[1] == {'uuids': ['run-1'], 'tn': 'baseline'}


def test_partial_country_failure_does_not_promote_failed_country_run_uuid():
    """If one country's cleaning fails within a batch spanning multiple
    countries, only the succeeding country's run_uuid may be marked
    promoted — the failing one must stay unpromoted so it's retried on the
    next run, not silently and permanently lost."""
    engine = MagicMock()
    conn = _make_conn_with_meta(['run-broken', 'run-ok'])
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    bronze_df = pd.DataFrame({
        'uniqueid': ['a', 'b'], 'countrycode': [2, 2], 'country': ['broken', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
        'run_uuid': ['run-broken', 'run-ok'],
    })
    cleaned = bronze_df.iloc[[1]]  # only the 'kenya'/'run-ok' row survived cleaning

    with patch('stages.bronze_to_silver.pd.read_sql', return_value=bronze_df), \
         patch('stages.bronze_to_silver.clean_full_history',
               return_value=(cleaned, ["[broken] Failed during cleaning: boom"], {'broken'})), \
         patch('stages.bronze_to_silver.append_history'), \
         patch('stages.bronze_to_silver.ensure_current_table'), \
         patch('stages.bronze_to_silver.upsert_latest'):
        stage = BronzeToSilver(config=_make_config(), engine=engine)
        result = stage.run()

    assert not result.success  # the collected error must surface
    update_calls = [
        c for c in conn.execute.call_args_list
        if 'UPDATE bronze_ibis.meta SET promoted_to_silver_at' in str(c.args[0])
    ]
    assert update_calls  # one per table (baseline, followup) — both use the same mocks here
    for call in update_calls:
        assert call.args[1]['uuids'] == ['run-ok']
        assert 'run-broken' not in call.args[1]['uuids']


def test_processes_both_baseline_and_followup_in_one_transaction():
    engine = MagicMock()
    conn = _make_conn_with_meta(['run-1'])
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    row = pd.DataFrame({
        'uniqueid': ['a'], 'countrycode': [2], 'country': ['kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01']), 'run_uuid': ['run-1'],
    })

    with patch('stages.bronze_to_silver.pd.read_sql', return_value=row), \
         patch('stages.bronze_to_silver.append_history'), \
         patch('stages.bronze_to_silver.ensure_current_table'), \
         patch('stages.bronze_to_silver.upsert_latest'):
        stage = BronzeToSilver(config=_make_config(), engine=engine)
        result = stage.run()

    assert result.success
    assert result.rows_written == 2  # one row promoted for baseline, one for followup
    engine.begin.assert_called_once()


def test_full_rebuild_bypasses_meta_filter_and_rewrites_current_table():
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 1
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    row = pd.DataFrame({
        'uniqueid': ['a'], 'countrycode': [2], 'country': ['kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01']),
    })

    with patch('stages.bronze_to_silver.pd.read_sql', return_value=row), \
         patch('stages.bronze_to_silver.clean_full_history', return_value=(row, [], set())) as mock_clean, \
         patch.object(pd.DataFrame, 'to_sql'):
        stage = BronzeToSilver(config=_make_config(), engine=engine)
        result = stage.run(full_rebuild=True)

    assert result.success
    mock_clean.assert_called()
    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    # Must target the live current table specifically — a regression that
    # truncated the *_history table instead (or in addition) would destroy
    # permanent data and must not pass this test.
    truncate_calls = [s for s in executed_sql if 'TRUNCATE' in s]
    assert truncate_calls
    assert all('_history' not in s for s in truncate_calls)
    assert any('TRUNCATE silver_ibis."baseline"' in s or 'TRUNCATE silver_ibis."followup"' in s
               for s in truncate_calls)
