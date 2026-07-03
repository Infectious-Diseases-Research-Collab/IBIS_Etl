from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from modules.incremental_writer import append_history, ensure_current_table, upsert_latest


def test_append_history_appends_without_replace():
    conn = MagicMock()
    df = pd.DataFrame({'uniqueid': ['a'], 'extracted_at': ['2026-01-01']})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        append_history(conn, df, 'silver_ibis', 'baseline_history')

    mock_to_sql.assert_called_once_with(
        'baseline_history', conn, schema='silver_ibis', if_exists='append', index=False
    )


def test_ensure_current_table_creates_like_history_and_adds_constraint_when_absent():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = None  # constraint does not exist yet

    ensure_current_table(conn, 'silver_ibis', 'baseline_history', 'baseline', 'uniqueid')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('CREATE TABLE IF NOT EXISTS silver_ibis."baseline"' in s and 'LIKE' in s for s in executed_sql)
    assert any('ADD COLUMN IF NOT EXISTS updated_at' in s for s in executed_sql)
    assert any('ADD CONSTRAINT "baseline_uniqueid_key" UNIQUE ("uniqueid")' in s for s in executed_sql)


def test_ensure_current_table_skips_constraint_when_already_present():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 1  # constraint already exists

    ensure_current_table(conn, 'silver_ibis', 'baseline_history', 'baseline', 'uniqueid')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert not any('ADD CONSTRAINT' in s for s in executed_sql)


def test_ensure_current_table_constraint_check_is_schema_scoped():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = None

    ensure_current_table(conn, 'silver_ibis', 'baseline_history', 'baseline', 'uniqueid')

    # Find the call that runs the constraint-existence check.
    constraint_check_calls = [
        c for c in conn.execute.call_args_list
        if 'pg_constraint' in str(c.args[0])
    ]
    assert len(constraint_check_calls) == 1
    call = constraint_check_calls[0]
    executed_sql = str(call.args[0])
    params = call.args[1] if len(call.args) > 1 else call.kwargs.get('parameters', {})

    assert 'pg_namespace' in executed_sql
    assert params.get('schema') == 'silver_ibis'
    assert params.get('name') == 'baseline_uniqueid_key'


@pytest.mark.parametrize('bad_name', ['bad;name', '1bad', 'DROP TABLE', 'ok-name', ''])
def test_append_history_rejects_invalid_identifiers(bad_name):
    conn = MagicMock()
    df = pd.DataFrame({'uniqueid': ['a']})

    with pytest.raises(ValueError):
        append_history(conn, df, bad_name, 'baseline_history')

    with pytest.raises(ValueError):
        append_history(conn, df, 'silver_ibis', bad_name)


@pytest.mark.parametrize('bad_name', ['bad;name', '1bad', 'DROP TABLE', 'ok-name', ''])
def test_ensure_current_table_rejects_invalid_identifiers(bad_name):
    conn = MagicMock()

    with pytest.raises(ValueError):
        ensure_current_table(conn, bad_name, 'baseline_history', 'baseline', 'uniqueid')

    with pytest.raises(ValueError):
        ensure_current_table(conn, 'silver_ibis', bad_name, 'baseline', 'uniqueid')

    with pytest.raises(ValueError):
        ensure_current_table(conn, 'silver_ibis', 'baseline_history', bad_name, 'uniqueid')

    with pytest.raises(ValueError):
        ensure_current_table(conn, 'silver_ibis', 'baseline_history', 'baseline', bad_name)


@pytest.mark.parametrize('bad_name', ['bad;name', '1bad', 'DROP TABLE', 'ok-name', ''])
def test_upsert_latest_rejects_invalid_identifiers(bad_name):
    conn = MagicMock()
    df = pd.DataFrame({'uniqueid': ['a'], 'extracted_at': ['2026-01-01'], 'name': ['Alice']})

    with pytest.raises(ValueError):
        upsert_latest(conn, df, bad_name, 'baseline', 'uniqueid', 'extracted_at')

    with pytest.raises(ValueError):
        upsert_latest(conn, df, 'silver_ibis', bad_name, 'uniqueid', 'extracted_at')

    with pytest.raises(ValueError):
        upsert_latest(conn, df, 'silver_ibis', 'baseline', bad_name, 'extracted_at')

    with pytest.raises(ValueError):
        upsert_latest(conn, df, 'silver_ibis', 'baseline', 'uniqueid', bad_name)


def test_upsert_latest_stages_then_upserts_with_conflict_clause():
    conn = MagicMock()
    df = pd.DataFrame({'uniqueid': ['a'], 'extracted_at': ['2026-01-01'], 'name': ['Alice']})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        upsert_latest(conn, df, 'silver_ibis', 'baseline', 'uniqueid', 'extracted_at')

    # staged into a throwaway table first
    mock_to_sql.assert_called_once_with(
        '_stage_baseline', conn, schema='silver_ibis', if_exists='replace', index=False
    )
    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('ON CONFLICT ("uniqueid") DO UPDATE' in s for s in executed_sql)
    assert any('excluded."extracted_at" > silver_ibis."baseline"."extracted_at"' in s for s in executed_sql)
    assert any('DROP TABLE silver_ibis."_stage_baseline"' in s for s in executed_sql)


def test_upsert_latest_is_noop_on_empty_dataframe():
    conn = MagicMock()
    df = pd.DataFrame(columns=['uniqueid', 'extracted_at'])

    upsert_latest(conn, df, 'silver_ibis', 'baseline', 'uniqueid', 'extracted_at')

    conn.execute.assert_not_called()
