import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from stages.reconcile_silver import ReconcileSilver


def _make_config():
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {'dedup_key': 'uniqueid', 'country_code_map': {'kenya': 2}},
    }.get(key, default)
    return config


def test_reports_no_drift_when_counts_and_checksum_match():
    engine = MagicMock()
    conn = MagicMock()
    # 4 scalar() calls per table (live_count, shadow_count, live_checksum,
    # shadow_checksum) x 2 tables (baseline, followup) = 8 values. Each
    # table's live/shadow pair matches -> no drift.
    conn.execute.return_value.scalar.side_effect = [2, 2, 'abc123', 'abc123', 2, 2, 'abc123', 'abc123']
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    bronze_df = pd.DataFrame({
        'uniqueid': ['a', 'b'], 'countrycode': [2, 2], 'country': ['kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })

    with patch('stages.reconcile_silver.pd.read_sql', return_value=bronze_df), \
         patch.object(pd.DataFrame, 'to_sql'):
        stage = ReconcileSilver(config=_make_config(), engine=engine)
        result = stage.run()

    assert result.success
    assert result.metadata['drifted_tables'] == []


def test_reports_drift_when_row_counts_differ():
    engine = MagicMock()
    conn = MagicMock()
    # live count=1, shadow(rebuilt) count=2 -> mismatch for baseline; then followup matches
    conn.execute.return_value.scalar.side_effect = [1, 'abc', 2, 'abc', 0, 'x', 0, 'x']
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    bronze_df = pd.DataFrame({
        'uniqueid': ['a', 'b'], 'countrycode': [2, 2], 'country': ['kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })

    with patch('stages.reconcile_silver.pd.read_sql', return_value=bronze_df), \
         patch.object(pd.DataFrame, 'to_sql'):
        stage = ReconcileSilver(config=_make_config(), engine=engine)
        result = stage.run()

    assert not result.success
    assert 'baseline' in result.metadata['drifted_tables']


def test_check_table_rejects_invalid_table_name():
    """table_name and the derived shadow_table are interpolated directly
    into raw SQL strings — an unvalidated identifier could break out of
    quoting. _check_table must validate table_name before building any SQL,
    matching the allow-listing pattern used elsewhere in the project (see
    stages/promote_ibis.py's _validate_table_name and
    modules/incremental_writer.py)."""
    engine = MagicMock()
    conn = MagicMock()
    stage = ReconcileSilver(config=_make_config(), engine=engine)

    with pytest.raises(ValueError):
        stage._check_table(conn, 'baseline; DROP TABLE x', 'uniqueid', {'kenya': 2})
