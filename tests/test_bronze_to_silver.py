import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from stages.bronze_to_silver import BronzeToSilver


def _make_config(dedup_key='uniqueid', strategy='latest_snapshot'):
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'trial': {
            'dedup_key': dedup_key,
            'dedup_strategy': strategy,
            'country_code_map': {'kenya': 2, 'uganda': 1},
        },
    }.get(key, default)
    return config


def test_bronze_to_silver_deduplicates():
    """Stage deduplicates bronze rows and writes to silver."""
    raw = pd.DataFrame({
        'uniqueid': ['a', 'a', 'b'],
        'countrycode': [2, 2, 2],
        '_source_db': ['x', 'x', 'y'],
        'run_uuid': ['r1', 'r1', 'r2'],
        'file_name': ['f1', 'f1', 'f2'],
        'file_path': ['p1', 'p1', 'p2'],
        'country': ['kenya', 'kenya', 'kenya'],
        'community': ['Sindo', 'Sindo', 'Sindo'],
        'extracted_at': [None, None, None],
    })

    engine = MagicMock()

    # patch DataFrame.to_sql to a no-op (avoids needing a real DB)
    with patch.object(pd.DataFrame, 'to_sql'):
        with patch('stages.bronze_to_silver.pd.read_sql', return_value=raw):
            stage = BronzeToSilver(config=_make_config(), engine=engine)
            result = stage.run()

    assert result.success
    # 2 unique uniqueid values → 2 rows written
    assert result.rows_written == 2
