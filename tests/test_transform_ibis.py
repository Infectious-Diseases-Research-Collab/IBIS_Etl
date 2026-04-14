import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path

from stages.transform_ibis import TransformIbis, _load_sql_files


def test_load_sql_files_returns_sorted_paths(tmp_path):
    (tmp_path / 'b_second.sql').write_text('SELECT 2')
    (tmp_path / 'a_first.sql').write_text('SELECT 1')
    files = _load_sql_files(str(tmp_path))
    assert [f.name for f in files] == ['a_first.sql', 'b_second.sql']


def test_transform_ibis_executes_all_sql_files(tmp_path):
    (tmp_path / 'd_participant.sql').write_text('CREATE TABLE gold_ibis.d_participant AS SELECT 1')

    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    config.get.return_value = None

    stage = TransformIbis(config=config, engine=engine)

    with patch('stages.transform_ibis.SQL_TRANSFORM_DIR', str(tmp_path)):
        result = stage.run()

    assert result.success
    assert mock_conn.execute.called


def test_transform_ibis_raises_on_sql_error(tmp_path):
    (tmp_path / 'bad.sql').write_text('NOT VALID SQL')

    engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("SQL syntax error")
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    stage = TransformIbis(config=config, engine=engine)

    with patch('stages.transform_ibis.SQL_TRANSFORM_DIR', str(tmp_path)):
        with pytest.raises(Exception, match="SQL syntax error"):
            stage.run()
