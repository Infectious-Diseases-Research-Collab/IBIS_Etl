import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import subprocess

from modules.access_reader import read_mdb_table, list_mdb_tables, AccessReader, select_latest_per_tablet


def test_read_mdb_table_success():
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "id,name,age\n1,Alice,30\n2,Bob,25\n"
    with patch('modules.access_reader.subprocess.run', return_value=mock_result):
        df = read_mdb_table('/fake/path.mdb', 'baseline')
    assert len(df) == 2
    assert list(df.columns) == ['id', 'name', 'age']


def test_read_mdb_table_failure_raises():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "Cannot open file"
    with patch('modules.access_reader.subprocess.run', return_value=mock_result):
        with pytest.raises(RuntimeError, match="mdb-export failed"):
            read_mdb_table('/fake/path.mdb', 'baseline')


def test_read_mdb_table_timeout_raises():
    with patch('modules.access_reader.subprocess.run',
               side_effect=subprocess.TimeoutExpired(cmd='mdb-export', timeout=60)):
        with pytest.raises(RuntimeError, match="timed out"):
            read_mdb_table('/fake/path.mdb', 'baseline')


def test_list_mdb_tables_success():
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "baseline\nother_table\n"
    with patch('modules.access_reader.subprocess.run', return_value=mock_result):
        tables = list_mdb_tables('/fake/path.mdb')
    assert tables == ['baseline', 'other_table']


def test_list_mdb_tables_failure_raises():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "bad file"
    with patch('modules.access_reader.subprocess.run', return_value=mock_result):
        with pytest.raises(RuntimeError, match="mdb-tables failed"):
            list_mdb_tables('/fake/path.mdb')


def test_list_mdb_tables_timeout_raises():
    with patch('modules.access_reader.subprocess.run',
               side_effect=subprocess.TimeoutExpired(cmd='mdb-tables', timeout=10)):
        with pytest.raises(RuntimeError, match="timed out"):
            list_mdb_tables('/fake/path.mdb')


def test_select_latest_per_tablet_picks_newest():
    """For the same tablet, only the most recent snapshot is returned."""
    older = '/root/Tablet221_2026_02_01-10_00_00/ibis_pilot.mdb'
    newer = '/root/Tablet221_2026_04_01-10_00_00/ibis_pilot.mdb'
    result = select_latest_per_tablet([older, newer])
    assert result == [newer]


def test_select_latest_per_tablet_excludes_databackup():
    """DataBackup paths are always excluded."""
    backup = '/root/Tablet221_2026_04_01-10_00_00/DataBackup/ibis_pilot.mdb'
    normal = '/root/Tablet222_2026_04_01-10_00_00/ibis_pilot.mdb'
    result = select_latest_per_tablet([backup, normal])
    assert result == [normal]


def test_select_latest_per_tablet_excluded_tablet_skipped():
    """Tablets in the excluded list are skipped entirely."""
    path = '/root/Tablet111_2026_04_01-10_00_00/ibis_pilot.mdb'
    result = select_latest_per_tablet([path], excluded_tablets=['Tablet111'])
    assert result == []


def test_access_reader_read_all_databases_calls_read_mdb_table(tmp_path):
    # Create a fake tablet snapshot directory structure
    tablet_dir = tmp_path / "Tablet221_2026_02_25-16_38_31"
    tablet_dir.mkdir()
    mdb = tablet_dir / "ibis_pilot.mdb"
    mdb.write_bytes(b"")

    mock_df = pd.DataFrame({'uniqueid': ['a', 'b'], 'countrycode': [2, 2]})

    with patch('modules.access_reader.read_mdb_table', return_value=mock_df):
        reader = AccessReader(table_name='baseline')
        combined, failures, schema_issues = reader.read_all_databases(str(tmp_path))

    assert len(combined) == 2
    assert failures == []


def test_mdb_to_bronze_skips_already_loaded():
    """Stage skips a file whose path+last_modified is already in bronze meta."""
    from unittest.mock import MagicMock, patch
    from stages.mdb_to_bronze import MdbToBronze

    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'communities': {'k': {'community_name': 'Sindo', 'country': 'kenya'}},
        'trial': {'country_code_map': {'kenya': 2}},
        'access_table_name': 'baseline',
        'excluded_tablets': [],
    }.get(key, default)

    engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = MagicMock(loaded=True)
    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    stage = MdbToBronze(config=config, engine=engine)

    with patch('stages.mdb_to_bronze.select_latest_per_tablet',
               return_value=['/fake/Tablet221_2026_02_25-16_38_31/ibis_pilot.mdb']):
        with patch('stages.mdb_to_bronze.glob_module.glob', return_value=['/fake/Tablet221_2026_02_25-16_38_31/ibis_pilot.mdb']):
            with patch('os.path.getmtime', return_value=1000000.0):
                result = stage.run()

    assert result.rows_written == 0
    assert result.success
