import pandas as pd
from unittest.mock import MagicMock, patch

from stages.mdb_to_bronze import MdbToBronze, _ingest_file, _process_mdb_file


def _make_config(access_table_name='baseline'):
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'communities': {
            'ug1': {'country': 'uganda', 'community_name': 'Mbarara'},
        },
        'trial': {'country_code_map': {'uganda': 1}},
        'access_table_name': access_table_name,
        'excluded_tablets': [],
    }.get(key, default)
    return config


def _mock_engine_for_ingest():
    """Engine whose meta-check and column-lookup queries both raise
    ProgrammingError (bronze_ibis tables don't exist yet)."""
    from sqlalchemy.exc import ProgrammingError
    engine = MagicMock()

    connect_ctx = MagicMock()
    connect_ctx.__enter__ = MagicMock(return_value=MagicMock(
        execute=MagicMock(side_effect=ProgrammingError('', {}, Exception()))
    ))
    connect_ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = connect_ctx

    begin_ctx = MagicMock()
    begin_conn = MagicMock()
    begin_ctx.__enter__ = MagicMock(return_value=begin_conn)
    begin_ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_ctx

    return engine


# ---------------------------------------------------------------------------
# _ingest_file (module-level function; was a bound method before this task)
# ---------------------------------------------------------------------------

def test_ingest_file_uses_table_name_in_to_sql():
    """_ingest_file writes to bronze_ibis.<table_name>, not hardcoded 'baseline'."""
    engine = _mock_engine_for_ingest()
    raw = pd.DataFrame({'uniqueid': ['x'], 'subjid': ['s1']})

    with patch('stages.mdb_to_bronze.read_mdb_table', return_value=raw), \
         patch('os.path.getmtime', return_value=0.0), \
         patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        _ingest_file(engine, '/fake/tablet.mdb', 'followup', 'uganda', 'Mbarara')

    # First to_sql call is for the data; second is for meta
    data_call = mock_to_sql.call_args_list[0]
    assert data_call.args[0] == 'followup'
    assert data_call.kwargs.get('schema') == 'bronze_ibis'


def test_ingest_file_writes_table_name_to_meta():
    """Meta row includes table_name so baseline and followup are distinguished."""
    engine = _mock_engine_for_ingest()
    raw = pd.DataFrame({'uniqueid': ['x']})
    captured = {}

    def patched_to_sql(self, name, conn, schema=None, **kwargs):
        if name == 'meta':
            captured['meta_df'] = self.copy()

    with patch('stages.mdb_to_bronze.read_mdb_table', return_value=raw), \
         patch('os.path.getmtime', return_value=0.0), \
         patch.object(pd.DataFrame, 'to_sql', patched_to_sql):
        _ingest_file(engine, '/fake/tablet.mdb', 'followup', 'uganda', 'Mbarara')

    assert 'table_name' in captured['meta_df'].columns
    assert captured['meta_df']['table_name'].iloc[0] == 'followup'


# ---------------------------------------------------------------------------
# _process_mdb_file (new: one file's full lifecycle — ingest, quarantine, followup)
# ---------------------------------------------------------------------------

def test_process_mdb_file_ingests_baseline_and_followup():
    engine = MagicMock()
    ingested = []

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        ingested.append(table_name)
        return 1

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=['baseline', 'followup']):
        rows, errors = _process_mdb_file(
            engine, '/fake/t1.mdb', 'baseline', 'uganda', 'Mbarara', '/fake',
        )

    assert ingested == ['baseline', 'followup']
    assert rows == 2
    assert errors == []


def test_process_mdb_file_skips_followup_when_table_absent():
    engine = MagicMock()
    ingested = []

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        ingested.append(table_name)
        return 1

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=['baseline']):
        rows, errors = _process_mdb_file(
            engine, '/fake/t1.mdb', 'baseline', 'uganda', 'Mbarara', '/fake',
        )

    assert ingested == ['baseline']
    assert rows == 1
    assert errors == []


def test_process_mdb_file_continues_when_list_mdb_tables_raises():
    engine = MagicMock()
    ingested = []

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        ingested.append(table_name)
        return 1

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables',
               side_effect=RuntimeError('mdb-tables failed')):
        rows, errors = _process_mdb_file(
            engine, '/fake/t1.mdb', 'baseline', 'uganda', 'Mbarara', '/fake',
        )

    assert ingested == ['baseline']
    assert rows == 1
    assert errors == []


def test_process_mdb_file_quarantines_corrupt_mdb():
    engine = MagicMock()

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        raise RuntimeError("mdb-export failed for 'IBIS_pilot.mdb': offset 4096 is beyond EOF")

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=[]), \
         patch('stages.mdb_to_bronze.os.makedirs'), \
         patch('stages.mdb_to_bronze.shutil') as mock_shutil:
        rows, errors = _process_mdb_file(
            engine,
            '/fake/Extracted/Uganda/Tablet53_2026_05_28/IBIS_pilot.mdb',
            'baseline', 'uganda', 'Mbarara', '/fake/Extracted/Uganda',
        )

    assert rows == 0
    assert errors == []  # a successful quarantine is a warning, not an error
    mock_shutil.move.assert_called_once_with(
        '/fake/Extracted/Uganda/Tablet53_2026_05_28',
        '/fake/Extracted/Uganda/Quarantine/Tablet53_2026_05_28',
    )


def test_process_mdb_file_quarantines_with_unique_dest_on_collision():
    """If a folder with the same name is already in Quarantine/, the move
    must still succeed — using a timestamp-suffixed destination — rather
    than raising and leaving the corrupt file to be retried forever."""
    engine = MagicMock()

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        raise RuntimeError("mdb-export failed: offset 4096 is beyond EOF")

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=[]), \
         patch('stages.mdb_to_bronze.os.makedirs'), \
         patch('stages.mdb_to_bronze.os.path.exists', return_value=True), \
         patch('stages.mdb_to_bronze.shutil') as mock_shutil:
        rows, errors = _process_mdb_file(
            engine,
            '/fake/Extracted/Uganda/Tablet53_2026_05_28/IBIS_pilot.mdb',
            'baseline', 'uganda', 'Mbarara', '/fake/Extracted/Uganda',
        )

    assert errors == []
    mock_shutil.move.assert_called_once()
    src, dest = mock_shutil.move.call_args[0]
    assert src == '/fake/Extracted/Uganda/Tablet53_2026_05_28'
    assert dest != '/fake/Extracted/Uganda/Quarantine/Tablet53_2026_05_28'
    assert dest.startswith('/fake/Extracted/Uganda/Quarantine/Tablet53_2026_05_28_')


def test_process_mdb_file_reports_error_when_quarantine_move_fails():
    """If shutil.move itself raises even after collision handling, this
    must surface as an error — not just a log line — since the corrupt
    file is left in the active tree and will be retried on every future run."""
    engine = MagicMock()

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        raise RuntimeError("mdb-export failed: offset 4096 is beyond EOF")

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=[]), \
         patch('stages.mdb_to_bronze.os.makedirs'), \
         patch('stages.mdb_to_bronze.shutil') as mock_shutil:
        mock_shutil.move.side_effect = OSError('disk full')
        rows, errors = _process_mdb_file(
            engine,
            '/fake/Extracted/Uganda/Tablet53_2026_05_28/IBIS_pilot.mdb',
            'baseline', 'uganda', 'Mbarara', '/fake/Extracted/Uganda',
        )

    assert rows == 0
    assert any('disk full' in e for e in errors)


def test_process_mdb_file_reports_error_when_followup_ingest_fails():
    engine = MagicMock()

    def fake_ingest(engine_arg, db_path, table_name, country, community):
        if table_name == 'followup':
            raise RuntimeError('mdb-export failed for followup')
        return 1

    with patch('stages.mdb_to_bronze._ingest_file', side_effect=fake_ingest), \
         patch('stages.mdb_to_bronze.list_mdb_tables', return_value=['baseline', 'followup']):
        rows, errors = _process_mdb_file(
            engine, '/fake/t1.mdb', 'baseline', 'uganda', 'Mbarara', '/fake',
        )

    assert rows == 1  # baseline succeeded
    assert len(errors) == 1
    assert 'Failed to ingest followup' in errors[0]


# ---------------------------------------------------------------------------
# run() (orchestration: per-country glob/dedup, thread-pool dispatch, aggregation)
# ---------------------------------------------------------------------------

def test_run_dispatches_all_files_and_aggregates_rows_and_errors():
    config = _make_config()
    engine = MagicMock()

    def fake_process(engine_arg, db_path, table_name, country, community, extract_path):
        if db_path.endswith('bad.mdb'):
            return 0, [f'error processing {db_path}']
        return 1, []

    with patch('stages.mdb_to_bronze.get_country_paths',
               return_value={'extract_path': '/fake'}), \
         patch('stages.mdb_to_bronze.glob_module.glob',
               return_value=['/fake/t1.mdb', '/fake/bad.mdb']), \
         patch('stages.mdb_to_bronze.select_latest_per_tablet',
               return_value=['/fake/t1.mdb', '/fake/bad.mdb']), \
         patch('stages.mdb_to_bronze._process_mdb_file', side_effect=fake_process):
        stage = MdbToBronze(config=config, engine=engine)
        result = stage.run()

    assert result.rows_written == 1
    assert len(result.errors) == 1
    assert not result.success


def test_run_processes_multiple_countries():
    communities = {
        'ug1': {'country': 'uganda', 'community_name': 'Mbarara'},
        'ke1': {'country': 'kenya', 'community_name': 'Sindo'},
    }
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'communities': communities,
        'trial': {'country_code_map': {'uganda': 1, 'kenya': 2}},
        'access_table_name': 'baseline',
        'excluded_tablets': [],
    }.get(key, default)
    engine = MagicMock()

    processed_countries = []

    def fake_process(engine_arg, db_path, table_name, country, community, extract_path):
        processed_countries.append(country)
        return 1, []

    with patch('stages.mdb_to_bronze.get_country_paths',
               return_value={'extract_path': '/fake'}), \
         patch('stages.mdb_to_bronze.glob_module.glob', return_value=['/fake/t1.mdb']), \
         patch('stages.mdb_to_bronze.select_latest_per_tablet',
               return_value=['/fake/t1.mdb']), \
         patch('stages.mdb_to_bronze._process_mdb_file', side_effect=fake_process):
        stage = MdbToBronze(config=config, engine=engine)
        result = stage.run()

    assert sorted(processed_countries) == ['kenya', 'uganda']
    assert result.rows_written == 2
    assert result.success
