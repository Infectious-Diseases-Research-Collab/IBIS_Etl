import pytest
from unittest.mock import MagicMock, patch, call
from modules.db import create_db_engine, init_schemas, SCHEMAS

def test_init_schemas_creates_all_schemas():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    init_schemas(mock_engine)

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    for schema in SCHEMAS:
        assert any(schema in sql for sql in executed_sql), f"Schema {schema} not created"
    mock_conn.commit.assert_called_once()

def test_schemas_list_contains_all_layers():
    assert set(SCHEMAS) == {'bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis', 'sms'}

def test_create_db_engine_reads_secret_file(tmp_path):
    secret_file = tmp_path / 'db_password'
    secret_file.write_text('secret')
    config = MagicMock()
    config.get.return_value = {
        'host': 'localhost', 'port': 5432, 'name': 'ibis',
        'user': 'ibis_user', 'password_secret_file': str(secret_file)
    }
    with patch('modules.db.create_engine') as mock_create:
        create_db_engine(config)
        url = mock_create.call_args[0][0]
        assert url.password == 'secret'
        assert url.host == 'localhost'
        assert mock_create.call_args[1].get('pool_pre_ping') is True

def test_create_db_engine_sets_statement_timeout(tmp_path):
    secret_file = tmp_path / 'db_password'
    secret_file.write_text('secret')
    config = MagicMock()
    config.get.return_value = {
        'host': 'localhost', 'port': 5432, 'name': 'ibis',
        'user': 'ibis_user', 'password_secret_file': str(secret_file)
    }
    with patch('modules.db.create_engine') as mock_create:
        create_db_engine(config)
        connect_args = mock_create.call_args[1].get('connect_args', {})
        options = connect_args.get('options', '')
        assert 'statement_timeout' in options
        assert 'lock_timeout' in options


# ---------------------------------------------------------------------------
# pipeline_lock
# ---------------------------------------------------------------------------

def test_pipeline_lock_acquires_and_releases():
    from modules.db import pipeline_lock

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = True
    engine = MagicMock()
    engine.connect.return_value = conn

    with pipeline_lock(engine, 'test_lock'):
        pass

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('pg_try_advisory_lock' in s for s in sqls)
    assert any('pg_advisory_unlock' in s for s in sqls)
    conn.close.assert_called_once()


def test_pipeline_lock_releases_even_if_block_raises():
    from modules.db import pipeline_lock

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = True
    engine = MagicMock()
    engine.connect.return_value = conn

    with pytest.raises(ValueError):
        with pipeline_lock(engine, 'test_lock'):
            raise ValueError('boom')

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('pg_advisory_unlock' in s for s in sqls)
    conn.close.assert_called_once()


def test_pipeline_lock_raises_when_already_held():
    from modules.db import pipeline_lock, PipelineLockError

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn

    with pytest.raises(PipelineLockError):
        with pipeline_lock(engine, 'test_lock'):
            pass  # pragma: no cover — should never be entered

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert not any('pg_advisory_unlock' in s for s in sqls), \
        "must not attempt to unlock a lock it never acquired"
    conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# init_readonly_role
# ---------------------------------------------------------------------------

def test_init_readonly_role_creates_when_absent():
    from modules.db import init_readonly_role, READONLY_ROLE, SCHEMAS

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = None  # role does not exist yet
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    init_readonly_role(engine, 'hunter2')

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('CREATE ROLE' in s and READONLY_ROLE in s for s in sqls)
    assert not any('ALTER ROLE' in s for s in sqls)
    for schema in SCHEMAS:
        assert any(f'GRANT USAGE ON SCHEMA {schema}' in s for s in sqls)
        assert any(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema}' in s for s in sqls)
        assert any(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {schema}' in s for s in sqls)


def test_init_readonly_role_alters_password_when_present():
    from modules.db import init_readonly_role, READONLY_ROLE

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 1  # role already exists
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    init_readonly_role(engine, 'newpassword')

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('ALTER ROLE' in s and READONLY_ROLE in s for s in sqls)
    assert not any('CREATE ROLE' in s for s in sqls)


def test_lock_key_is_deterministic_and_distinct():
    from modules.db import _lock_key

    assert _lock_key('ibis_etl_pipeline') == _lock_key('ibis_etl_pipeline')
    assert _lock_key('ibis_etl_pipeline') != _lock_key('ibis_sms_pipeline')
