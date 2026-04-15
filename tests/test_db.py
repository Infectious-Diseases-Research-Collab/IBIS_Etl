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
    assert set(SCHEMAS) == {'bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis'}

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
