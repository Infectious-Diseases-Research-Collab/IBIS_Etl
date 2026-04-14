from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from cryptography.fernet import Fernet
from modules.notifier import _load_smtp_credentials, _should_notify
from stages.base import StageResult


def test_load_smtp_credentials_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(
        f"Username={cipher.encrypt(b'user@example.com').decode()}\n"
        f"Password={cipher.encrypt(b's3cr3t').decode()}\n"
    )

    username, password = _load_smtp_credentials(str(ini_file), str(key_file))
    assert username == 'user@example.com'
    assert password == 's3cr3t'


def test_load_smtp_credentials_missing_username_raises(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Password={cipher.encrypt(b's3cr3t').decode()}\n")

    with pytest.raises(KeyError, match='Username'):
        _load_smtp_credentials(str(ini_file), str(key_file))


def test_load_smtp_credentials_missing_password_raises(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Username={cipher.encrypt(b'user@example.com').decode()}\n")

    with pytest.raises(KeyError, match='Password'):
        _load_smtp_credentials(str(ini_file), str(key_file))


def test_should_notify_true_on_stage_failure():
    results = {
        'mdb_to_bronze': StageResult(success=False, errors=['boom']),
        'bronze_to_silver': StageResult(success=True, rows_written=100),
    }
    engine = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, engine) is True


def test_should_notify_true_on_error_in_report():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['ERROR', 'WARNING'], 'check': ['a', 'b']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is True


def test_should_notify_false_on_clean_run():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['WARNING'], 'check': ['a']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is False


def test_should_notify_false_when_report_is_none():
    results = {'mdb_to_bronze': StageResult(success=True)}
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, MagicMock()) is False
