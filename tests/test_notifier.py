from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from modules.notifier import _load_smtp_credentials


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
