# tests/test_utils.py
import pytest
from cryptography.fernet import Fernet
from modules.utils import get_decrypted_password, load_fernet_key


def test_get_decrypted_password_roundtrip_via_key_file(tmp_path):
    """No env var set — falls back to the key file (local-dev path)."""
    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted = cipher.encrypt(b'mysecret').decode()

    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text(f'# comment\nPassword={encrypted}\n')

    result = get_decrypted_password(str(cred_file), 'IBIS_TEST_UNSET_VAR', str(key_file))
    assert result == 'mysecret'


def test_get_decrypted_password_prefers_env_var_over_key_file(tmp_path, monkeypatch):
    """When the env var is set, it wins even if a (wrong) key file is also given."""
    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted = cipher.encrypt(b'mysecret').decode()

    cred_file = tmp_path / 'test.ini'
    cred_file.write_text(f'Password={encrypted}\n')

    wrong_key_file = tmp_path / 'wrong.key'
    wrong_key_file.write_text(Fernet.generate_key().decode())

    monkeypatch.setenv('IBIS_TEST_KEY', key.decode())
    result = get_decrypted_password(str(cred_file), 'IBIS_TEST_KEY', str(wrong_key_file))
    assert result == 'mysecret'


def test_get_decrypted_password_missing_key_raises(tmp_path):
    key = Fernet.generate_key()
    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text('OtherKey=something\n')

    with pytest.raises(KeyError, match="Password"):
        get_decrypted_password(str(cred_file), 'IBIS_TEST_UNSET_VAR', str(key_file))


def test_get_decrypted_password_no_key_source_raises(tmp_path):
    cred_file = tmp_path / 'test.ini'
    cred_file.write_text('Password=whatever\n')

    with pytest.raises(KeyError, match='IBIS_TEST_UNSET_VAR'):
        get_decrypted_password(str(cred_file), 'IBIS_TEST_UNSET_VAR')


# ---------------------------------------------------------------------------
# load_fernet_key
# ---------------------------------------------------------------------------

def test_load_fernet_key_from_env_var(monkeypatch):
    monkeypatch.setenv('IBIS_TEST_KEY', 'abc123')
    assert load_fernet_key('IBIS_TEST_KEY') == b'abc123'


def test_load_fernet_key_falls_back_to_file(tmp_path, caplog):
    import logging
    key_file = tmp_path / 'k.key'
    key_file.write_text('filekey123')

    with caplog.at_level(logging.WARNING):
        result = load_fernet_key('IBIS_TEST_UNSET_VAR', str(key_file))

    assert result == b'filekey123'
    assert 'IBIS_TEST_UNSET_VAR' in caplog.text


def test_load_fernet_key_raises_when_neither_available():
    with pytest.raises(KeyError, match='IBIS_TEST_UNSET_VAR'):
        load_fernet_key('IBIS_TEST_UNSET_VAR')
