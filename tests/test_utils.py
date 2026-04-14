# tests/test_utils.py
import pytest
from cryptography.fernet import Fernet
from modules.utils import get_decrypted_password


def test_get_decrypted_password_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted = cipher.encrypt(b'mysecret').decode()

    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text(f'# comment\nPassword={encrypted}\n')

    result = get_decrypted_password(str(cred_file), str(key_file))
    assert result == 'mysecret'


def test_get_decrypted_password_missing_key_raises(tmp_path):
    key = Fernet.generate_key()
    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text('OtherKey=something\n')

    with pytest.raises(KeyError, match="Password"):
        get_decrypted_password(str(cred_file), str(key_file))
