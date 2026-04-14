# modules/utils.py
from __future__ import annotations

from cryptography.fernet import Fernet


def get_decrypted_password(cred_filename: str, key_file: str) -> str:
    """
    Decrypt a Fernet-encrypted password from a credential file.
    File format: key=value lines; blank lines and lines starting with # are ignored.
    Raises KeyError if 'Password' key is absent.
    """
    with open(key_file, 'r') as f:
        key = f.read().strip().encode()

    cipher = Fernet(key)

    config: dict[str, str] = {}
    with open(cred_filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            config[k.strip()] = v.strip()

    if 'Password' not in config:
        raise KeyError(
            f"'Password' key not found in credential file: {cred_filename}"
        )

    return cipher.decrypt(config['Password'].encode()).decode()
