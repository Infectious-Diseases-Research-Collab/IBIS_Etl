from __future__ import annotations

import logging
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


def _load_smtp_credentials(ini_path: str, key_path: str) -> tuple[str, str]:
    """
    Read Fernet-encrypted SMTP credentials from ini_path using the key in key_path.
    Returns (username, password).
    Raises KeyError if 'Username' or 'Password' is absent from the ini file.
    """
    with open(key_path, 'r') as f:
        key = f.read().strip().encode()
    cipher = Fernet(key)

    cfg: dict[str, str] = {}
    with open(ini_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            cfg[k.strip()] = v.strip()

    if 'Username' not in cfg:
        raise KeyError(f"'Username' key not found in credential file: {ini_path}")
    if 'Password' not in cfg:
        raise KeyError(f"'Password' key not found in credential file: {ini_path}")

    return (
        cipher.decrypt(cfg['Username'].encode()).decode(),
        cipher.decrypt(cfg['Password'].encode()).decode(),
    )
