# modules/utils.py
from __future__ import annotations

import logging
import os

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


def load_fernet_key(env_var: str, key_file: str | None = None) -> bytes:
    """
    Load a Fernet decryption key, preferring the environment variable
    *env_var* over a key file.

    Storing a Fernet key in the same secrets/ directory as the ciphertext it
    decrypts means anyone with read access to that directory holds both the
    lock and the key. Sourcing it from the environment — populated by the
    platform's secret store, never written to disk alongside the ciphertext —
    removes that exposure. *key_file* is supported only as a fallback for
    local development and logs a warning when used.

    Raises KeyError if neither source is available.
    """
    value = os.environ.get(env_var)
    if value:
        return value.strip().encode()

    if key_file:
        logger.warning(
            "%s is not set — falling back to the Fernet key file '%s'. "
            "This stores the key alongside its ciphertext and should only "
            "be used for local development; set %s instead.",
            env_var, key_file, env_var,
        )
        with open(key_file, 'r') as f:
            return f.read().strip().encode()

    raise KeyError(
        f"Fernet key not found: set the {env_var} environment variable "
        f"(no fallback key file was provided either)."
    )


def get_decrypted_password(
    cred_filename: str, env_var: str, key_file: str | None = None
) -> str:
    """
    Decrypt a Fernet-encrypted password from a credential file.
    The Fernet key is read from the *env_var* environment variable
    (preferred) or, as a local-dev fallback, from *key_file*.
    Credential file format: key=value lines; blank lines and lines starting
    with # are ignored. Raises KeyError if 'Password' key is absent.
    """
    cipher = Fernet(load_fernet_key(env_var, key_file))

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
