from __future__ import annotations

import logging
from cryptography.fernet import Fernet
import pandas as pd

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


def _query_validation_report(engine) -> pd.DataFrame | None:
    """
    Query gold_ibis.ds_validation_report.
    Returns None if the table does not exist or any error occurs.
    """
    try:
        return pd.read_sql('SELECT * FROM gold_ibis.ds_validation_report', engine)
    except Exception:
        return None


def _should_notify(results: dict, engine) -> bool:
    """
    Return True if the run has any failures OR any ERROR-severity validation rows.
    """
    if any(not r.success for r in results.values()):
        return True
    report = _query_validation_report(engine)
    if report is not None and not report.empty:
        if (report['severity'] == 'ERROR').any():
            return True
    return False
