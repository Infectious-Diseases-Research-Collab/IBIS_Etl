from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests
from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

BLASTA_BASE_URL = "https://sms.dmarkmobile.com/v3/api"


# ---------------------------------------------------------------------------
# Credential loading (follows existing Fernet .ini/.key pattern)
# ---------------------------------------------------------------------------

def _load_blasta_creds(ini_path: str, key_path: str) -> tuple[str, str]:
    """Load and decrypt BLASTA username and password from secrets files."""
    with open(key_path) as f:
        key = f.read().strip().encode()
    cipher = Fernet(key)

    cfg: dict[str, str] = {}
    with open(ini_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            cfg[k.strip()] = v.strip()

    username = cfg.get('Username', '')
    encrypted_password = cfg.get('Password', '')
    if not username:
        raise KeyError("'Username' not found in BLASTA.ini")
    if not encrypted_password:
        raise KeyError("'Password' not found in BLASTA.ini")

    return username, cipher.decrypt(encrypted_password.encode()).decode()


# ---------------------------------------------------------------------------
# Placeholder substitution
# ---------------------------------------------------------------------------

def _substitute_placeholder(message: str, appointment_date) -> str:
    """Replace [...] in message with formatted appointment_date (DD/MM/YYYY)."""
    if not appointment_date:
        return message
    try:
        if isinstance(appointment_date, str):
            d = datetime.strptime(appointment_date, '%d/%m/%Y').date()
        else:
            d = appointment_date  # already a date object
        return re.sub(r'\[.*?\]', d.strftime('%d/%m/%Y'), message)
    except (ValueError, TypeError):
        logger.warning("Invalid appointment date for placeholder substitution: %s", appointment_date)
        return message
