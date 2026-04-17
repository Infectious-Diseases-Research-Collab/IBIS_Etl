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


# ---------------------------------------------------------------------------
# BLASTA API client
# ---------------------------------------------------------------------------

class BlastaClient:
    def __init__(self, username: str, password: str, max_retries: int = 3):
        self._username = username
        self._password = password
        self._max_retries = max_retries
        self._token: str | None = None

    def _get_token(self) -> str:
        resp = requests.post(
            f"{BLASTA_BASE_URL}/get_token/",
            json={"username": self._username, "password": self._password},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def send(self, phone_number: str, message: str) -> dict:
        """Send SMS. Returns provider response dict. Raises RequestException on permanent failure."""
        if self._token is None:
            self._token = self._get_token()

        for attempt in range(self._max_retries):
            try:
                resp = requests.post(
                    f"{BLASTA_BASE_URL}/send_sms/",
                    headers={"authToken": self._token},
                    json={"msg": message, "numbers": phone_number},
                    timeout=30,
                )
                if resp.status_code == 401:
                    logger.info("Token expired, refreshing...")
                    self._token = self._get_token()
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                if attempt < self._max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        "Attempt %d/%d failed for %s, retrying in %ds: %s",
                        attempt + 1, self._max_retries, phone_number, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    raise
        raise requests.RequestException(f"All {self._max_retries} attempts failed for {phone_number}")
