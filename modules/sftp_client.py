# modules/sftp_client.py
from __future__ import annotations

import logging
import paramiko
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_TABLET_ARCHIVE_RE = re.compile(
    r'^(Tablet\d+)_(\d{4}_\d{2}_\d{2}-\d{2}_\d{2}_\d{2})\.7z$',
    re.IGNORECASE,
)


def select_latest_remote_per_tablet(filenames: list[str]) -> dict[str, str]:
    """
    Return {tablet_id: filename} for the latest .7z archive per tablet.
    Filenames not matching the Tablet###_YYYY_MM_DD-HH_MM_SS.7z pattern
    are silently ignored.
    """
    latest: dict[str, tuple[datetime, str]] = {}
    for name in filenames:
        m = _TABLET_ARCHIVE_RE.match(name)
        if not m:
            continue
        tablet_id = m.group(1)
        ts = datetime.strptime(m.group(2), '%Y_%m_%d-%H_%M_%S')
        existing_ts, _ = latest.get(tablet_id, (datetime.min, ''))
        if ts > existing_ts:
            latest[tablet_id] = (ts, name)
    return {tablet_id: name for tablet_id, (_, name) in latest.items()}


class SFTPClient:
    """
    Minimal paramiko SFTP wrapper. Use as a context manager — the connection
    is opened on entry and closed on exit regardless of exceptions.
    """

    def __init__(self, hostname: str, username: str, password: str) -> None:
        self._hostname = hostname
        self._username = username
        self._password = password
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def __enter__(self) -> 'SFTPClient':
        self._transport = paramiko.Transport((self._hostname, 22))
        self._transport.connect(username=self._username, password=self._password)
        try:
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        except Exception:
            self._transport.close()
            raise
        logger.info(f"Connected to SFTP: {self._hostname}")
        return self

    def __exit__(self, *args) -> None:
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        logger.info(f"Disconnected from SFTP: {self._hostname}")

    def list_files(self, remote_path: str) -> list[str]:
        """Return filenames (not full paths) in remote_path."""
        return [attr.filename for attr in self._sftp.listdir_attr(remote_path)]

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download remote_path to local_path."""
        self._sftp.get(remote_path, local_path)
        logger.info(f"Downloaded: {remote_path} → {local_path}")
