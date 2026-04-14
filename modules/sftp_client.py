# modules/sftp_client.py
from __future__ import annotations

import logging
import re
from datetime import datetime

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
