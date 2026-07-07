#!/usr/bin/env python3
"""
Alerts when a scheduled pipeline/SMS invocation hasn't run recently enough
(a "missed run" — the cron job never fired, as opposed to a run that
happened and failed, which is already alerted on by send_pipeline_report/
send_sms_flagged_alert). Reads ops.pipeline_runs (see modules/metrics.py).

Only invocations that go through ibis.py/sms.py (and therefore call
start_pipeline_run) are visible here — scripts/backup_db.sh and
scripts/export_ug_incentive_arm.py are standalone and not tracked.

Usage:
    python scripts/check_missed_runs.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.config import ConfigLoader
from modules.db import create_db_engine
from modules.logging_utils import configure_logging
from modules.notifier import send_missed_run_alert

configure_logging()
logger = logging.getLogger(__name__)

# Keys must exactly match the strings produced by ibis.py's and sms.py's
# _compute_invocation() functions — see test_ibis_scheduled_invocations_are_tracked
# and test_sms_scheduled_invocations_are_tracked in tests/test_check_missed_runs.py.
_TRACKED_INVOCATIONS: dict[str, timedelta] = {
    '-a': timedelta(hours=26),
    '-p store_ibis': timedelta(days=8.5),
    'sms --check-delivery': timedelta(hours=26),
    'sms --weekly-report': timedelta(days=8.5),
    '-p reconcile_silver': timedelta(days=8.5),
}


def find_overdue(engine) -> list[dict]:
    """Return a list of {invocation, threshold, last_seen} for every tracked
    invocation whose most recent ops.pipeline_runs row (if any) is older
    than its allowed threshold. last_seen is None if never seen at all."""
    overdue: list[dict] = []
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        for invocation, threshold in _TRACKED_INVOCATIONS.items():
            row = conn.execute(text("""
                SELECT started_at FROM ops.pipeline_runs
                WHERE invocation = :invocation
                ORDER BY started_at DESC
                LIMIT 1
            """), {'invocation': invocation}).fetchone()
            if row is None:
                overdue.append({'invocation': invocation, 'threshold': threshold, 'last_seen': None})
            elif now - row.started_at > threshold:
                overdue.append({'invocation': invocation, 'threshold': threshold, 'last_seen': row.started_at})
    return overdue


def main() -> None:
    config = ConfigLoader('config.json')
    engine = create_db_engine(config)

    overdue = find_overdue(engine)
    if overdue:
        logger.warning("Overdue invocations detected: %s", [o['invocation'] for o in overdue])
        send_missed_run_alert(overdue, config)
    else:
        logger.info("All tracked invocations are within their expected schedule.")


if __name__ == '__main__':
    main()
