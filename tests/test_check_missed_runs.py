from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from scripts.check_missed_runs import _TRACKED_INVOCATIONS, find_overdue


def test_find_overdue_flags_invocation_with_no_rows_at_all():
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    overdue = find_overdue(engine)

    assert len(overdue) == len(_TRACKED_INVOCATIONS)
    assert all(item['last_seen'] is None for item in overdue)


def test_find_overdue_flags_invocation_past_threshold():
    engine = MagicMock()
    conn = MagicMock()
    old_time = datetime.now(timezone.utc) - timedelta(days=30)
    row = MagicMock(started_at=old_time)
    conn.execute.return_value.fetchone.return_value = row
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    overdue = find_overdue(engine)

    assert len(overdue) == len(_TRACKED_INVOCATIONS)


def test_find_overdue_returns_empty_when_all_recent():
    engine = MagicMock()
    conn = MagicMock()
    recent_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    row = MagicMock(started_at=recent_time)
    conn.execute.return_value.fetchone.return_value = row
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    overdue = find_overdue(engine)

    assert overdue == []


def test_find_overdue_only_flags_the_specific_overdue_invocation():
    engine = MagicMock()
    conn = MagicMock()
    recent_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    old_time = datetime.now(timezone.utc) - timedelta(days=30)

    def fake_execute(stmt, params):
        result = MagicMock()
        if params['invocation'] == '-a':
            result.fetchone.return_value = MagicMock(started_at=old_time)
        else:
            result.fetchone.return_value = MagicMock(started_at=recent_time)
        return result

    conn.execute.side_effect = fake_execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    overdue = find_overdue(engine)

    assert len(overdue) == 1
    assert overdue[0]['invocation'] == '-a'


def test_ibis_scheduled_invocations_are_tracked():
    from argparse import Namespace
    from ibis import _compute_invocation as ibis_compute_invocation

    assert ibis_compute_invocation(Namespace(all=True, pipeline=None)) in _TRACKED_INVOCATIONS
    assert ibis_compute_invocation(Namespace(all=False, pipeline='store_ibis')) in _TRACKED_INVOCATIONS
    assert ibis_compute_invocation(Namespace(all=False, pipeline='reconcile_silver')) in _TRACKED_INVOCATIONS


def test_sms_scheduled_invocations_are_tracked():
    from argparse import Namespace
    from sms import _compute_invocation as sms_compute_invocation

    check_delivery_args = Namespace(
        init_db=False, resend=False, check_delivery=True, weekly_report=False, sync=False,
    )
    weekly_report_args = Namespace(
        init_db=False, resend=False, check_delivery=False, weekly_report=True, sync=False,
        week_to_date=False,
    )
    weekly_report_wtd_args = Namespace(
        init_db=False, resend=False, check_delivery=False, weekly_report=True, sync=False,
        week_to_date=True,
    )

    assert sms_compute_invocation(check_delivery_args) in _TRACKED_INVOCATIONS
    assert sms_compute_invocation(weekly_report_args) in _TRACKED_INVOCATIONS
    assert sms_compute_invocation(weekly_report_wtd_args) in _TRACKED_INVOCATIONS
