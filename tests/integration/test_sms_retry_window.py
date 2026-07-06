from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from modules.sms_processor import SmsProcessor

pytestmark = pytest.mark.integration


def _make_processor(engine):
    return SmsProcessor(config={'sms': {}}, engine=engine)


def test_count_recent_failures_ignores_failures_before_last_resend(clean_engine):
    """Reproduces the exact bug scenario from the code review: a subjid/week
    hits the retry cap (3 failures), a data manager runs --resend after fixing
    the phone number, and the resend itself fails once more. The count used to
    decide auto-retry-vs-give-up must reflect only failures since that resend
    (1), not the stale all-time total (4) — otherwise the participant is
    silently re-flagged 'failed' and falls out of auto-retry forever."""
    subjid, week = 'SUBJ100', 8
    now = datetime.now(timezone.utc)

    with clean_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sms.queue (subjid, mobile_number, arm_text, language, week, scheduled_date)
            VALUES (:subjid, '256700000100', 'HIV Risk Assessment', 'English', :week, CURRENT_DATE)
        """), {"subjid": subjid, "week": week})

        # Three historical failures that already hit the old retry cap.
        for i in range(3):
            conn.execute(text("""
                INSERT INTO sms.log (subjid, mobile_number, week, message_text, attempt, status, error_message, created_at)
                VALUES (:subjid, '256700000100', :week, 'msg', :attempt, 'failed', 'boom', :created_at)
            """), {
                "subjid": subjid, "week": week, "attempt": i + 1,
                "created_at": now - timedelta(days=5 - i),
            })

        # Data manager resends after fixing the phone number.
        conn.execute(text("""
            INSERT INTO sms.resend_log (subjid, week, actor, note, resent_at)
            VALUES (:subjid, :week, 'data_manager', 'fixed phone number', :resent_at)
        """), {"subjid": subjid, "week": week, "resent_at": now - timedelta(days=1)})

        # The resend itself fails once more.
        conn.execute(text("""
            INSERT INTO sms.log (subjid, mobile_number, week, message_text, attempt, status, error_message, created_at)
            VALUES (:subjid, '256700000100', :week, 'msg', 4, 'failed', 'still bad', :created_at)
        """), {"subjid": subjid, "week": week, "created_at": now})

    processor = _make_processor(clean_engine)
    count = processor._count_recent_failures(subjid, week)

    # Only the 1 post-resend failure should count, not the 4 all-time failures.
    assert count == 1


def test_count_recent_failures_counts_all_time_when_never_resent(clean_engine):
    """With no sms.resend_log row for this (subjid, week), the window falls
    back to '1970-01-01' so all historical failures still count — unchanged
    behavior for participants who have never been manually resent."""
    subjid, week = 'SUBJ101', 11
    now = datetime.now(timezone.utc)

    with clean_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sms.queue (subjid, mobile_number, arm_text, language, week, scheduled_date)
            VALUES (:subjid, '256700000101', 'HIV Risk Assessment', 'English', :week, CURRENT_DATE)
        """), {"subjid": subjid, "week": week})

        for i in range(2):
            conn.execute(text("""
                INSERT INTO sms.log (subjid, mobile_number, week, message_text, attempt, status, error_message, created_at)
                VALUES (:subjid, '256700000101', :week, 'msg', :attempt, 'failed', 'boom', :created_at)
            """), {
                "subjid": subjid, "week": week, "attempt": i + 1,
                "created_at": now - timedelta(days=2 - i),
            })

    processor = _make_processor(clean_engine)
    count = processor._count_recent_failures(subjid, week)

    assert count == 2
