from __future__ import annotations

import pytest
from sqlalchemy import text

from scripts.send_arm_broadcast import resolve_recipients

pytestmark = pytest.mark.integration


def _create_baseline(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE ibis.baseline (
                subjid TEXT,
                mobile_number TEXT,
                preferred_language_text TEXT,
                arm_text TEXT,
                countrycode TEXT,
                consent TEXT
            )
        """))


def test_resolve_recipients_includes_only_consented_non_opted_out_arm_matches(clean_engine):
    """Reproduces the exact safety property this script exists for: message
    exactly the consented, non-opted-out members of one arm, and nobody
    else. Seeds a realistic mix (right arm+consented+not-opted-out,
    opted-out, wrong arm, not consented, wrong country) directly into a
    real ibis.baseline/sms.opt_outs and asserts resolve_recipients's actual
    SQL — not a mock — returns exactly the one expected recipient."""
    _create_baseline(clean_engine)

    with clean_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ibis.baseline
                (subjid, mobile_number, preferred_language_text, arm_text, countrycode, consent)
            VALUES
                ('S1', '256700000001', 'English',    'Incentive', '1', '1'),
                ('S2', '256700000002', 'English',    'Incentive', '1', '1'),
                ('S3', '256700000003', 'Luganda',    'HIV Risk Assessment', '1', '1'),
                ('S4', '256700000004', 'English',    'Incentive', '1', '0'),
                ('S5', '256700000005', 'English',    'Incentive', '2', '1')
        """))
        # S2 is a consented Incentive-arm participant who has opted out.
        conn.execute(text("""
            INSERT INTO sms.opt_outs (subjid, mobile_number, reason)
            VALUES ('S2', '256700000002', 'requested stop')
        """))

    messages = {'English': 'Pause notice (EN)', 'Luganda': 'Pause notice (LG)'}
    resolved, skipped = resolve_recipients(clean_engine, 'Incentive', '1', messages)

    assert [r['subjid'] for r in resolved] == ['S1']
    assert resolved[0] == {
        'subjid': 'S1', 'mobile_number': '256700000001',
        'language': 'English', 'message_text': 'Pause notice (EN)',
    }
    assert skipped == []
