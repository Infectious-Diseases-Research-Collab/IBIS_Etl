import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


def test_fixture_participant_reaches_ibis_baseline(pipeline_engine):
    with pipeline_engine.connect() as conn:
        row = conn.execute(text(
            'SELECT subjid, arm_text, mobile_number FROM ibis.baseline WHERE uniqueid = :u'
        ), {'u': 'e2e-test-001'}).fetchone()
    assert row is not None
    assert row.subjid == 'SUBJ001'
    assert row.arm_text == 'HIV Risk Assessment'
    assert row.mobile_number == '256700000001'


def test_fixture_participant_reaches_ibis_d_enrollment(pipeline_engine):
    with pipeline_engine.connect() as conn:
        row = conn.execute(text(
            'SELECT subjid, screening_id, consent FROM ibis.d_enrollment WHERE uniqueid = :u'
        ), {'u': 'e2e-test-001'}).fetchone()
    assert row is not None
    assert row.subjid == 'SUBJ001'
    assert row.screening_id == 'SCR001'
    assert row.consent == '1'


def test_fixture_participant_reaches_store_ibis_snapshot(pipeline_engine):
    with pipeline_engine.connect() as conn:
        row = conn.execute(text(
            'SELECT subjid FROM store_ibis.baseline WHERE uniqueid = :u AND snapshot_date = CURRENT_DATE'
        ), {'u': 'e2e-test-001'}).fetchone()
    assert row is not None
    assert row.subjid == 'SUBJ001'


def test_silver_history_retains_the_promoted_row(pipeline_engine):
    """The incremental-processing design's append-only history table must
    contain this row too, not just the current/latest table."""
    with pipeline_engine.connect() as conn:
        row = conn.execute(text(
            'SELECT subjid FROM silver_ibis.baseline_history WHERE uniqueid = :u'
        ), {'u': 'e2e-test-001'}).fetchone()
    assert row is not None
    assert row.subjid == 'SUBJ001'
