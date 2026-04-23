from __future__ import annotations

import smtplib
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from cryptography.fernet import Fernet
from modules.notifier import (
    _load_smtp_password,
    _query_validation_report,
    _build_stage_summary,
    _build_validation_summary,
    send_pipeline_report,
)
from stages.base import StageResult


# ---------------------------------------------------------------------------
# _load_smtp_password
# ---------------------------------------------------------------------------

def test_load_smtp_password_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Password={cipher.encrypt(b's3cr3t').decode()}\n")

    assert _load_smtp_password(str(ini_file), str(key_file)) == 's3cr3t'


def test_load_smtp_password_missing_raises(tmp_path):
    key = Fernet.generate_key()
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text('Host=smtp.example.com\n')

    with pytest.raises(KeyError, match='Password'):
        _load_smtp_password(str(ini_file), str(key_file))


# ---------------------------------------------------------------------------
# _query_validation_report
# ---------------------------------------------------------------------------

def test_query_validation_report_returns_none_on_db_error():
    with patch('pandas.read_sql', side_effect=Exception('connection refused')):
        result = _query_validation_report(MagicMock())
    assert result is None


# ---------------------------------------------------------------------------
# _build_stage_summary
# ---------------------------------------------------------------------------

def test_build_stage_summary_shows_all_statuses():
    results = {
        'mdb_to_bronze':    StageResult(success=True, rows_written=5416),
        'bronze_to_silver': StageResult(success=False, errors=['err']),
    }
    stages = ['mdb_to_bronze', 'bronze_to_silver', 'transform_ibis']
    text = _build_stage_summary(results, stages)
    assert '✓' in text and 'mdb_to_bronze' in text
    assert '✗' in text and 'bronze_to_silver' in text
    assert '—' in text and 'transform_ibis' in text
    assert '5,416' in text


def test_build_stage_summary_no_rows_for_zero():
    results = {'transform_ibis': StageResult(success=True, rows_written=0)}
    text = _build_stage_summary(results, ['transform_ibis'])
    assert '✓' in text
    assert '0' not in text


# ---------------------------------------------------------------------------
# _build_validation_summary
# ---------------------------------------------------------------------------

def test_build_validation_summary_none_returns_unavailable():
    text = _build_validation_summary(None)
    assert 'unavailable' in text.lower()


def test_build_validation_summary_groups_by_severity_country_site():
    report = pd.DataFrame({
        'severity': ['ERROR', 'WARNING', 'WARNING'],
        'check':    ['dup_id', 'missing_appt', 'sparse_col'],
        'country':  ['Kenya', 'Uganda', 'Kenya'],
        'site':     ['21 (X)', '11 (Y)', '21 (X)'],
        'record_count': [2, 3, 1],
    })
    text = _build_validation_summary(report)
    assert 'ERRORS' in text
    assert 'WARNINGS' in text
    assert 'Kenya / 21 (X)' in text
    assert 'Uganda / 11 (Y)' in text
    assert 'dup_id' in text
    assert 'see attachment' in text.lower()


def test_build_validation_summary_empty_df_returns_sep_only():
    report = pd.DataFrame(columns=['severity', 'check', 'country', 'site', 'record_count'])
    text = _build_validation_summary(report)
    # No severity sections — just header and separators
    assert 'ERRORS' not in text
    assert 'WARNINGS' not in text


# ---------------------------------------------------------------------------
# send_pipeline_report helpers
# ---------------------------------------------------------------------------

def _make_email_cfg(tmp_path):
    """Minimal email config with real Fernet-encrypted password."""
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Password={cipher.encrypt(b's3cr3t').decode()}\n")
    return {
        'smtp_host': 'smtp.example.com',
        'smtp_port': 587,
        'sender': 'ibis@example.com',
        'smtp_username': 'user@example.com',
        'pipeline_recipients': ['admin@example.com'],
        'field_recipients': {
            'uganda': ['ug-team@example.com'],
            'kenya':  ['ke-team@example.com'],
        },
        'keyfiles': {
            'smtp_ini': str(ini_file),
            'smtp_key': str(key_file),
        },
    }


def _config(email_cfg):
    """Wrap email_cfg in a dict the way config.json is structured."""
    return {'email': email_cfg}


# ---------------------------------------------------------------------------
# send_pipeline_report
# ---------------------------------------------------------------------------

def test_send_pipeline_report_no_config_is_silent():
    send_pipeline_report(
        results={'mdb_to_bronze': StageResult(success=False)},
        stages=['mdb_to_bronze'],
        engine=MagicMock(),
        config={},
    )


def test_send_pipeline_report_always_sends_to_pipeline_recipients(tmp_path):
    """Pipeline recipients receive an email on every run, including clean ones."""
    config = _config(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=True)}
    clean_report = pd.DataFrame(columns=['severity', 'check', 'country', 'site'])

    mock_smtp_instance = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=clean_report):
        with patch('smtplib.SMTP') as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )

    mock_smtp_instance.sendmail.assert_called_once()
    to_arg = mock_smtp_instance.sendmail.call_args[0][1]
    assert to_arg == ['admin@example.com']


def test_send_pipeline_report_failed_subject_says_failed(tmp_path):
    config = _config(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False, errors=['boom'])}

    mock_smtp_instance = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP') as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )

    msg_string = mock_smtp_instance.sendmail.call_args[0][2]
    assert 'FAILED' in msg_string


def test_send_pipeline_report_field_email_sent_on_issues(tmp_path):
    """Field recipients receive email only when their country has validation issues."""
    config = _config(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({
        'severity': ['WARNING'], 'check': ['dup_phone'],
        'country': ['Uganda'], 'site': ['Mbarara'],
        'record_count': [2], 'detail': ['test'],
        'affected_subjids': ['IBIS001'], 'affected_tablets': ['44'],
    })

    sendmail_calls = []
    mock_smtp_instance = MagicMock()
    mock_smtp_instance.sendmail.side_effect = lambda *a, **kw: sendmail_calls.append(a)

    with patch('modules.notifier._query_validation_report', return_value=report):
        with patch('smtplib.SMTP') as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )

    recipients_seen = [call[1] for call in sendmail_calls]
    assert ['admin@example.com'] in recipients_seen       # pipeline email
    assert ['ug-team@example.com'] in recipients_seen     # Uganda field email
    assert ['ke-team@example.com'] not in recipients_seen # Kenya had no issues


def test_send_pipeline_report_does_not_raise_on_smtp_error(tmp_path):
    """SMTP failure is logged and swallowed — pipeline must not raise."""
    config = _config(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False)}

    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP', side_effect=smtplib.SMTPException('conn refused')):
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )


# ---------------------------------------------------------------------------
# _build_sms_summary
# ---------------------------------------------------------------------------

def test_build_sms_summary_shows_sent_failed_skipped():
    from modules.notifier import _build_sms_summary
    from stages.base import StageResult

    results = {
        'send_sms': StageResult(
            success=True,
            rows_written=10,
            metadata={
                'sent': 10,
                'failed': 2,
                'skipped': 1,
                'failures': [
                    {'subjid': 'IBIS001', 'mobile_number': '0700001', 'week': 8, 'error': 'timeout'},
                ],
            },
        )
    }
    summary = _build_sms_summary(results)

    assert 'Sent:' in summary
    assert '10' in summary
    assert 'Failed:' in summary
    assert '2' in summary
    assert 'IBIS001' in summary
    assert 'timeout' in summary


def test_build_sms_summary_returns_none_when_stage_absent():
    from modules.notifier import _build_sms_summary
    assert _build_sms_summary({}) is None


def test_build_sms_summary_returns_none_when_no_metadata():
    from modules.notifier import _build_sms_summary
    from stages.base import StageResult
    results = {'send_sms': StageResult(success=True)}
    assert _build_sms_summary(results) is None


# ---------------------------------------------------------------------------
# _build_weekly_sms_report
# ---------------------------------------------------------------------------

def test_build_weekly_sms_report_includes_sites_and_week_ending():
    from modules.notifier import _build_weekly_sms_report

    weekly_rows = [
        {'health_facility_ug': '11', 'week': 8, 'submitted': 5, 'delivered': 4, 'undelivered': 1, 'pending': 0},
        {'health_facility_ug': '14', 'week': 8, 'submitted': 3, 'delivered': 3, 'undelivered': 0, 'pending': 0},
    ]
    cumulative_rows = [
        {'health_facility_ug': '11', 'week': 8, 'submitted': 20, 'delivered': 18, 'undelivered': 2, 'pending': 0},
    ]
    report = _build_weekly_sms_report(weekly_rows, cumulative_rows, '17 Apr 2026')

    assert '17 Apr 2026' in report
    assert 'This week' in report
    assert 'Cumulative' in report
    assert 'Bushenyi HCIV' in report
    assert 'Ruhoko HCIV' in report
    assert 'Total' in report


def test_build_weekly_sms_table_no_activity():
    from modules.notifier import _build_weekly_sms_table
    result = _build_weekly_sms_table([], 'This week')
    assert 'No activity' in result
