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

# ---------------------------------------------------------------------------
# send_sms_flagged_alert
# ---------------------------------------------------------------------------

def _plain_text_body(msg_string: str) -> str:
    """Extract and decode the text/plain part from a serialized MIME message
    (the body is base64-encoded by default for non-ASCII utf-8 content, so a
    raw substring search on msg_string won't find plain-language content)."""
    import email
    msg = email.message_from_string(msg_string)
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            return part.get_payload(decode=True).decode('utf-8')
    raise AssertionError('no text/plain part found')


def test_send_sms_flagged_alert_emits_audited_resend_command_not_raw_sql(tmp_path):
    """The remediation instructions must route through `sms.py --resend`
    (audited, logged to sms.resend_log) rather than a raw UPDATE statement
    that leaves no record of who ran it."""
    from modules.notifier import send_sms_flagged_alert

    email_cfg = _make_email_cfg(tmp_path)
    email_cfg['sms_dm_recipients'] = ['dm@example.com']
    config = _config(email_cfg)

    flagged = [
        {'subjid': 'IBIS001', 'health_facility_ug': '11', 'week': 8, 'last_error': 'timeout'},
        {'subjid': 'IBIS002', 'health_facility_ug': '11', 'week': 8, 'last_error': 'timeout'},
    ]

    mock_smtp_instance = MagicMock()
    with patch('smtplib.SMTP') as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
        send_sms_flagged_alert(flagged, config, engine=MagicMock())

    body = _plain_text_body(mock_smtp_instance.sendmail.call_args[0][2])
    assert 'UPDATE sms.queue' not in body
    assert '--resend' in body
    assert '--week 8' in body
    assert 'IBIS001' in body and 'IBIS002' in body
    assert '--actor' in body


def test_send_sms_flagged_alert_skips_when_no_recipients(tmp_path):
    from modules.notifier import send_sms_flagged_alert

    email_cfg = _make_email_cfg(tmp_path)  # no sms_dm_recipients
    config = _config(email_cfg)

    with patch('smtplib.SMTP') as mock_smtp_cls:
        send_sms_flagged_alert(
            [{'subjid': 'X', 'health_facility_ug': '11', 'week': 8, 'last_error': 'e'}],
            config, engine=MagicMock(),
        )

    mock_smtp_cls.assert_not_called()


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
        {'health_facility_ug': '11', 'week': 8, 'due': 6, 'submitted': 5, 'delivered': 4, 'undelivered': 1, 'pending': 0},
        {'health_facility_ug': '14', 'week': 8, 'due': 4, 'submitted': 3, 'delivered': 3, 'undelivered': 0, 'pending': 0},
    ]
    cumulative_rows = [
        {'health_facility_ug': '11', 'week': 8, 'due': 22, 'submitted': 20, 'delivered': 18, 'undelivered': 2, 'pending': 0},
    ]
    report = _build_weekly_sms_report(weekly_rows, cumulative_rows, '17 Apr 2026')

    assert '17 Apr 2026' in report
    assert 'This week' in report
    assert 'Cumulative' in report
    assert 'Bushenyi HCIV' in report
    assert 'Ruhoko HCIV' in report
    assert 'Total' in report
    assert 'Due' in report
    assert 'Sent' in report
    assert 'Failed' in report
    assert 'Pending' in report
    assert 'Submitted' not in report    # old label must be gone
    assert 'Undelivered' not in report  # old label must be gone


# ---------------------------------------------------------------------------
# Shared table-formatting helpers (_ordered_sites_present / _table_header /
# _table_separator) — extracted from what used to be four copy-pasted
# versions of the same "which sites appear, in canonical order" and
# "build the header/separator row" logic across the weekly SMS and
# follow-up table/DataFrame builders.
# ---------------------------------------------------------------------------

def test_ordered_sites_present_filters_and_preserves_canonical_order():
    from modules.notifier import _ordered_sites_present
    # '13' appears before '11' in the input rows, but canonical order (as
    # declared in _UG_SITE_NAMES) must win, and '12'/'14' (absent) must be excluded.
    rows = [{'health_facility_ug': '13'}, {'health_facility_ug': '11'}]
    assert _ordered_sites_present(rows) == ['11', '13']


def test_ordered_sites_present_supports_alternate_key():
    from modules.notifier import _ordered_sites_present
    rows = [{'site': '14'}]
    assert _ordered_sites_present(rows, key='site') == ['14']


def test_table_header_and_separator_widths_match():
    from modules.notifier import _table_header, _table_separator
    # col_w=17 matches production usage — large enough that no site name
    # exceeds it (format specs pad short strings but never truncate long
    # ones, so a too-small col_w would make header longer than sep).
    header = _table_header(label_w=10, col_w=17, site_codes=['11', '12'])
    sep = _table_separator(label_w=10, col_w=17, n_sites=2)
    assert len(header) == len(sep)
    assert header.rstrip().endswith('Total')


def test_build_weekly_sms_table_renders_full_grid():
    """Locks in the exact table shape (header/separator/row layout) the
    weekly SMS report email depends on — this format is relied on by field
    teams and must not shift silently when the builder is refactored."""
    from modules.notifier import _build_weekly_sms_table

    rows = [
        {'health_facility_ug': '11', 'week': 8, 'due': 10, 'submitted': 9,
         'delivered': 8, 'undelivered': 1, 'pending': 0},
        {'health_facility_ug': '12', 'week': 8, 'due': 5, 'submitted': 5,
         'delivered': 5, 'undelivered': 0, 'pending': 0},
    ]
    result = _build_weekly_sms_table(rows, 'This week')
    lines = result.split('\n')

    assert lines[0] == 'This week'
    assert len(lines[1]) == len(lines[2])  # separator width == header width
    assert 'Bushenyi HCIV' in lines[2]
    assert 'Ishaka Adv. Hosp' in lines[2]
    assert lines[2].rstrip().endswith('Total')
    assert any('Week 8' in l and 'Due' in l for l in lines)
    assert any('Delivered' in l for l in lines)


def test_build_followup_table_renders_full_grid():
    """Same guarantee as above for the follow-up tracking table."""
    from modules.notifier import _build_followup_table

    rows = [
        {'health_facility_ug': '11', 'entered_window': 10, 'primary_endpoint_done': 6,
         'done_not_due': 2, 'due_pending': 1, 'overdue': 1},
    ]
    result = _build_followup_table(rows)
    lines = result.split('\n')

    assert lines[0] == 'Follow-up Tracking — Uganda'
    assert len(lines[1]) == len(lines[2])  # separator width == header width
    assert 'Bushenyi HCIV' in lines[2]
    assert lines[2].rstrip().endswith('Total')
    assert any('Entered follow-up period' in l for l in lines)


def test_build_weekly_sms_table_no_activity():
    from modules.notifier import _build_weekly_sms_table
    result = _build_weekly_sms_table([], 'This week')
    assert 'No activity' in result


def test_build_weekly_sms_df_five_rows_per_week():
    from modules.notifier import _build_weekly_sms_df

    rows = [
        {
            'health_facility_ug': '11',
            'week': 8,
            'due': 6,
            'submitted': 5,
            'delivered': 4,
            'undelivered': 1,
            'pending': 0,
        }
    ]
    df = _build_weekly_sms_df(rows, 'This week (ending 22 Apr 2026)')

    labels = df[''].tolist()
    assert 'Due for 8wk SMS (n)'   in labels
    assert '  • Sent (n, %)'       in labels
    assert '  • Delivered (n, %)'  in labels
    assert '  • Failed (N, %)'     in labels
    assert '  • Pending (n, %)'    in labels
    # Sent % is over Due: 5/6 = 83.3%
    sent_row = df[df[''] == '  • Sent (n, %)'].iloc[0]
    assert sent_row['%'] == '83.3%'
    # Delivered % is over Sent: 4/5 = 80.0%
    del_row = df[df[''] == '  • Delivered (n, %)'].iloc[0]
    assert del_row['%'] == '80.0%'
    # Failed % is over Sent: 1/5 = 20.0%
    fail_row = df[df[''] == '  • Failed (N, %)'].iloc[0]
    assert fail_row['%'] == '20.0%'
    # Pending % is over Sent: 0/5 = 0.0%
    pend_row = df[df[''] == '  • Pending (n, %)'].iloc[0]
    assert pend_row['%'] == '0.0%'


def test_build_weekly_sms_df_due_row_has_no_pct():
    from modules.notifier import _build_weekly_sms_df

    rows = [{'health_facility_ug': '11', 'week': 8, 'due': 6,
             'submitted': 5, 'delivered': 4, 'undelivered': 1, 'pending': 0}]
    df = _build_weekly_sms_df(rows, 'Test')
    due_row = df[df[''] == 'Due for 8wk SMS (n)'].iloc[0]
    assert due_row['%'] == ''
    assert due_row['Total'] == 6



# ---------------------------------------------------------------------------
# send_missed_run_alert
# ---------------------------------------------------------------------------

def test_send_missed_run_alert_sends_email_listing_overdue_invocations(tmp_path):
    from datetime import datetime, timedelta, timezone
    from modules.notifier import send_missed_run_alert

    config = _config(_make_email_cfg(tmp_path))
    overdue = [{
        'invocation': '-a',
        'threshold': timedelta(hours=26),
        'last_seen': datetime(2026, 1, 1, tzinfo=timezone.utc),
    }]

    mock_smtp_instance = MagicMock()
    with patch('smtplib.SMTP') as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
        send_missed_run_alert(overdue, config)

    mock_smtp_instance.sendmail.assert_called_once()
    to_arg = mock_smtp_instance.sendmail.call_args[0][1]
    assert to_arg == ['admin@example.com']

    body = _plain_text_body(mock_smtp_instance.sendmail.call_args[0][2])
    assert '-a' in body
    assert '2026-01-01' in body


def test_send_missed_run_alert_mentions_never_seen_invocation(tmp_path):
    from modules.notifier import send_missed_run_alert

    config = _config(_make_email_cfg(tmp_path))
    overdue = [{'invocation': 'store_ibis', 'threshold': None, 'last_seen': None}]

    mock_smtp_instance = MagicMock()
    with patch('smtplib.SMTP') as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
        send_missed_run_alert(overdue, config)

    body = _plain_text_body(mock_smtp_instance.sendmail.call_args[0][2])
    assert 'store_ibis' in body
    assert 'never seen' in body


def test_send_missed_run_alert_no_op_when_no_email_config():
    from modules.notifier import send_missed_run_alert

    with patch('smtplib.SMTP') as mock_smtp_cls:
        send_missed_run_alert(
            [{'invocation': '-a', 'threshold': None, 'last_seen': None}],
            config={},
        )

    mock_smtp_cls.assert_not_called()


def test_send_missed_run_alert_does_not_raise_on_smtp_error(tmp_path):
    """SMTP failure is logged and swallowed — must not raise to the caller."""
    from modules.notifier import send_missed_run_alert

    config = _config(_make_email_cfg(tmp_path))
    overdue = [{'invocation': '-a', 'threshold': None, 'last_seen': None}]

    with patch('smtplib.SMTP', side_effect=smtplib.SMTPException('conn refused')):
        send_missed_run_alert(overdue, config)


def test_build_weekly_sms_df_zero_due_no_crash():
    """When due and submitted are 0, % cells and site cells should be empty strings."""
    from modules.notifier import _build_weekly_sms_df

    rows = [{'health_facility_ug': '11', 'week': 8, 'due': 0,
             'submitted': 0, 'delivered': 0, 'undelivered': 0, 'pending': 0}]
    df = _build_weekly_sms_df(rows, 'Test')
    sent_row = df[df[''] == '  • Sent (n, %)'].iloc[0]
    assert sent_row['%'] == ''
    assert sent_row['Bushenyi HCIV'] == ''
    for label in ['  • Delivered (n, %)', '  • Failed (N, %)', '  • Pending (n, %)']:
        row = df[df[''] == label].iloc[0]
        assert row['%'] == ''
        assert row['Bushenyi HCIV'] == ''
