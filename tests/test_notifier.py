from __future__ import annotations

import smtplib
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from cryptography.fernet import Fernet
from modules.notifier import _load_smtp_credentials, _should_notify, _query_validation_report, _build_stage_summary, _build_validation_section, send_pipeline_report
from stages.base import StageResult


def test_load_smtp_credentials_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(
        f"Username={cipher.encrypt(b'user@example.com').decode()}\n"
        f"Password={cipher.encrypt(b's3cr3t').decode()}\n"
    )

    username, password = _load_smtp_credentials(str(ini_file), str(key_file))
    assert username == 'user@example.com'
    assert password == 's3cr3t'


def test_load_smtp_credentials_missing_username_raises(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Password={cipher.encrypt(b's3cr3t').decode()}\n")

    with pytest.raises(KeyError, match='Username'):
        _load_smtp_credentials(str(ini_file), str(key_file))


def test_load_smtp_credentials_missing_password_raises(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Username={cipher.encrypt(b'user@example.com').decode()}\n")

    with pytest.raises(KeyError, match='Password'):
        _load_smtp_credentials(str(ini_file), str(key_file))


def test_should_notify_true_on_stage_failure():
    results = {
        'mdb_to_bronze': StageResult(success=False, errors=['boom']),
        'bronze_to_silver': StageResult(success=True, rows_written=100),
    }
    engine = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, engine) is True


def test_should_notify_true_on_error_in_report():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['ERROR', 'WARNING'], 'check': ['a', 'b']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is True


def test_should_notify_false_on_clean_run():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['WARNING'], 'check': ['a']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is False


def test_should_notify_false_when_report_is_none():
    results = {'mdb_to_bronze': StageResult(success=True)}
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, MagicMock()) is False


def test_query_validation_report_returns_none_on_db_error():
    with patch('pandas.read_sql', side_effect=Exception('connection refused')):
        result = _query_validation_report(MagicMock())
    assert result is None


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
    stages = ['transform_ibis']
    text = _build_stage_summary(results, stages)
    assert '✓' in text
    assert '0' not in text


def test_build_validation_section_none_engine():
    text = _build_validation_section(None)
    assert 'unavailable' in text.lower()


def test_build_validation_section_errors_and_warnings():
    report = pd.DataFrame({
        'severity':        ['ERROR', 'ERROR', 'WARNING', 'WARNING', 'WARNING'],
        'check':           ['dup_id', 'dup_id', 'missing_appt', 'missing_appt', 'sparse_col'],
        'country':         ['kenya', 'kenya', 'uganda', 'uganda', 'kenya'],
        'site':            ['21 (X)', '21 (X)', '11 (Y)', '11 (Y)', '21 (X)'],
        'record_count':    [2, 1, 3, 1, 5],
        'affected_subjids':['P001,P002', 'P003', 'U001,U002,U003', 'U004', ''],
    })
    text = _build_validation_section(report)
    assert 'Validation Errors' in text
    assert 'kenya / 21 (X)' in text
    assert 'P001, P002' in text
    assert 'Warnings (summary)' in text
    assert 'missing_appt' in text
    assert '3 warning(s)' in text


def test_build_validation_section_truncates_ids():
    ids = ','.join([f'P{i:03d}' for i in range(15)])
    report = pd.DataFrame({
        'severity':        ['ERROR'],
        'check':           ['dup_id'],
        'country':         ['kenya'],
        'site':            ['21 (X)'],
        'record_count':    [15],
        'affected_subjids':[ids],
    })
    text = _build_validation_section(report)
    assert '… and 5 more' in text


def _make_email_cfg(tmp_path):
    """Build a minimal email config with real Fernet credentials."""
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(
        f"Username={cipher.encrypt(b'user@example.com').decode()}\n"
        f"Password={cipher.encrypt(b's3cr3t').decode()}\n"
    )
    return {
        'smtp_host': 'smtp.example.com',
        'smtp_port': 587,
        'sender': 'ibis@example.com',
        'recipients': ['dm@example.com', 'pi@example.com'],
        'keyfiles': {
            'smtp_ini': str(ini_file),
            'smtp_key': str(key_file),
        },
    }


class _FakeConfig:
    def __init__(self, email_cfg):
        self._email_cfg = email_cfg

    def get(self, key, default=None):
        return self._email_cfg if key == 'email' else default


def test_send_pipeline_report_no_config_is_silent():
    """No email config → function returns without error."""
    config = _FakeConfig(None)
    send_pipeline_report(
        results={'mdb_to_bronze': StageResult(success=False)},
        stages=['mdb_to_bronze'],
        engine=MagicMock(),
        config=config,
    )
    # No exception = pass


def test_send_pipeline_report_clean_run_no_email(tmp_path):
    """Clean run with no ERRORs → no email sent."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['WARNING'], 'check': ['a']})

    with patch('modules.notifier._query_validation_report', return_value=report):
        with patch('smtplib.SMTP') as mock_smtp:
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )
    mock_smtp.assert_not_called()


def test_send_pipeline_report_sends_on_failure(tmp_path):
    """Stage failure → email is sent to all recipients."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False, errors=['boom'])}

    mock_smtp_instance = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP') as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )

    mock_smtp_instance.starttls.assert_called_once()
    mock_smtp_instance.login.assert_called_once_with('user@example.com', 's3cr3t')
    sendmail_args = mock_smtp_instance.sendmail.call_args
    recipients_arg = sendmail_args[0][1]
    assert set(recipients_arg) == {'dm@example.com', 'pi@example.com'}


def test_send_pipeline_report_does_not_raise_on_smtp_error(tmp_path):
    """SMTP failure is logged and swallowed — pipeline is unaffected."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False)}

    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP', side_effect=smtplib.SMTPException('conn refused')):
            # Must not raise
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )
