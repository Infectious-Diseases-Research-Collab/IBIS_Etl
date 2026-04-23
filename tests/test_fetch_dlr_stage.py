from __future__ import annotations

from unittest.mock import MagicMock, patch


def make_engine_mock():
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def make_config():
    cfg = MagicMock()
    cfg.get.return_value = {
        'blasta_ini': 'secrets/BLASTA.ini',
        'blasta_key': 'secrets/BLASTA.key',
        'max_retries': 3,
        'dry_run': False,
        'countrycode': '1',
    }
    return cfg


def test_fetch_dlr_stage_returns_success_when_no_errors():
    from stages.fetch_dlr import FetchDlr
    from modules.sms_processor import DlrResult

    engine = make_engine_mock()
    stage = FetchDlr(config=make_config(), engine=engine)

    with patch('stages.fetch_dlr.SmsProcessor') as MockProc, \
         patch('stages.fetch_dlr.send_sms_flagged_alert') as mock_alert:
        MockProc.return_value.fetch_delivery_statuses.return_value = DlrResult(
            checked=5, updated=4, pending=1, errors=[]
        )
        MockProc.return_value.get_flagged_messages.return_value = []
        result = stage.run()

    assert result.success is True
    assert result.rows_written == 4
    mock_alert.assert_not_called()


def test_fetch_dlr_stage_sends_alert_when_flagged_messages_exist():
    from stages.fetch_dlr import FetchDlr
    from modules.sms_processor import DlrResult

    engine = make_engine_mock()
    cfg = make_config()
    stage = FetchDlr(config=cfg, engine=engine)

    flagged = [{'subjid': 'IBIS001', 'health_facility_ug': '14', 'week': 8, 'last_error': 'timeout'}]

    with patch('stages.fetch_dlr.SmsProcessor') as MockProc, \
         patch('stages.fetch_dlr.send_sms_flagged_alert') as mock_alert:
        MockProc.return_value.fetch_delivery_statuses.return_value = DlrResult(
            checked=1, updated=0, pending=0, errors=[]
        )
        MockProc.return_value.get_flagged_messages.return_value = flagged
        result = stage.run()

    mock_alert.assert_called_once_with(flagged, cfg, engine)


def test_fetch_dlr_stage_returns_failure_when_all_errored():
    from stages.fetch_dlr import FetchDlr
    from modules.sms_processor import DlrResult

    engine = make_engine_mock()
    stage = FetchDlr(config=make_config(), engine=engine)

    with patch('stages.fetch_dlr.SmsProcessor') as MockProc, \
         patch('stages.fetch_dlr.send_sms_flagged_alert'):
        MockProc.return_value.fetch_delivery_statuses.return_value = DlrResult(
            checked=2, updated=0, pending=0,
            errors=[{'log_id': 1, 'error': 'timeout'}, {'log_id': 2, 'error': 'timeout'}],
        )
        MockProc.return_value.get_flagged_messages.return_value = []
        result = stage.run()

    assert result.success is False
