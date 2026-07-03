from __future__ import annotations

from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from sms import _run
from stages.base import StageResult


def _args(**overrides):
    defaults = dict(
        sync=False, dry_run=False, weekly_report=False, init_db=False,
        check_delivery=False, resend=False, subjid=None, week=None,
        actor=None, note=None, verbose=False,
    )
    defaults.update(overrides)
    return Namespace(**defaults)


def test_check_delivery_delegates_to_fetch_dlr_stage():
    """--check-delivery must run the FetchDlr stage (not duplicate its logic
    inline) so the "fail only when every checked row errored" rule and the
    metadata it returns are the single source of truth."""
    config = MagicMock()
    engine = MagicMock()

    with patch('sms.FetchDlr') as MockFetchDlr:
        MockFetchDlr.return_value.run.return_value = StageResult(
            success=True, rows_written=3,
            metadata={'checked': 3, 'updated': 3, 'pending': 0, 'errors': [], 'flagged': 0},
        )
        with pytest.raises(SystemExit) as exc_info:
            _run(_args(check_delivery=True), config, engine)

    MockFetchDlr.assert_called_once_with(config=config, engine=engine)
    MockFetchDlr.return_value.run.assert_called_once()
    assert exc_info.value.code == 0


def test_check_delivery_exits_nonzero_when_stage_fails():
    config = MagicMock()
    engine = MagicMock()

    with patch('sms.FetchDlr') as MockFetchDlr:
        MockFetchDlr.return_value.run.return_value = StageResult(
            success=False, rows_written=0,
            metadata={'checked': 2, 'updated': 0, 'pending': 0, 'errors': [{'log_id': 1}, {'log_id': 2}], 'flagged': 0},
            errors=['all 2 checked row(s) errored'],
        )
        with pytest.raises(SystemExit) as exc_info:
            _run(_args(check_delivery=True), config, engine)

    assert exc_info.value.code == 1
