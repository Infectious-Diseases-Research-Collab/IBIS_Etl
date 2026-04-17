from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from cryptography.fernet import Fernet


# ---------------------------------------------------------------------------
# Helpers used across tests
# ---------------------------------------------------------------------------

def make_engine_mock(fetchall_return=None, rowcount=0):
    """Return a mock SQLAlchemy engine whose context managers work."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = fetchall_return or []
    conn.execute.return_value.rowcount = rowcount

    # engine.connect() as conn
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # engine.begin() as conn
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine, conn


def make_config(dry_run=False):
    cfg = MagicMock()
    cfg.get.return_value = {
        'blasta_ini': 'secrets/BLASTA.ini',
        'blasta_key': 'secrets/BLASTA.key',
        'max_retries': 3,
        'dry_run': dry_run,
    }
    return cfg


# ---------------------------------------------------------------------------
# load_templates_from_excel
# ---------------------------------------------------------------------------

def test_load_templates_from_excel_reads_all_languages(tmp_path):
    """Each xlsx file produces templates for both weeks."""
    import openpyxl
    from scripts.seed_sms_templates import load_templates_from_excel

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Arm', 'Wk 8 SMS', 'Wk 11 SMS'])
    ws.append(['HIV Risk Assessment', 'Week 8 message', 'Week 11 message'])
    wb.save(tmp_path / 'English.xlsx')

    templates = load_templates_from_excel(str(tmp_path))

    assert len(templates) == 2
    assert templates[0] == {
        'arm': 'HIV Risk Assessment',
        'language': 'English',
        'week': 8,
        'message_text': 'Week 8 message',
        'has_placeholder': False,
    }
    assert templates[1]['week'] == 11


def test_load_templates_detects_placeholder(tmp_path):
    import openpyxl
    from scripts.seed_sms_templates import load_templates_from_excel

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Arm', 'Wk 8 SMS', 'Wk 11 SMS'])
    ws.append(['Default appointment setting', 'Your appt is [date here]', 'Follow up on [date]'])
    wb.save(tmp_path / 'English.xlsx')

    templates = load_templates_from_excel(str(tmp_path))
    assert all(t['has_placeholder'] for t in templates)


def test_load_templates_skips_blank_arms(tmp_path):
    import openpyxl
    from scripts.seed_sms_templates import load_templates_from_excel

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Arm', 'Wk 8 SMS', 'Wk 11 SMS'])
    ws.append([None, 'msg', 'msg'])  # blank arm — should be skipped
    ws.append(['HIV Risk Assessment', 'msg', 'msg'])
    wb.save(tmp_path / 'English.xlsx')

    templates = load_templates_from_excel(str(tmp_path))
    assert len(templates) == 2
