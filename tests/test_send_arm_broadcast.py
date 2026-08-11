from __future__ import annotations

from collections import namedtuple
from unittest.mock import MagicMock

import pytest

Row = namedtuple('Row', ['subjid', 'mobile_number', 'preferred_language_text'])


def make_engine_mock(fetchall_return=None):
    """Mock SQLAlchemy engine whose `with engine.connect() as conn:` works."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = fetchall_return or []
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


MESSAGES = {
    'English': 'Pause notice (EN)',
    'Luganda': 'Pause notice (LG)',
    'Runyankole': 'Pause notice (NY)',
}


def test_resolve_recipients_maps_language_to_message():
    from scripts.send_arm_broadcast import resolve_recipients

    engine, conn = make_engine_mock([
        Row('S1', '256700000001', 'Runyonkole'),
        Row('S2', '256700000002', 'Luganda'),
        Row('S3', '256700000003', 'english'),
    ])

    resolved, skipped = resolve_recipients(engine, 'Incentive', '1', MESSAGES)

    assert skipped == []
    assert resolved == [
        {'subjid': 'S1', 'mobile_number': '256700000001', 'language': 'Runyankole',
         'message_text': 'Pause notice (NY)'},
        {'subjid': 'S2', 'mobile_number': '256700000002', 'language': 'Luganda',
         'message_text': 'Pause notice (LG)'},
        {'subjid': 'S3', 'mobile_number': '256700000003', 'language': 'English',
         'message_text': 'Pause notice (EN)'},
    ]


def test_resolve_recipients_skips_unmapped_language():
    from scripts.send_arm_broadcast import resolve_recipients

    engine, conn = make_engine_mock([
        Row('S1', '256700000001', 'Runyonkole'),
        Row('S9', '256700000009', 'French'),
        Row('S10', '256700000010', None),
    ])

    resolved, skipped = resolve_recipients(engine, 'Incentive', '1', MESSAGES)

    assert len(resolved) == 1
    assert resolved[0]['subjid'] == 'S1'
    assert skipped == [
        {'subjid': 'S9', 'mobile_number': '256700000009', 'preferred_language_text': 'French'},
        {'subjid': 'S10', 'mobile_number': '256700000010', 'preferred_language_text': ''},
    ]


def test_resolve_recipients_skips_language_with_no_matching_message_key():
    """A language _LANGUAGE_MAP knows about, but the caller didn't supply a
    message for, is skipped — never sent a wrong-language fallback."""
    from scripts.send_arm_broadcast import resolve_recipients

    engine, conn = make_engine_mock([
        Row('S1', '256700000001', 'Luganda'),
    ])
    messages_missing_luganda = {'English': 'Pause notice (EN)'}

    resolved, skipped = resolve_recipients(engine, 'Incentive', '1', messages_missing_luganda)

    assert resolved == []
    assert skipped == [
        {'subjid': 'S1', 'mobile_number': '256700000001', 'preferred_language_text': 'Luganda'},
    ]


def test_print_summary_runs_without_error(capsys):
    from scripts.send_arm_broadcast import print_summary

    resolved = [
        {'subjid': 'S1', 'mobile_number': '256700000001', 'language': 'English',
         'message_text': 'Pause notice (EN)'},
    ]
    skipped = [
        {'subjid': 'S9', 'mobile_number': '256700000009', 'preferred_language_text': 'French'},
    ]
    print_summary(resolved, skipped)
    out = capsys.readouterr().out
    assert 'Recipients: 1' in out
    assert 'skipped: 1' in out
    assert 'S9' in out


def test_send_broadcast_logs_sent_and_failed(monkeypatch):
    from scripts.send_arm_broadcast import send_broadcast

    engine, conn = make_engine_mock()
    config = MagicMock()
    config.get.return_value = {
        'blasta_ini': 'secrets/BLASTA.ini', 'blasta_key': 'secrets/BLASTA.key',
        'max_retries': 3,
    }

    fake_client = MagicMock()
    fake_client.send.side_effect = [
        {'msg_id': 'MID1'},
        Exception('Blasta API error: insufficient credits'),
    ]

    monkeypatch.setattr(
        'scripts.send_arm_broadcast._load_blasta_creds',
        lambda ini, key: ('user', 'pass'),
    )
    monkeypatch.setattr(
        'scripts.send_arm_broadcast.BlastaClient',
        lambda username, password, max_retries: fake_client,
    )

    resolved = [
        {'subjid': 'S1', 'mobile_number': '256700000001', 'language': 'English',
         'message_text': 'msg1'},
        {'subjid': 'S2', 'mobile_number': '256700000002', 'language': 'Luganda',
         'message_text': 'msg2'},
    ]

    sent, failed = send_broadcast(engine, config, 'Incentive', resolved, actor='tester')

    assert (sent, failed) == (1, 1)
    assert fake_client.send.call_count == 2

    insert_calls = [
        c for c in conn.execute.call_args_list
        if 'INSERT INTO sms.broadcast_log' in str(c.args[0])
    ]
    assert len(insert_calls) == 2

    first_params = insert_calls[0].args[1]
    assert first_params['subjid'] == 'S1'
    assert first_params['status'] == 'sent'
    assert first_params['provider_message_id'] == 'MID1'
    assert first_params['actor'] == 'tester'

    second_params = insert_calls[1].args[1]
    assert second_params['subjid'] == 'S2'
    assert second_params['status'] == 'failed'
    assert 'insufficient credits' in second_params['error_message']
