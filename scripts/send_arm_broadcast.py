#!/usr/bin/env python3
"""Send an ad-hoc, per-language broadcast SMS to every participant of a study arm.

Dry run by default (no messages sent, nothing written to sms.broadcast_log).
Pass --send to actually send, which requires confirming the exact recipient
count when prompted.

Usage:
    python scripts/send_arm_broadcast.py --arm Incentive \
        --messages-file data/sms_broadcasts/2026-08-incentive-pause.json \
        --actor "Emmanuel"

    python scripts/send_arm_broadcast.py --arm Incentive \
        --messages-file data/sms_broadcasts/2026-08-incentive-pause.json \
        --actor "Emmanuel" --send
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.config import ConfigLoader
from modules.db import create_db_engine, init_schemas, init_sms_tables
from modules.logging_utils import configure_logging
from modules.sms_processor import BlastaClient, _LANGUAGE_MAP, _load_blasta_creds

configure_logging()
logger = logging.getLogger(__name__)

RECIPIENTS_QUERY = """
    SELECT subjid, mobile_number, preferred_language_text
    FROM ibis.baseline
    WHERE countrycode::integer = :countrycode
      AND consent::integer = 1
      AND subjid IS NOT NULL
      AND mobile_number IS NOT NULL
      AND arm_text = :arm
      AND subjid NOT IN (SELECT subjid FROM sms.opt_outs)
    ORDER BY subjid
"""


def resolve_recipients(
    engine, arm: str, countrycode: str, messages: dict[str, str],
) -> tuple[list[dict], list[dict]]:
    """Query arm participants and resolve each one's message by language.

    Returns (resolved, skipped). `resolved` items have subjid, mobile_number,
    language, message_text. `skipped` items (no message for their normalized
    language) have subjid, mobile_number, preferred_language_text.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(RECIPIENTS_QUERY), {"arm": arm, "countrycode": countrycode}
        ).fetchall()

    resolved: list[dict] = []
    skipped: list[dict] = []
    for row in rows:
        raw_language = (row.preferred_language_text or '').strip()
        canonical = _LANGUAGE_MAP.get(raw_language.lower())
        message_text = messages.get(canonical) if canonical else None
        if message_text is None:
            skipped.append({
                'subjid': row.subjid,
                'mobile_number': str(row.mobile_number),
                'preferred_language_text': raw_language,
            })
            continue
        resolved.append({
            'subjid': row.subjid,
            'mobile_number': str(row.mobile_number),
            'language': canonical,
            'message_text': message_text,
        })
    return resolved, skipped


def print_summary(resolved: list[dict], skipped: list[dict]) -> None:
    counts: dict[str, int] = {}
    for r in resolved:
        counts[r['language']] = counts.get(r['language'], 0) + 1

    print(f"\nRecipients: {len(resolved)} (skipped: {len(skipped)})")
    for language, count in sorted(counts.items()):
        print(f"  {language}: {count}")
        sample = next(r for r in resolved if r['language'] == language)
        print(f"    e.g. {sample['subjid']} -> {sample['message_text'][:80]}")

    if skipped:
        print("\nSkipped (no message for their language):")
        for s in skipped:
            print(f"  {s['subjid']} (preferred_language_text={s['preferred_language_text']!r})")


def log_result(
    engine, *, arm: str, subjid: str, mobile_number: str, language: str,
    message_text: str, status: str, provider_message_id: str | None,
    error_message: str | None, actor: str, sent_at,
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sms.broadcast_log
                (arm, subjid, mobile_number, language, message_text,
                 status, provider_message_id, error_message, actor, sent_at)
            VALUES
                (:arm, :subjid, :mobile_number, :language, :message_text,
                 :status, :provider_message_id, :error_message, :actor, :sent_at)
        """), {
            "arm": arm, "subjid": subjid, "mobile_number": mobile_number,
            "language": language, "message_text": message_text, "status": status,
            "provider_message_id": provider_message_id, "error_message": error_message,
            "actor": actor, "sent_at": sent_at,
        })


def send_broadcast(
    engine, config, arm: str, resolved: list[dict], actor: str,
) -> tuple[int, int]:
    """Send every resolved recipient's message via Blasta. One failure does
    not stop the rest. Every attempt (sent or failed) is logged. Returns
    (sent_count, failed_count)."""
    sms_cfg = config.get('sms') or {}
    username, password = _load_blasta_creds(sms_cfg['blasta_ini'], sms_cfg.get('blasta_key'))
    client = BlastaClient(username, password, sms_cfg.get('max_retries', 3))

    sent = 0
    failed = 0
    for r in resolved:
        provider_msg_id = None
        error_msg = None
        status = 'failed'
        sent_at = None
        try:
            response = client.send(r['mobile_number'], r['message_text'])
            provider_msg_id = response.get('msg_id')
            status = 'sent'
            sent_at = datetime.now(timezone.utc)
            sent += 1
            logger.info("Sent to %s (%s) msg_id=%s", r['subjid'], r['language'], provider_msg_id)
        except Exception as exc:
            error_msg = str(exc)
            failed += 1
            logger.error("Failed to send to %s (%s): %s", r['subjid'], r['language'], exc)

        log_result(
            engine, arm=arm, subjid=r['subjid'], mobile_number=r['mobile_number'],
            language=r['language'], message_text=r['message_text'], status=status,
            provider_message_id=provider_msg_id, error_message=error_msg,
            actor=actor, sent_at=sent_at,
        )

    return sent, failed


def _run(args, config, engine) -> None:
    messages = json.loads(Path(args.messages_file).read_text(encoding='utf-8'))

    init_schemas(engine)
    init_sms_tables(engine)

    sms_cfg = config.get('sms') or {}
    countrycode = sms_cfg.get('countrycode', '1')

    resolved, skipped = resolve_recipients(engine, args.arm, countrycode, messages)
    print_summary(resolved, skipped)

    if not args.send:
        print("\nDry run only — nothing sent. Re-run with --send to actually send.")
        return

    if not resolved:
        print("\nNo recipients to send to. Exiting.")
        sys.exit(0)

    confirm = input(
        f"\nType the recipient count ({len(resolved)}) to confirm sending: "
    ).strip()
    if confirm != str(len(resolved)):
        print("Confirmation did not match recipient count — aborting, nothing sent.")
        sys.exit(1)

    sent, failed = send_broadcast(engine, config, args.arm, resolved, args.actor)
    print(f"\nDone — sent: {sent}  failed: {failed}  skipped: {len(skipped)}")
    sys.exit(1 if failed > 0 else 0)


def main() -> None:
    parser = argparse.ArgumentParser(description='Send an ad-hoc broadcast SMS to a study arm')
    parser.add_argument('--arm', required=True,
                         help="Study arm, e.g. 'Incentive' (matches ibis.baseline.arm_text)")
    parser.add_argument('--messages-file', required=True,
                         help='Path to JSON file: {"English": "...", "Luganda": "...", "Runyankole": "..."}')
    parser.add_argument('--actor', required=True,
                         help='Name/email of the person requesting this broadcast '
                              '(recorded in sms.broadcast_log)')
    parser.add_argument('--send', action='store_true', help='Actually send (default is dry run)')
    args = parser.parse_args()

    config = ConfigLoader('config.json')
    engine = create_db_engine(config)
    _run(args, config, engine)


if __name__ == '__main__':
    main()
