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
