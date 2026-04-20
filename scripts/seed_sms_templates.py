#!/usr/bin/env python3
"""Seed sms.templates from Excel files in data/sms_messages/.

Run from project root after init_sms_schema.sql has been applied:
    python scripts/seed_sms_templates.py

Re-run whenever message content in the Excel files changes.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import openpyxl
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.config import ConfigLoader
from modules.db import create_db_engine


def load_templates_from_excel(messages_dir: str) -> list[dict]:
    """Parse all .xlsx files in messages_dir. Filename (without extension) = language.

    Expected column layout: col 1 = arm name, col 2 = week-8 message, col 3 = week-11 message.
    Row 1 is the header and is skipped (min_row=2).
    """
    templates = []
    for xlsx_path in sorted(Path(messages_dir).glob('*.xlsx')):
        language = xlsx_path.stem
        wb = openpyxl.load_workbook(xlsx_path)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            arm = row[0]
            if not arm or not isinstance(arm, str) or not arm.strip():
                continue
            for week, message in [(8, row[1]), (11, row[2])]:
                if not message:
                    continue
                message = re.sub(r'[ \t]+', ' ', str(message)).strip()
                templates.append({
                    'arm': arm.strip(),
                    'language': language,
                    'week': week,
                    'message_text': message,
                    'has_placeholder': bool(re.search(r'\[.*?\]', message)),
                })
    return templates


def seed_templates(engine, templates: list[dict]) -> int:
    """Upsert templates into sms.templates. Returns number of rows upserted."""
    with engine.begin() as conn:
        for t in templates:
            conn.execute(text("""
                INSERT INTO sms.templates (arm, language, week, message_text, has_placeholder)
                VALUES (:arm, :language, :week, :message_text, :has_placeholder)
                ON CONFLICT (arm, language, week)
                DO UPDATE SET
                    message_text    = EXCLUDED.message_text,
                    has_placeholder = EXCLUDED.has_placeholder
            """), t)
    return len(templates)


if __name__ == '__main__':
    config = ConfigLoader('config.json')
    engine = create_db_engine(config)
    sms_cfg = config.get('sms') or {}
    templates = load_templates_from_excel(sms_cfg['messages_dir'])
    count = seed_templates(engine, templates)
    print(f"Seeded {count} template(s) into sms.templates")
