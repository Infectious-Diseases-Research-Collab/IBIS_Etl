#!/usr/bin/env python3
"""Standalone IBIS SMS runner.

Usage:
    python sms.py                  # sync queue + send today's messages
    python sms.py --sync           # sync queue only, no sending
    python sms.py --dry-run        # log what would be sent, no actual send
    python sms.py --weekly-report  # send weekly facility report email
    python sms.py --init-db        # create SMS tables (run once after setup)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from modules.config import ConfigLoader
from modules.db import PipelineLockError, create_db_engine, init_schemas, init_sms_tables, pipeline_lock, run_migrations
from modules.logging_utils import configure_logging
from modules.metrics import finish_pipeline_run, record_stage_run, start_pipeline_run
from modules.notifier import send_sms_weekly_report
from modules.sms_processor import SmsProcessor
from stages.fetch_dlr import FetchDlr

configure_logging()
logger = logging.getLogger(__name__)

# Separate from ibis.py's lock: SMS operations (sync/send/DLR poll/reports)
# don't mutate the medallion schemas, but must be serialised against each
# other so an overrunning cron invocation doesn't overlap the next one.
SMS_LOCK_NAME = 'ibis_sms_pipeline'


def init_db(engine) -> None:
    """Create SMS tables (delegates to db.init_sms_tables)."""
    init_sms_tables(engine)
    logger.info("SMS tables created (or already existed).")


def main() -> None:
    parser = argparse.ArgumentParser(description='IBIS SMS standalone runner')
    parser.add_argument('--sync',            action='store_true', help='Sync queue only, no sending')
    parser.add_argument('--dry-run',         action='store_true', help='Log what would be sent, no actual send')
    parser.add_argument('--weekly-report',   action='store_true', help='Send weekly facility report email')
    parser.add_argument('--init-db',         action='store_true', help='Create SMS tables (run once at setup)')
    parser.add_argument('--check-delivery',  action='store_true',
                        help='Poll Blasta DLR for all unconfirmed sent messages')
    parser.add_argument('--resend', action='store_true',
                        help='Reset flagged/failed messages to pending so the next run '
                             'resends them. Audited (logged to sms.resend_log) — requires '
                             '--subjid, --week, and --actor.')
    parser.add_argument('--subjid', nargs='+', metavar='SUBJID',
                        help='Subject ID(s) to resend (used with --resend)')
    parser.add_argument('--week', type=int,
                        help='Week number of the messages to resend (used with --resend)')
    parser.add_argument('--actor',
                        help='Name or email of the person requesting the resend '
                             '(used with --resend; recorded in the audit log)')
    parser.add_argument('--note',
                        help='Optional note explaining the resend, e.g. "confirmed '
                             'number by phone" (used with --resend)')
    parser.add_argument('-v', '--verbose',   action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = ConfigLoader('config.json')
    engine = create_db_engine(config)

    try:
        with pipeline_lock(engine, SMS_LOCK_NAME):
            _run(args, config, engine)
    except PipelineLockError as exc:
        logger.error(str(exc))
        sys.exit(1)


def _run(args, config, engine) -> None:
    init_schemas(engine)  # ensures sms schema exists
    run_migrations(engine)  # ensures ops.pipeline_runs/stage_runs exist

    if args.init_db:
        invocation = 'sms --init-db'
    elif args.resend:
        invocation = 'sms --resend'
    elif args.check_delivery:
        invocation = 'sms --check-delivery'
    elif args.weekly_report:
        invocation = 'sms --weekly-report'
    elif args.sync:
        invocation = 'sms --sync'
    else:
        invocation = 'sms'
    try:
        pipeline_run_id = start_pipeline_run(engine, invocation)
    except Exception as exc:
        logger.exception(f"Failed to record pipeline_run start (metrics only, continuing): {exc}")
        pipeline_run_id = None
    started_at = datetime.now(timezone.utc)
    stage_name = 'sms_init_db'
    success = True
    rows_written = 0
    errors: list[str] = []

    try:
        if args.init_db:
            stage_name = 'sms_init_db'
            init_db(engine)
            return

        if args.resend:
            stage_name = 'sms_resend'
            missing = [
                name for name, val in
                [('--subjid', args.subjid), ('--week', args.week), ('--actor', args.actor)]
                if not val
            ]
            if missing:
                logger.error('--resend requires %s', ', '.join(missing))
                success = False
                errors = [f'--resend requires {", ".join(missing)}']
                sys.exit(1)
            processor = SmsProcessor(config=config, engine=engine)
            updated = processor.resend(
                args.subjid, args.week, actor=args.actor, note=args.note
            )
            rows_written = updated
            note_suffix = f' — note: {args.note}' if args.note else ''
            logger.info(
                "Resend by %s: %d queue row(s) reset to pending "
                "(week=%d, subjids=%s)%s",
                args.actor, updated, args.week, args.subjid, note_suffix,
            )
            return

        if args.check_delivery:
            # Delegates to the FetchDlr stage rather than duplicating its logic
            # here: FetchDlr also encodes the "fail only when every checked row
            # errored" success rule, which this branch previously didn't apply —
            # it always returned 0, so a fully-failed DLR poll would never be
            # visible via the cron job's exit code.
            stage_name = 'sms_check_delivery'
            stage = FetchDlr(config=config, engine=engine)
            result = stage.run()
            meta = result.metadata
            logger.info(
                'DLR check complete: checked=%d updated=%d pending=%d errors=%d',
                meta.get('checked', 0), meta.get('updated', 0),
                meta.get('pending', 0), len(meta.get('errors', [])),
            )
            if meta.get('flagged'):
                logger.info('%d message(s) flagged — alert sent to data manager.', meta['flagged'])
            success = result.success
            rows_written = meta.get('updated', 0)
            errors = result.errors
            sys.exit(0 if result.success else 1)

        if args.weekly_report:
            stage_name = 'sms_weekly_report'
            send_sms_weekly_report(engine, config)
            return

        stage_name = 'sms_send'

        if args.dry_run:
            sms_cfg = dict(config.get('sms') or {})
            sms_cfg['dry_run'] = True
            config.config['sms'] = sms_cfg

        processor = SmsProcessor(config=config, engine=engine)

        if args.sync:
            stage_name = 'sms_sync'
            inserted = processor.sync_queue()
            rows_written = inserted
            logger.info("Queue sync complete: %d new row(s) inserted.", inserted)
            return

        result = processor.run()
        rows_written = result.sent
        success = result.failed == 0
        errors = [f['error'] for f in result.failures]
        logger.info(
            "SMS run complete — sent: %d  failed: %d  skipped: %d",
            result.sent, result.failed, result.skipped,
        )
        if result.failures:
            logger.warning("Failed messages:")
            for f in result.failures:
                logger.warning(
                    "  subjid=%s  mobile=%s  week=%d: %s",
                    f['subjid'], f['mobile_number'], f['week'], f['error'],
                )
        sys.exit(1 if result.failed > 0 else 0)
    except Exception as exc:
        success = False
        errors = errors + [str(exc)]
        logger.exception(f"Unexpected error in sms.py ({stage_name}): {exc}")
        raise
    finally:
        if pipeline_run_id is not None:
            try:
                record_stage_run(
                    engine, pipeline_run_id, stage_name, started_at,
                    success=success, rows_written=rows_written, errors=errors,
                )
            except Exception as exc:
                logger.exception(f"Failed to record stage_run (metrics only, continuing): {exc}")
            try:
                finish_pipeline_run(
                    engine, pipeline_run_id,
                    success=success, rows_written=rows_written, error_count=len(errors),
                )
            except Exception as exc:
                logger.exception(f"Failed to record pipeline_run finish (metrics only, continuing): {exc}")


if __name__ == '__main__':
    main()
