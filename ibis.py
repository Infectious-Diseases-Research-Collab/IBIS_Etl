from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone

from modules.config import ConfigLoader
from modules.db import (
    PipelineLockError,
    create_db_engine,
    init_readonly_role,
    init_schemas,
    init_sms_tables,
    pipeline_lock,
    run_migrations,
)
from modules.logging_utils import configure_logging
from modules.metrics import finish_pipeline_run, record_stage_run, start_pipeline_run
from stages.base import StageResult

from stages.ftp_to_extracted import FtpToExtracted
from stages.mdb_to_bronze import MdbToBronze
from stages.bronze_to_silver import BronzeToSilver
from stages.transform_ibis import TransformIbis
from stages.measures_ibis import MeasuresIbis
from stages.promote_ibis import PromoteIbis
from stages.store_ibis import StoreIbis
from stages.send_sms import SendSms
from stages.reconcile_silver import ReconcileSilver

from modules.notifier import send_pipeline_report

configure_logging()
logger = logging.getLogger(__name__)

STAGE_CLASSES = {
    'ftp_to_extracted': FtpToExtracted,
    'mdb_to_bronze':    MdbToBronze,
    'bronze_to_silver': BronzeToSilver,
    'transform_ibis':   TransformIbis,
    'measures_ibis':    MeasuresIbis,
    'promote_ibis':     PromoteIbis,
    'store_ibis':       StoreIbis,
    'send_sms':         SendSms,
}

STAGE_DEPS = {name: cls.dependencies for name, cls in STAGE_CLASSES.items()}

# Stages runnable via -p but deliberately excluded from -a / the dependency
# graph — they're driven by their own cron schedule, not the nightly run.
STANDALONE_STAGE_CLASSES = {
    'reconcile_silver': ReconcileSilver,
}

# Precomputed once at import time (before any test patches STAGE_CLASSES[x].run)
# rather than introspected per-call on stage.run — a per-call check against
# stage.run.__code__ breaks when tests mock run via patch.object(), since
# MagicMock doesn't fabricate a real __code__ matching the original function.
STAGE_ACCEPTS_FULL_REBUILD = {
    name: 'full_rebuild' in cls.run.__code__.co_varnames
    for name, cls in STAGE_CLASSES.items()
}

# Shared across `-a` and `-p <stage>` invocations — a single-stage run (e.g.
# store_ibis) still touches schemas a full run writes to, so both must be
# serialised against each other, not just against themselves.
PIPELINE_LOCK_NAME = 'ibis_etl_pipeline'


def topological_sort(deps: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm. Returns stages in a valid execution order."""
    in_degree: dict[str, int] = defaultdict(int)
    graph: dict[str, list[str]] = defaultdict(list)

    for name in deps:
        in_degree.setdefault(name, 0)
        for dep in deps[name]:
            graph[dep].append(name)
            in_degree[name] += 1

    queue = deque(n for n in deps if in_degree[n] == 0)
    order: list[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return order


def build_run_list(
    deps: dict[str, list[str]],
    *,
    run_all: bool,
    pipeline: str | None = None,
) -> list[str]:
    if run_all:
        return topological_sort(deps)
    if pipeline not in deps and pipeline not in STANDALONE_STAGE_CLASSES:
        logger.error(f"Unknown stage '{pipeline}'. Valid stages: {sorted(deps) + sorted(STANDALONE_STAGE_CLASSES)}")
        sys.exit(1)
    return [pipeline]


def run_pipeline(
    stages: list[str], config: ConfigLoader, engine, full_rebuild: bool = False,
    invocation: str = '',
) -> None:
    results: dict[str, StageResult] = {}
    failed: set[str] = set()

    pipeline_run_id = start_pipeline_run(engine, invocation)

    for name in stages:
        cls = STAGE_CLASSES.get(name) or STANDALONE_STAGE_CLASSES[name]
        blocked_by = [d for d in cls.dependencies if d in failed]
        if blocked_by:
            logger.warning(f"Skipping '{name}' — upstream failure(s): {blocked_by}")
            failed.add(name)
            continue

        logger.info(f"=== Running stage: {name} ===")
        stage = cls(config=config, engine=engine)
        started_at = datetime.now(timezone.utc)
        try:
            if full_rebuild and STAGE_ACCEPTS_FULL_REBUILD.get(name, False):
                result = stage.run(full_rebuild=True)
            else:
                result = stage.run()
        except Exception as exc:
            result = StageResult(success=False, errors=[str(exc)])
            logger.exception(f"Stage '{name}' raised an unexpected exception.")

        record_stage_run(
            engine, pipeline_run_id, name, started_at,
            success=result.success, rows_written=result.rows_written, errors=result.errors,
        )

        results[name] = result
        if not result.success:
            failed.add(name)
            for err in result.errors:
                logger.error(f"  [{name}] {err}")
        else:
            logger.info(f"  [{name}] OK — {result.rows_written} row(s) written.")

    total_rows = sum(r.rows_written for r in results.values())
    total_errors = sum(len(r.errors) for r in results.values())
    finish_pipeline_run(
        engine, pipeline_run_id,
        success=len(failed) == 0, rows_written=total_rows, error_count=total_errors,
    )

    _log_summary(results, failed)
    send_pipeline_report(results=results, stages=stages, engine=engine, config=config)
    if failed:
        sys.exit(1)


def _log_summary(results: dict[str, StageResult], failed: set[str]) -> None:
    logger.info('=== Pipeline Run Summary ===')
    for name, result in results.items():
        status = 'FAILED' if name in failed else 'OK'
        logger.info(f"  {status:6s}  {name}  ({result.rows_written} rows)")
    skipped = [n for n in STAGE_CLASSES if n not in results]
    for name in skipped:
        logger.info(f"  SKIP    {name}")
    if not failed:
        logger.info('Result: SUCCESS')
    else:
        logger.warning(f'Result: FAILED ({len(failed)} stage(s))')


def _maybe_init_readonly_role(config: ConfigLoader, engine) -> None:
    """Provision the read-only reporting role if a password secret is configured.
    Opt-in: skipped (with a one-time log line) when not configured, so this
    is safe to introduce without forcing every deployment to set it up."""
    secret_file = (config.get('db') or {}).get('readonly_password_secret_file')
    if not secret_file:
        logger.debug(
            "db.readonly_password_secret_file not set — skipping read-only "
            "role provisioning."
        )
        return
    with open(secret_file) as f:
        password = f.read().strip()
    init_readonly_role(engine, password)


def main() -> None:
    parser = argparse.ArgumentParser(description='IBIS ETL orchestrator')
    parser.add_argument('-p', '--pipeline', help='Run a single named stage')
    parser.add_argument('-a', '--all', action='store_true', help='Run all stages')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument(
        '--full-rebuild', action='store_true',
        help='Recovery path: re-clean ALL of bronze from scratch instead of '
             'only new files (bronze_to_silver only; other stages ignore this).',
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.all and not args.pipeline:
        parser.error('Specify -a (all stages) or -p <stage_name>')

    config = ConfigLoader('config.json')
    engine = create_db_engine(config)

    try:
        with pipeline_lock(engine, PIPELINE_LOCK_NAME):
            init_schemas(engine)
            run_migrations(engine)
            init_sms_tables(engine)
            _maybe_init_readonly_role(config, engine)

            stages = build_run_list(STAGE_DEPS, run_all=args.all, pipeline=args.pipeline)
            invocation = '-a' if args.all else f'-p {args.pipeline}'
            run_pipeline(stages, config, engine, full_rebuild=args.full_rebuild, invocation=invocation)
    except PipelineLockError as exc:
        logger.error(str(exc))
        sys.exit(1)


if __name__ == '__main__':
    main()
