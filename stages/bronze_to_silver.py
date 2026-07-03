from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

from modules.incremental_writer import append_history, ensure_current_table, upsert_latest
from modules.silver_rebuild import clean_full_history
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

_TABLES = ('baseline', 'followup')


class BronzeToSilver(BaseStage):
    name = 'bronze_to_silver'
    dependencies: list[str] = ['mdb_to_bronze']

    def run(self, full_rebuild: bool = False) -> StageResult:
        trial = self.config.get('trial')
        dedup_key = trial['dedup_key']
        country_code_map: dict[str, int] = trial.get('country_code_map', {})

        errors: list[str] = []
        total_written = 0

        # baseline and followup are written on one connection/transaction so a
        # crash between them can't leave silver_ibis with a fresh baseline and
        # a stale followup (or vice versa) — they move together or not at all.
        with self.engine.begin() as conn:
            for table_name in _TABLES:
                if full_rebuild:
                    n, errs = self._full_rebuild_table(conn, table_name, dedup_key, country_code_map)
                else:
                    n, errs = self._process_table(conn, table_name, dedup_key, country_code_map)
                total_written += n
                errors.extend(errs)

        return StageResult(success=len(errors) == 0, rows_written=total_written, errors=errors)

    def _process_table(
        self, conn, table_name: str, dedup_key: str, country_code_map: dict[str, int]
    ) -> tuple[int, list[str]]:
        """Clean only newly-ingested bronze_ibis.<table_name> rows (per
        bronze_ibis.meta.promoted_to_silver_at) and fold them into
        silver_ibis.<table_name>_history (append) and silver_ibis.<table_name>
        (upsert, keep-latest-by-extracted_at)."""
        new_meta = conn.execute(text("""
            SELECT run_uuid FROM bronze_ibis.meta
            WHERE table_name = :tn AND loaded = TRUE AND promoted_to_silver_at IS NULL
        """), {'tn': table_name}).fetchall()
        run_uuids = [r[0] for r in new_meta]

        if not run_uuids:
            logger.info(f"No new bronze_ibis.{table_name} files to promote to silver.")
            return 0, []

        bronze_df = pd.read_sql(
            text(f'SELECT * FROM bronze_ibis.{table_name} WHERE run_uuid = ANY(:uuids)'),
            conn, params={'uuids': run_uuids},
        )
        logger.info(f"Read {len(bronze_df)} new row(s) from bronze_ibis.{table_name}.")

        cleaned, errors = clean_full_history(bronze_df, dedup_key, country_code_map)

        if not cleaned.empty:
            history_table = f'{table_name}_history'
            append_history(conn, cleaned, 'silver_ibis', history_table)
            ensure_current_table(conn, 'silver_ibis', history_table, table_name, 'uniqueid')
            upsert_latest(conn, cleaned, 'silver_ibis', table_name, 'uniqueid', 'extracted_at')

        conn.execute(text("""
            UPDATE bronze_ibis.meta SET promoted_to_silver_at = now()
            WHERE run_uuid = ANY(:uuids) AND table_name = :tn
        """), {'uuids': run_uuids, 'tn': table_name})

        logger.info(f"Promoted {len(cleaned)} row(s) → silver_ibis.{table_name}.")
        return len(cleaned), errors

    def _full_rebuild_table(
        self, conn, table_name: str, dedup_key: str, country_code_map: dict[str, int]
    ) -> tuple[int, list[str]]:
        """Recovery path: re-clean ALL of bronze_ibis.<table_name> (ignoring
        promoted_to_silver_at) and replace the CURRENT table's contents.
        Never touches _history — history is append-only under every code
        path, this only rebuilds the derived/materialized current table."""
        bronze_df = pd.read_sql(f'SELECT * FROM bronze_ibis.{table_name}', conn)
        if bronze_df.empty:
            return 0, []

        cleaned, errors = clean_full_history(bronze_df, dedup_key, country_code_map)
        if cleaned.empty:
            return 0, errors

        history_table = f'{table_name}_history'
        # ensure_current_table needs history_table to already exist (it does
        # `LIKE history_table`) — on a from-scratch recovery (no incremental
        # run has ever happened yet) it might not. A zero-row append is a
        # no-op if it already exists, and creates it with the right columns
        # if not, matching append_history's own creation semantics.
        cleaned.iloc[0:0].to_sql(history_table, conn, schema='silver_ibis', if_exists='append', index=False)
        ensure_current_table(conn, 'silver_ibis', history_table, table_name, 'uniqueid')
        conn.execute(text(f'TRUNCATE silver_ibis."{table_name}"'))
        cleaned_with_ts = cleaned.copy()
        cleaned_with_ts['updated_at'] = pd.Timestamp.now()
        cleaned_with_ts.to_sql(table_name, conn, schema='silver_ibis', if_exists='append', index=False)

        logger.warning(
            f"--full-rebuild: replaced silver_ibis.{table_name} with {len(cleaned)} "
            f"freshly-cleaned row(s) from full bronze history."
        )
        return len(cleaned), errors
