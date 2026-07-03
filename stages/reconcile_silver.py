from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

from modules.silver_rebuild import clean_full_history
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

_TABLES = ('baseline', 'followup')


class ReconcileSilver(BaseStage):
    """
    Weekly safety net for incremental bronze-to-silver processing (see
    docs/superpowers/specs/2026-07-03-incremental-silver-gold-design.md §5).
    Rebuilds each silver table from scratch into a throwaway shadow table
    and diffs it against the live incrementally-maintained table. Never
    auto-corrects — only logs and reports drift for a human to investigate.
    Deliberately not part of ibis.py -a; runs on its own cron schedule.
    """
    name = 'reconcile_silver'
    dependencies: list[str] = []

    def run(self) -> StageResult:
        trial = self.config.get('trial')
        dedup_key = trial['dedup_key']
        country_code_map: dict[str, int] = trial.get('country_code_map', {})

        drifted: list[str] = []
        errors: list[str] = []

        with self.engine.begin() as conn:
            for table_name in _TABLES:
                try:
                    if self._check_table(conn, table_name, dedup_key, country_code_map):
                        drifted.append(table_name)
                except Exception as exc:
                    msg = f"Reconciliation failed for '{table_name}': {exc}"
                    logger.error(msg)
                    errors.append(msg)

        if drifted:
            logger.warning(f"Reconciliation drift detected in: {drifted}")

        return StageResult(
            success=len(drifted) == 0 and len(errors) == 0,
            errors=errors,
            metadata={'drifted_tables': drifted},
        )

    def _check_table(self, conn, table_name: str, dedup_key: str, country_code_map: dict) -> bool:
        """Returns True if drift was found for this table."""
        bronze_df = pd.read_sql(f'SELECT * FROM bronze_ibis.{table_name}', conn)
        rebuilt, clean_errors, failed_countries = clean_full_history(bronze_df, dedup_key, country_code_map)
        if failed_countries:
            # A country that fails during the shadow rebuild will already
            # surface as a row-count/checksum mismatch below, but log it
            # explicitly too so the cause is obvious without cross-referencing.
            logger.warning(
                f"[{table_name}] Reconciliation rebuild had failures for: "
                f"{sorted(failed_countries)} — {clean_errors}"
            )

        shadow_table = f'_reconcile_{table_name}'
        conn.execute(text(f'DROP TABLE IF EXISTS silver_ibis."{shadow_table}"'))
        rebuilt.to_sql(shadow_table, conn, schema='silver_ibis', if_exists='replace', index=False)

        live_count = conn.execute(
            text(f'SELECT COUNT(*) FROM silver_ibis."{table_name}"')
        ).scalar()
        shadow_count = conn.execute(
            text(f'SELECT COUNT(*) FROM silver_ibis."{shadow_table}"')
        ).scalar()

        live_checksum = conn.execute(text(f"""
            SELECT md5(string_agg(uniqueid || extracted_at::text, ',' ORDER BY uniqueid))
            FROM silver_ibis."{table_name}"
        """)).scalar()
        shadow_checksum = conn.execute(text(f"""
            SELECT md5(string_agg(uniqueid || extracted_at::text, ',' ORDER BY uniqueid))
            FROM silver_ibis."{shadow_table}"
        """)).scalar()

        conn.execute(text(f'DROP TABLE silver_ibis."{shadow_table}"'))

        return live_count != shadow_count or live_checksum != shadow_checksum
