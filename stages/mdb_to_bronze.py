from __future__ import annotations

import glob as glob_module
import logging
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from modules.access_reader import read_mdb_table, select_latest_per_tablet
from modules.config import get_country_paths
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class MdbToBronze(BaseStage):
    name = 'mdb_to_bronze'
    dependencies: list[str] = []

    def run(self) -> StageResult:
        communities = self.config.get('communities')
        trial = self.config.get('trial')
        table_name = self.config.get('access_table_name')
        excluded_tablets = self.config.get('excluded_tablets', [])

        # Build country → community name lookup (one community per country)
        country_community: dict[str, str] = {
            c['country']: c['community_name'] for c in communities.values()
        }
        countries = set(country_community)

        total_rows = 0
        errors: list[str] = []

        for country in sorted(countries):
            paths = get_country_paths(country)
            extract_path = paths['extract_path']
            community_name = country_community[country]

            db_files = (
                glob_module.glob(
                    os.path.join(extract_path, '**', '*.[mM][dD][bB]'), recursive=True
                ) +
                glob_module.glob(
                    os.path.join(extract_path, '**', '*.[aA][cC][cC][dD][bB]'), recursive=True
                )
            )
            db_files = select_latest_per_tablet(db_files, excluded_tablets)
            logger.info(f"[{country}] {len(db_files)} MDB file(s) to process.")

            for db_path in db_files:
                try:
                    n = self._ingest_file(db_path, table_name, country, community_name)
                    total_rows += n
                except Exception as exc:
                    msg = (
                        f"[{country}] Failed to ingest "
                        f"'{os.path.basename(db_path)}': {exc}"
                    )
                    logger.error(msg)
                    errors.append(msg)

        return StageResult(
            success=len(errors) == 0,
            rows_written=total_rows,
            errors=errors,
        )

    def _ingest_file(
        self,
        db_path: str,
        table_name: str,
        country: str,
        community: str,
    ) -> int:
        """Load one MDB file into bronze_ibis.baseline. Returns rows written (0 if skipped)."""
        last_modified = datetime.fromtimestamp(os.path.getmtime(db_path), tz=timezone.utc)

        # Skip if already loaded. On first run the meta table does not yet exist —
        # treat that as "not loaded" so the initial ingest proceeds normally.
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT loaded FROM bronze_ibis.meta "
                        "WHERE file_path = :fp AND last_modified = :lm"
                    ),
                    {'fp': db_path, 'lm': last_modified},
                ).fetchone()
                if row and row.loaded:
                    logger.info(f"Skipping already-loaded: {os.path.basename(db_path)}")
                    return 0
        except ProgrammingError:
            # meta table doesn't exist yet (fresh deployment) — treat as not loaded
            pass

        run_id = str(uuid.uuid4())
        extracted_at = datetime.now(timezone.utc)

        df = read_mdb_table(db_path, table_name)
        df['run_uuid'] = run_id
        df['file_name'] = os.path.basename(db_path)
        df['file_path'] = db_path
        df['country'] = country
        df['community'] = community
        df['extracted_at'] = extracted_at

        # If the bronze table already exists, align the DataFrame to its schema:
        # drop columns not in the table and fill any missing columns with NaN.
        # This handles schema drift between different form versions.
        try:
            with self.engine.connect() as conn:
                existing_cols = [
                    row[0] for row in conn.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema = 'bronze_ibis' AND table_name = 'baseline'"
                        )
                    ).fetchall()
                ]
            if existing_cols:
                df = df.reindex(columns=existing_cols)
        except ProgrammingError:
            pass  # table does not exist yet — let to_sql create it

        meta = pd.DataFrame([{
            'run_uuid': run_id,
            'file_name': os.path.basename(db_path),
            'file_path': db_path,
            'country': country,
            'community': community,
            'extracted_at': extracted_at,
            'last_modified': last_modified,
            'loaded': True,
        }])
        with self.engine.begin() as conn:
            df.to_sql('baseline', conn, schema='bronze_ibis', if_exists='append', index=False)
            meta.to_sql('meta', conn, schema='bronze_ibis', if_exists='append', index=False)

        logger.info(
            f"Ingested {len(df)} rows from '{os.path.basename(db_path)}'"
            f" → bronze_ibis.baseline (run_uuid={run_id})"
        )
        return len(df)
