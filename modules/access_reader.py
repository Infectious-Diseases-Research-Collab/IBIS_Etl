from __future__ import annotations

import io
import logging
import os
import re
import glob
import subprocess
from collections import defaultdict
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_TABLET_FOLDER_RE = re.compile(
    r'^(Tablet\d+)_(\d{4}_\d{2}_\d{2}-\d{2}_\d{2}_\d{2})$',
    re.IGNORECASE,
)

_DATETIME_COERCE_COLS = {
    'dob', 'vdate', 'starttime', 'stoptime',
    'next_appt_3m', 'next_appt_6m', 'appt_w1_2m', 'appt_w2_8m',
    'sms_schedule_8weeks', 'sms_schedule_11weeks',
    'dflt_appt_arm_schd_appt_date',
}
_NUMERIC_COERCE_COLS: set[str] = set()  # 'age' is always empty — age data is in respondants_age

_MDB_EXPORT_TIMEOUT = 60   # seconds — table export
_MDB_TABLES_TIMEOUT = 10   # seconds — listing tables is fast


def read_mdb_table(db_path: str, table_name: str) -> pd.DataFrame:
    """
    Export *table_name* from *db_path* using mdb-export and return as DataFrame.
    Raises RuntimeError if mdb-export exits with a non-zero code or times out.
    """
    try:
        result = subprocess.run(
            ['mdb-export', '-T', '%d/%m/%Y %H:%M:%S', db_path, table_name],
            capture_output=True,
            text=True,
            timeout=_MDB_EXPORT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"mdb-export timed out after {_MDB_EXPORT_TIMEOUT}s "
            f"for '{os.path.basename(db_path)}'"
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"mdb-export failed for '{os.path.basename(db_path)}': {result.stderr.strip()}"
        )
    # Read all columns as str so the bronze layer stores raw text.
    # Type coercions (dates, numerics) happen in the silver stage.
    return pd.read_csv(io.StringIO(result.stdout), dtype=str, keep_default_na=False)


def list_mdb_tables(db_path: str) -> list[str]:
    """Return the list of user table names in an MDB file using mdb-tables."""
    try:
        result = subprocess.run(
            ['mdb-tables', '-1', db_path],
            capture_output=True,
            text=True,
            timeout=_MDB_TABLES_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"mdb-tables timed out after {_MDB_TABLES_TIMEOUT}s "
            f"for '{os.path.basename(db_path)}'"
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"mdb-tables failed for '{os.path.basename(db_path)}': {result.stderr.strip()}"
        )
    return [t for t in result.stdout.strip().split('\n') if t]


def _parse_tablet_snapshot(path: str) -> tuple[Optional[str], Optional[datetime]]:
    parts = path.replace('\\', '/').split('/')
    for part in parts:
        m = _TABLET_FOLDER_RE.match(part)
        if m:
            tablet_id = m.group(1)
            ts = datetime.strptime(m.group(2), '%Y_%m_%d-%H_%M_%S')
            return tablet_id, ts
    return None, None


def select_latest_per_tablet(
    db_files: list[str],
    excluded_tablets: list[str] | None = None,
) -> list[str]:
    """
    From a list of MDB paths return only the latest snapshot per tablet,
    excluding DataBackup subfolders and any tablets in *excluded_tablets*.
    """
    excluded = {t.lower() for t in (excluded_tablets or [])}
    already_logged: set[str] = set()

    normalised = [p.replace('\\', '/') for p in db_files]
    no_backup = [
        p for p in normalised
        if '/DataBackup/' not in p
        and not p.lower().endswith('/databackup/ibis_pilot.mdb')
    ]

    tablet_files: dict[str, tuple[datetime, str]] = {}
    root_files: list[str] = []

    for path in no_backup:
        tablet_id, ts = _parse_tablet_snapshot(path)
        if tablet_id is None:
            root_files.append(path)
        else:
            if tablet_id.lower() in excluded:
                if tablet_id not in already_logged:
                    logger.info(f"Skipping excluded tablet '{tablet_id}'.")
                    already_logged.add(tablet_id)
                continue
            existing_ts, _ = tablet_files.get(tablet_id, (datetime.min, ''))
            if ts > existing_ts:
                tablet_files[tablet_id] = (ts, path)

    selected = [path for _, path in tablet_files.values()]
    if not selected and root_files:
        logger.warning("No tablet snapshot folders found; using root-level MDB(s).")
        selected = root_files

    return sorted(selected)


def _compare_schemas(labelled: list[tuple[str, pd.DataFrame]]) -> list[dict]:
    if not labelled:
        return []

    seen: dict[str, None] = {}
    for _, df in labelled:
        for c in df.columns:
            seen.setdefault(c, None)

    col_dtypes: dict[str, set[str]] = defaultdict(set)
    for _, df in labelled:
        for col, dt in df.dtypes.items():
            col_dtypes[col].add(str(dt))

    version_groups: dict[frozenset, list[str]] = defaultdict(list)
    for label, df in labelled:
        version_groups[frozenset(df.columns)].append(label)

    ref_cols = max(version_groups.keys(), key=len)
    issues: list[dict] = []

    for cols, tablets in version_groups.items():
        missing = sorted(ref_cols - cols)
        if missing:
            for tablet in tablets:
                issues.append(dict(
                    tablet=tablet,
                    issue_type='missing_columns',
                    columns=missing,
                    detail=(
                        f"Tablet has {len(cols)} columns vs {len(ref_cols)} in "
                        f"most complete schema. Missing: {missing}"
                    ),
                ))

    meaningful_conflicts = {
        col: dts for col, dts in col_dtypes.items()
        if len(dts) > 1 and not _is_nullable_int_conflict(dts)
    }
    for col, dts in sorted(meaningful_conflicts.items()):
        issues.append(dict(
            tablet='(all)',
            issue_type='type_conflict',
            columns=[col],
            detail=f"Column '{col}' has conflicting dtypes: {dts}",
        ))

    return issues


def _is_nullable_int_conflict(dtypes: set[str]) -> bool:
    return dtypes <= {'int64', 'float64'}


def _harmonise_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in _DATETIME_COERCE_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in _NUMERIC_COERCE_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


class AccessReader:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def read_all_databases(
        self,
        folder: str,
        *,
        latest_per_tablet: bool = True,
        skip_databackup: bool = True,
        excluded_tablets: list[str] | None = None,
    ) -> tuple[pd.DataFrame, list[str], list[dict]]:
        """
        Read a table from all MDB files under *folder*.
        Returns (combined_df, error_strings, schema_issues).
        """
        db_files = (
            glob.glob(os.path.join(folder, '**', '*.[mM][dD][bB]'), recursive=True) +
            glob.glob(os.path.join(folder, '**', '*.[aA][cC][cC][dD][bB]'), recursive=True)
        )
        logger.info(f"Found {len(db_files)} database file(s) in '{folder}'.")

        if latest_per_tablet:
            db_files = select_latest_per_tablet(db_files, excluded_tablets=excluded_tablets)
            logger.info(f"Latest-per-tablet selection: {len(db_files)} database(s).")
        elif skip_databackup:
            db_files = [p for p in db_files if '/DataBackup/' not in p.replace('\\', '/')]

        labelled: list[tuple[str, pd.DataFrame]] = []
        failures: list[str] = []

        for db in db_files:
            try:
                df = read_mdb_table(db, self.table_name)
                df['_source_db'] = os.path.normpath(db)
                label = os.path.basename(os.path.dirname(db))
                labelled.append((label, df))
                logger.info(f"Read {len(df)} rows from '{label}'.")
            except Exception as exc:
                msg = (
                    f"Error reading '{os.path.basename(db)}': {exc}. "
                    f"Field team should re-upload: {os.path.normpath(db)}"
                )
                logger.error(msg)
                failures.append(msg)

        if not labelled:
            logger.warning(f"No data successfully read from '{folder}'.")
            return pd.DataFrame(), failures, []

        schema_issues = _compare_schemas(labelled)
        combined = pd.concat([df for _, df in labelled], ignore_index=True, join='outer')
        combined = _harmonise_types(combined)

        logger.info(
            f"Combined {len(combined)} rows from "
            f"{len(labelled)}/{len(db_files)} database(s) | "
            f"{len(combined.columns)} columns."
        )
        return combined, failures, schema_issues
