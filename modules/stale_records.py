from __future__ import annotations

import logging
import re

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

_TABLET_RE = re.compile(r'Tablet(\d+)', re.IGNORECASE)


def _validate_table_name(name: str) -> str:
    """Reject names that could break SQL identifier interpolation below."""
    if not re.match(r'^[a-z_][a-z0-9_]*$', name):
        raise ValueError(f"Invalid table name: '{name}'")
    return name


def _tablet_number(file_path: str) -> str | None:
    """
    Extract the numeric tablet id from a bronze_ibis.meta.file_path value,
    e.g. 'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb' -> '53'.
    Mirrors the Tablet\\d+ pattern already used in modules/access_reader.py.
    """
    if file_path is None or (isinstance(file_path, float) and pd.isna(file_path)):
        return None
    m = _TABLET_RE.search(str(file_path))
    return m.group(1) if m else None


def _normalize_tablet(value) -> str:
    """
    Strip the spurious '.0' pandas adds when tabletnum round-trips through a
    float64 column — same issue modules/data_validator.py's
    _strip_float_suffix works around for the same column.
    """
    s = str(value).strip()
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    return s


def find_stale_uniqueids(engine, table_name: str, min_absent_syncs: int = 2) -> set[str]:
    """
    Return uniqueids present in silver_ibis.<table_name> but absent from
    their tablet's `min_absent_syncs` most recent successful bronze_ibis
    syncs — candidates for having been deleted on the tablet after ingestion.

    bronze_ibis is append-only and silver_ibis.<table_name> is upserted
    keyed by uniqueid (see modules/incremental_writer.py::upsert_latest),
    so a record deleted on the tablet after being ingested never gets
    removed on its own. See docs/superpowers/specs/
    2026-07-27-stale-record-validator-check-design.md for the full design.

    Never raises: any failure degrades to "nothing flagged this run"
    (returns an empty set) rather than failing the calling pipeline stage.
    """
    try:
        table_name = _validate_table_name(table_name)

        with engine.connect() as conn:
            meta = pd.read_sql(
                text(
                    "SELECT file_path, run_uuid, last_modified FROM bronze_ibis.meta "
                    "WHERE table_name = :tn AND loaded = TRUE "
                    "ORDER BY last_modified DESC"
                ),
                conn, params={'tn': table_name},
            )

        if meta.empty:
            return set()

        meta['tablet'] = meta['file_path'].map(_tablet_number)
        meta = meta.dropna(subset=['tablet'])

        recent_run_uuids: list[str] = []
        tablets_with_history: set[str] = set()
        for tablet, group in meta.groupby('tablet'):
            top = group.sort_values('last_modified', ascending=False).head(min_absent_syncs)
            if len(top) < min_absent_syncs:
                continue  # not enough sync history for this tablet yet
            tablets_with_history.add(tablet)
            recent_run_uuids.extend(top['run_uuid'].tolist())

        if not tablets_with_history:
            return set()

        with engine.connect() as conn:
            recent_present = pd.read_sql(
                text(
                    f"SELECT DISTINCT uniqueid FROM bronze_ibis.{table_name} "
                    "WHERE run_uuid = ANY(:uuids)"
                ),
                conn, params={'uuids': recent_run_uuids},
            )
            silver = pd.read_sql(
                text(f"SELECT uniqueid, tabletnum FROM silver_ibis.{table_name}"),
                conn,
            )

        present_ids = set(recent_present['uniqueid'].dropna().astype(str))
        silver = silver[silver['tabletnum'].map(_normalize_tablet).isin(tablets_with_history)]
        silver_ids = set(silver['uniqueid'].dropna().astype(str))

        stale = silver_ids - present_ids
        if stale:
            logger.info(
                f"find_stale_uniqueids({table_name}): {len(stale)} stale uniqueid(s) found."
            )
        return stale
    except Exception as exc:
        logger.warning(f"find_stale_uniqueids({table_name}): failed, returning empty set: {exc}")
        return set()


def remove_stale_records(
    engine,
    table_name: str,
    min_absent_syncs: int = 3,
    reason: str = 'stale_record_missing_from_tablet',
) -> list[dict]:
    """
    Delete records from silver_ibis.<table_name> confirmed absent from their
    tablet's last `min_absent_syncs` successful syncs, archiving a tombstone
    row (table_name, uniqueid, subjid, tabletnum, reason, removed_at) into
    ops.removed_records for each one first.

    Uses a stricter min_absent_syncs than find_stale_uniqueids's WARNING-level
    default (2) since deletion is harder to reverse than flagging. The full
    original row is not duplicated here — it already lives permanently in
    bronze_ibis and silver_ibis.<table_name>_history (both append-only), so
    this only needs to record that a removal happened and why.

    Never raises: any failure degrades to "nothing removed this run" (returns
    an empty list) rather than failing the calling pipeline stage.
    """
    try:
        table_name = _validate_table_name(table_name)
    except ValueError as exc:
        logger.warning(f"remove_stale_records({table_name!r}): {exc}")
        return []

    stale_ids = find_stale_uniqueids(engine, table_name, min_absent_syncs=min_absent_syncs)
    if not stale_ids:
        return []

    try:
        with engine.begin() as conn:
            rows = pd.read_sql(
                text(
                    f'SELECT uniqueid, subjid, tabletnum FROM silver_ibis.{table_name} '
                    'WHERE uniqueid = ANY(:ids)'
                ),
                conn, params={'ids': list(stale_ids)},
            )
            if rows.empty:
                return []

            archive = rows.copy()
            archive['table_name'] = table_name
            archive['reason'] = reason
            archive.to_sql('removed_records', conn, schema='ops', if_exists='append', index=False)

            conn.execute(
                text(f'DELETE FROM silver_ibis.{table_name} WHERE uniqueid = ANY(:ids)'),
                {'ids': rows['uniqueid'].tolist()},
            )

        removed = archive[['table_name', 'uniqueid', 'subjid', 'tabletnum', 'reason']].to_dict('records')
        logger.warning(
            f"remove_stale_records({table_name}): removed {len(removed)} record(s): "
            f"{[r['subjid'] for r in removed]}"
        )
        return removed
    except Exception as exc:
        logger.warning(f"remove_stale_records({table_name}): failed, removed nothing: {exc}")
        return []
