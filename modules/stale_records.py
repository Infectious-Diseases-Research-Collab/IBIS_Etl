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
    m = _TABLET_RE.search(file_path)
    return m.group(1) if m else None


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
    table_name = _validate_table_name(table_name)

    try:
        with engine.connect() as conn:
            meta = pd.read_sql(
                text(
                    "SELECT file_path, run_uuid, last_modified FROM bronze_ibis.meta "
                    "WHERE table_name = :tn AND loaded = TRUE "
                    "ORDER BY last_modified DESC"
                ),
                conn, params={'tn': table_name},
            )
    except Exception as exc:
        logger.warning(
            f"find_stale_uniqueids({table_name}): could not read bronze_ibis.meta: {exc}"
        )
        return set()

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

    try:
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
    except Exception as exc:
        logger.warning(
            f"find_stale_uniqueids({table_name}): could not read bronze/silver data: {exc}"
        )
        return set()

    present_ids = set(recent_present['uniqueid'].dropna().astype(str))
    silver = silver[silver['tabletnum'].astype(str).isin(tablets_with_history)]
    silver_ids = set(silver['uniqueid'].dropna().astype(str))

    stale = silver_ids - present_ids
    if stale:
        logger.info(f"find_stale_uniqueids({table_name}): {len(stale)} stale uniqueid(s) found.")
    return stale
