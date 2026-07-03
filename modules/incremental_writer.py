from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)


def append_history(conn, df: pd.DataFrame, schema: str, history_table: str) -> None:
    """
    Append df to schema.history_table, creating it on first use (same
    if_exists='append' pattern already used for bronze_ibis tables — see
    stages/mdb_to_bronze.py). Never updates or deletes existing rows: this
    table is the permanent record of every cleaned version of every record
    ever seen, and no other function in this module writes to it.
    """
    df.to_sql(history_table, conn, schema=schema, if_exists='append', index=False)
    logger.info(f"Appended {len(df)} row(s) to {schema}.{history_table}.")


def ensure_current_table(
    conn, schema: str, history_table: str, current_table: str, key_col: str
) -> None:
    """
    Create schema.current_table with the same columns/types as
    schema.history_table (which must already exist — call append_history
    first) if it doesn't exist yet, then ensure it has an updated_at column
    and a UNIQUE(key_col) constraint. Both are required by upsert_latest().
    Idempotent — safe to call on every run.
    """
    conn.execute(text(
        f'CREATE TABLE IF NOT EXISTS {schema}."{current_table}" '
        f'(LIKE {schema}."{history_table}" INCLUDING ALL)'
    ))
    conn.execute(text(
        f'ALTER TABLE {schema}."{current_table}" '
        f'ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT now()'
    ))
    constraint_name = f'{current_table}_{key_col}_key'
    exists = conn.execute(
        text('SELECT 1 FROM pg_constraint WHERE conname = :name'),
        {'name': constraint_name},
    ).scalar()
    if not exists:
        conn.execute(text(
            f'ALTER TABLE {schema}."{current_table}" '
            f'ADD CONSTRAINT "{constraint_name}" UNIQUE ("{key_col}")'
        ))
        logger.info(f"Added UNIQUE({key_col}) constraint to {schema}.{current_table}.")


def upsert_latest(
    conn,
    df: pd.DataFrame,
    schema: str,
    current_table: str,
    key_col: str,
    order_col: str,
) -> None:
    """
    Upsert df into schema.current_table keyed by key_col: a row with a
    key_col value not already present is inserted; a row that conflicts is
    only overwritten if df's order_col value is greater than the existing
    row's — otherwise the existing (newer) row is left untouched.

    Stages df into a throwaway "_stage_<current_table>" table first, since
    pandas/SQLAlchemy have no first-class upsert support — matches the
    "_new_<table>" / "_old_<table>" staging-table naming convention already
    used in stages/promote_ibis.py.
    """
    if df.empty:
        return

    staging_table = f'_stage_{current_table}'
    df.to_sql(staging_table, conn, schema=schema, if_exists='replace', index=False)

    cols = list(df.columns)
    col_list = ', '.join(f'"{c}"' for c in cols)
    update_cols = [c for c in cols if c != key_col]
    update_list = ', '.join(f'"{c}" = excluded."{c}"' for c in update_cols)

    conn.execute(text(f"""
        INSERT INTO {schema}."{current_table}" ({col_list}, updated_at)
        SELECT {col_list}, now() FROM {schema}."{staging_table}"
        ON CONFLICT ("{key_col}") DO UPDATE
        SET {update_list}, updated_at = now()
        WHERE excluded."{order_col}" > {schema}."{current_table}"."{order_col}"
    """))
    conn.execute(text(f'DROP TABLE {schema}."{staging_table}"'))
    logger.info(f"Upserted {len(df)} row(s) into {schema}.{current_table}.")
