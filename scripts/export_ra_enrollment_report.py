#!/usr/bin/env python3
"""Export RA (interviewer) enrollment counts by month, for facilitation
payments and performance tracking.

Produces: Output/ra_enrollment_report_<date>.xlsx
  - Sheet "Uganda (IDRC)": RA ID x month enrollment counts for countrycode=1
  - Sheet "Kenya (KEMRI)": RA ID x month enrollment counts for countrycode=2
Each sheet includes a Facility column, one column per calendar month
covering the full study to date, a Total column, and a Grand Total row.

RA IDs (interviewer_id) are only unique within a country — the same
numeric ID is reused across Uganda and Kenya — so the two countries are
always reported on separate sheets, never combined.

Run from project root:
    python scripts/export_ra_enrollment_report.py
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.config import ConfigLoader
from modules.db import create_db_engine
from modules.reference_data import FACILITY_CODES_KE, FACILITY_CODES_UG

QUERY = """
    SELECT
        countrycode,
        interviewer_id,
        health_facility_ug,
        health_facility_ke,
        starttime
    FROM ibis.baseline
    WHERE consent::integer = 1
      AND subjid IS NOT NULL
      AND interviewer_id IS NOT NULL
"""

OUTPUT = Path('Output/ra_enrollment_report.xlsx')


def _facility_label(row, facility_codes: dict) -> str:
    code = row['health_facility_ug'] if row['countrycode'] == '1' else row['health_facility_ke']
    try:
        return facility_codes.get(int(code), f'Unknown ({code})')
    except (TypeError, ValueError):
        return 'Unknown'


def _build_country_sheet(df: pd.DataFrame, facility_codes: dict, month_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    df['facility'] = df.apply(lambda r: _facility_label(r, facility_codes), axis=1)

    pivot = (
        df.groupby(['interviewer_id', 'facility', 'month'])
        .size()
        .unstack('month', fill_value=0)
        .reindex(columns=month_cols, fill_value=0)
    )
    pivot = pivot.reset_index()
    # Facility is stable per RA in practice, but if an RA's rows carry more
    # than one facility label, groupby above would have split them into
    # separate rows — collapse back to one row per RA, keeping the first
    # facility seen and summing months.
    pivot = pivot.groupby('interviewer_id', as_index=False).agg(
        {**{'facility': 'first'}, **{c: 'sum' for c in month_cols}}
    )
    pivot['Total'] = pivot[month_cols].sum(axis=1)
    pivot = pivot.sort_values('Total', ascending=False)

    grand_total = {'interviewer_id': 'GRAND TOTAL', 'facility': ''}
    for c in month_cols:
        grand_total[c] = pivot[c].sum()
    grand_total['Total'] = pivot['Total'].sum()
    pivot = pd.concat([pivot, pd.DataFrame([grand_total])], ignore_index=True)

    pivot = pivot.rename(columns={'interviewer_id': 'RA ID', 'facility': 'Facility'})
    return pivot[['RA ID', 'Facility', *month_cols, 'Total']]


def main() -> None:
    config = ConfigLoader('config.json')
    db_cfg = config.get('db')
    if os.environ.get('DB_HOST'):
        db_cfg['host'] = os.environ['DB_HOST']
    if os.environ.get('DB_PORT'):
        db_cfg['port'] = int(os.environ['DB_PORT'])
    if os.environ.get('DB_PASSWORD_FILE'):
        db_cfg['password_secret_file'] = os.environ['DB_PASSWORD_FILE']
    engine = create_db_engine(config)

    print("Querying ibis.baseline for enrollments by RA ...")
    with engine.connect() as conn:
        df = pd.read_sql(text(QUERY), conn)

    if df.empty:
        print("No enrollments found.")
        return

    df['enrollment_date'] = pd.to_datetime(df['starttime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df['month'] = df['enrollment_date'].dt.strftime('%Y-%m')

    month_cols = sorted(df['month'].dropna().unique())

    ug_df = df[df['countrycode'] == '1']
    ke_df = df[df['countrycode'] == '2']

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
        if not ug_df.empty:
            ug_sheet = _build_country_sheet(ug_df, FACILITY_CODES_UG, month_cols)
            ug_sheet.to_excel(writer, sheet_name='Uganda (IDRC)', index=False)
            print(f"  Uganda (IDRC): {len(ug_sheet) - 1} RA(s), {ug_df.shape[0]} enrollment(s)")
        if not ke_df.empty:
            ke_sheet = _build_country_sheet(ke_df, FACILITY_CODES_KE, month_cols)
            ke_sheet.to_excel(writer, sheet_name='Kenya (KEMRI)', index=False)
            print(f"  Kenya (KEMRI): {len(ke_sheet) - 1} RA(s), {ke_df.shape[0]} enrollment(s)")

    print(f"Done → {OUTPUT.resolve()}")


if __name__ == '__main__':
    main()
