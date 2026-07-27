from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from modules import reference_data
from modules.data_validator import DataValidator
from modules.stale_records import find_stale_uniqueids
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

SQL_MEASURES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql', 'measures')

# Country name → (facility column, code→name map)
_FACILITY_CONFIG: dict[str, tuple[str, dict]] = {
    'kenya':  ('health_facility_ke', reference_data.FACILITY_CODES_KE),
    'uganda': ('health_facility_ug', reference_data.FACILITY_CODES_UG),
}


def _load_sql_files(directory: str) -> list[Path]:
    return sorted(Path(directory).glob('*.sql'))


def _facility_name(code, code_map: dict) -> str:
    """Return a decoded facility label, e.g. '11 (Bushenyi HCIV)'."""
    try:
        icode = int(float(str(code)))
    except (ValueError, TypeError):
        return str(code)
    return f"{icode} ({code_map.get(icode, 'Unknown')})"


def _facility_site_groups(
    country_group: pd.DataFrame, country_str: str
) -> list[tuple[str, pd.DataFrame]]:
    """
    Split a country's DataFrame into (site_name, sub-group) pairs, one per
    health facility. Shared by the baseline validation loop in run() and the
    followup stale-record loop in _validate_followup — both need identical
    per-facility grouping, just applied to different tables.
    """
    fac_field, fac_codes = _FACILITY_CONFIG.get(country_str, (None, {}))
    if fac_field and fac_field in country_group.columns:
        fac_col = pd.to_numeric(country_group[fac_field], errors='coerce')
        tmp = country_group.copy()
        tmp['_fac'] = fac_col

        site_groups: list[tuple[str, pd.DataFrame]] = []
        for fac_code, fac_group in tmp.groupby('_fac', dropna=False):
            if pd.isna(fac_code):
                site = '(Unknown facility)'
            else:
                site = _facility_name(fac_code, fac_codes)
            site_groups.append((site, fac_group.drop(columns=['_fac'])))
        return site_groups
    # No facility breakdown available — validate whole country as one group
    return [('', country_group)]


class MeasuresIbis(BaseStage):
    name = 'measures_ibis'
    dependencies: list[str] = ['transform_ibis']

    def run(self) -> StageResult:
        trial = self.config.get('trial')
        country_code_map: dict[str, int] = trial.get('country_code_map', {})

        silver_df = pd.read_sql('SELECT * FROM silver_ibis.baseline', self.engine)

        if silver_df.empty:
            logger.warning("silver_ibis.baseline is empty — skipping measures.")
            return StageResult(success=True, rows_written=0)

        errors: list[str] = []
        all_reports: list[pd.DataFrame] = []

        stale_baseline = find_stale_uniqueids(self.engine, 'baseline')

        for country, country_group in silver_df.groupby('country'):
            country_str = str(country)
            country_code = country_code_map.get(country_str)
            if country_code is None:
                logger.warning(
                    f"[{country_str}] No country_code in config — "
                    f"countrycode mismatch check skipped."
                )

            # Run identity checks (duplicate phone/name/subjid) across the full
            # country dataset so cross-facility duplicates are not missed.
            try:
                id_validator = DataValidator()
                id_report = id_validator.validate(
                    country_group.copy(),
                    country_code=country_code,
                    country_name=country_str,
                    site_name='',
                    skip_identity=False,
                )
                # Keep only the identity-check rows to avoid duplicating other checks.
                # DataValidator.IDENTITY_CHECK_NAMES is the single source of
                # truth for which check names these are (see its _CHECKS registry).
                id_report = id_report[id_report['check'].isin(DataValidator.IDENTITY_CHECK_NAMES)]
                if not id_report.empty:
                    all_reports.append(id_report)
            except Exception as exc:
                logger.error(f"[{country_str}] Country-level identity check failed: {exc}")
                errors.append(f"[{country_str}] Country-level identity check failed: {exc}")

            site_groups = _facility_site_groups(country_group, country_str)

            for site, group in site_groups:
                label = f"{country_str}/{site}" if site else country_str
                try:
                    validator = DataValidator()
                    report = validator.validate(
                        group.copy(),
                        country_code=country_code,
                        country_name=country_str,
                        site_name=site,
                        skip_identity=True,
                        stale_uniqueids=stale_baseline,
                    )
                    all_reports.append(report)
                except Exception as exc:
                    msg = f"[{label}] Validation failed: {exc}"
                    logger.error(msg)
                    errors.append(msg)

        self._validate_followup(all_reports, errors)

        if not all_reports:
            logger.warning("All validations failed — skipping report write.")
            return StageResult(success=False, rows_written=0, errors=errors)

        full_report = pd.concat(all_reports, ignore_index=True)
        full_report.to_sql(
            'ds_validation_report', self.engine, schema='gold_ibis',
            if_exists='replace', index=False,
        )
        logger.info(
            f"Wrote {len(full_report)} validation issue(s) → gold_ibis.ds_validation_report."
        )

        # Run measures SQL files
        sql_files = _load_sql_files(SQL_MEASURES_DIR)
        if not sql_files:
            msg = f"No SQL files found in '{SQL_MEASURES_DIR}'."
            logger.error(msg)
            errors.append(msg)
            return StageResult(success=False, rows_written=len(full_report), errors=errors)
        # See transform_ibis.py for why this is try/except-wrapped rather
        # than left to propagate: the inner raise aborts the transaction on
        # the first failing file, and the outer except gets back to the
        # return below with *errors* (and rows_written) populated instead of
        # losing that detail to a bare caught-and-rewrapped exception upstream.
        try:
            with self.engine.begin() as conn:
                for sql_path in sql_files:
                    try:
                        sql = sql_path.read_text()
                        conn.execute(text(sql))
                        logger.info(f"Executed: {sql_path.name}")
                    except Exception as exc:
                        msg = f"SQL error in '{sql_path.name}': {exc}"
                        logger.error(msg)
                        errors.append(msg)
                        raise
        except Exception as exc:
            if not errors:
                errors.append(str(exc))

        return StageResult(success=len(errors) == 0, rows_written=len(full_report), errors=errors)

    def _validate_followup(self, all_reports: list[pd.DataFrame], errors: list[str]) -> None:
        """
        Run only the stale-record check against silver_ibis.followup —
        followup does not go through the full DataValidator suite (see
        docs/superpowers/specs/2026-07-27-stale-record-validator-check-design.md).
        Appends any issues found directly to *all_reports*. The whole method
        body is wrapped in one outer try/except (in addition to the more
        specific per-site guard below) so any followup-specific failure —
        the read, the groupby, or an unexpected schema — can never escape
        and prevent run() from writing the baseline report it already
        computed before calling this method.
        """
        try:
            followup_df = pd.read_sql('SELECT * FROM silver_ibis.followup', self.engine)
            if followup_df.empty:
                logger.info("silver_ibis.followup is empty — skipping followup stale-record check.")
                return

            stale_followup = find_stale_uniqueids(self.engine, 'followup')
            if not stale_followup:
                return

            for country, country_group in followup_df.groupby('country'):
                country_str = str(country)
                site_groups = _facility_site_groups(country_group, country_str)

                for site, group in site_groups:
                    label = f"{country_str}/{site}" if site else country_str
                    try:
                        validator = DataValidator()
                        report = validator.validate_stale_records(
                            group.copy(), stale_followup, country_name=country_str, site_name=site,
                        )
                        if not report.empty:
                            all_reports.append(report)
                    except Exception as exc:
                        msg = f"[{label}] Followup stale-record check failed: {exc}"
                        logger.error(msg)
                        errors.append(msg)
        except Exception as exc:
            msg = f"Followup stale-record check failed: {exc}"
            logger.error(msg)
            errors.append(msg)
