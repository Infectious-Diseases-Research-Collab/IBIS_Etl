from __future__ import annotations

import logging
from cryptography.fernet import Fernet
import pandas as pd
from stages.base import StageResult

logger = logging.getLogger(__name__)


def _load_smtp_credentials(ini_path: str, key_path: str) -> tuple[str, str]:
    """
    Read Fernet-encrypted SMTP credentials from ini_path using the key in key_path.
    Returns (username, password).
    Raises KeyError if 'Username' or 'Password' is absent from the ini file.
    """
    with open(key_path, 'r') as f:
        key = f.read().strip().encode()
    cipher = Fernet(key)

    cfg: dict[str, str] = {}
    with open(ini_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            cfg[k.strip()] = v.strip()

    if 'Username' not in cfg:
        raise KeyError(f"'Username' key not found in credential file: {ini_path}")
    if 'Password' not in cfg:
        raise KeyError(f"'Password' key not found in credential file: {ini_path}")

    return (
        cipher.decrypt(cfg['Username'].encode()).decode(),
        cipher.decrypt(cfg['Password'].encode()).decode(),
    )


def _query_validation_report(engine) -> pd.DataFrame | None:
    """
    Query gold_ibis.ds_validation_report.
    Returns None if the table does not exist or any error occurs.
    """
    try:
        return pd.read_sql('SELECT * FROM gold_ibis.ds_validation_report', engine)
    except Exception as exc:
        logger.warning("Could not query ds_validation_report: %s", exc)
        return None


def _should_notify(results: dict, engine) -> bool:
    """
    Return True if the run has any failures OR any ERROR-severity validation rows.
    """
    if any(not r.success for r in results.values()):
        return True
    report = _query_validation_report(engine)
    if report is not None and not report.empty:
        if (report['severity'] == 'ERROR').any():
            return True
    return False


def _build_stage_summary(
    results: dict[str, StageResult],
    stages: list[str],
) -> str:
    sep = '─' * 47
    lines = ['Stage Results', sep]
    for name in stages:
        if name not in results:
            lines.append(f'  —  {name:<28}  skipped')
        elif results[name].success:
            rw = results[name].rows_written
            row_str = f'{rw:,} rows' if rw else ''
            lines.append(f'  ✓  {name:<28}  {row_str}')
        else:
            lines.append(f'  ✗  {name:<28}  FAILED')
    lines.append(sep)
    return '\n'.join(lines)


def _build_validation_section(report_df: pd.DataFrame | None) -> str:
    if report_df is None:
        return 'Validation report unavailable — measures_ibis did not run.'

    errors = report_df[report_df['severity'] == 'ERROR']
    warnings = report_df[report_df['severity'] == 'WARNING']
    sep = '─' * 47
    lines: list[str] = []

    # --- Errors ---
    if errors.empty:
        lines.append('No validation errors.')
    else:
        lines += ['Validation Errors', sep]
        for (country, site), group in errors.groupby(['country', 'site'], sort=True):
            header = f'{country} / {site}' if site else str(country)
            lines.append(header)
            for _, row in group.iterrows():
                subjids = str(row.get('affected_subjids') or '')
                id_list = [s.strip() for s in subjids.split(',') if s.strip()]
                if len(id_list) > 10:
                    id_str = ', '.join(id_list[:10]) + f'  … and {len(id_list) - 10} more'
                else:
                    id_str = ', '.join(id_list)
                count = row.get('record_count', '')
                detail = f"  •  {row['check']:<32}  {count} record(s)"
                if id_str:
                    detail += f'  — IDs: {id_str}'
                lines.append(detail)
        lines.append(sep)

    lines.append('')

    # --- Warnings ---
    if warnings.empty:
        lines.append('No warnings.')
    else:
        warn_counts = warnings.groupby('check').size().sort_values(ascending=False)
        lines += ['Warnings (summary)', sep]
        for check, count in warn_counts.items():
            lines.append(f'  {check:<36}  {count}')
        lines.append(sep)
        lines.append(f'Total: {len(warnings)} warning(s)')

    return '\n'.join(lines)


def _build_body(
    results: dict[str, StageResult],
    stages: list[str],
    report_df: pd.DataFrame | None,
) -> tuple[str, str]:
    stage_section = _build_stage_summary(results, stages)
    validation_section = _build_validation_section(report_df)
    plain = f'{stage_section}\n\n{validation_section}'
    html = f'<pre style="font-family:monospace;font-size:13px">{plain}</pre>'
    return plain, html
