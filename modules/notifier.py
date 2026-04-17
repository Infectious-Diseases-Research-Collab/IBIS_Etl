from __future__ import annotations

import html as _html
import io
import logging
import smtplib
import ssl
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from cryptography.fernet import Fernet
import pandas as pd
from stages.base import StageResult

logger = logging.getLogger(__name__)


def _load_smtp_password(ini_path: str, key_path: str) -> str:
    """
    Read the Fernet-encrypted Password from ini_path using the key in key_path.
    Raises KeyError if 'Password' is absent from the ini file.
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

    if 'Password' not in cfg:
        raise KeyError("'Password' key not found in SMTP credential file.")

    return cipher.decrypt(cfg['Password'].encode()).decode()


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


def _build_validation_summary(report_df: pd.DataFrame | None) -> str:
    """Concise summary for the email body — full detail is in the CSV attachment."""
    if report_df is None:
        return 'Validation report unavailable — measures_ibis did not run.\n'

    sep = '─' * 47
    lines = ['Validation Issues (see attachment for full detail)', sep]

    for severity in ['ERROR', 'WARNING']:
        subset = report_df[report_df['severity'] == severity]
        if subset.empty:
            continue
        lines.append(f'\n  {severity}S ({len(subset)} record(s)):')
        for (country, site), group in subset.groupby(['country', 'site'], sort=True, dropna=False):
            header = f'{country} / {site}' if site else str(country)
            lines.append(f'    {header}')
            for check, cnt in group.groupby('check').size().items():
                lines.append(f'      • {check}  ({cnt})')

    lines.append(sep)
    return '\n'.join(lines)


def _send(
    email_cfg: dict,
    recipients: list[str],
    subject: str,
    plain: str,
    html: str,
    attachment_df: pd.DataFrame | None = None,
) -> None:
    """Assemble a multipart email and send it via SMTP with STARTTLS."""
    ini_path = email_cfg['keyfiles']['smtp_ini']
    key_path = email_cfg['keyfiles']['smtp_key']
    username = email_cfg['smtp_username']
    password = _load_smtp_password(ini_path, key_path)

    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = email_cfg['sender']
    msg['To'] = ', '.join(recipients)

    alt = MIMEMultipart('alternative')
    alt.attach(MIMEText(plain, 'plain'))
    alt.attach(MIMEText(html, 'html'))
    msg.attach(alt)

    if attachment_df is not None:
        csv_buffer = io.StringIO()
        # Sanitise cells to prevent formula injection when opened in Excel/LibreOffice.
        safe_df = attachment_df.copy()
        for col in safe_df.select_dtypes(include='str').columns:
            safe_df[col] = safe_df[col].map(
                lambda v: ("'" + v) if isinstance(v, str) and v and v[0] in ('=', '+', '-', '@', '\t', '\r') else v
            )
        safe_df.to_csv(csv_buffer, index=False)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_buffer.getvalue().encode('utf-8'))
        encoders.encode_base64(part)
        filename = f'ibis_validation_{date.today().strftime("%Y-%m-%d")}.csv'
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)

    with smtplib.SMTP(email_cfg['smtp_host'], email_cfg['smtp_port']) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(username, password)
        smtp.sendmail(email_cfg['sender'], recipients, msg.as_string())


def send_pipeline_report(
    results: dict[str, StageResult],
    stages: list[str],
    engine,
    config,
) -> None:
    """
    Send two targeted emails after a pipeline run:

    - pipeline_recipients: always notified (success or failure) with stage summary.
    - field_recipients: notified only when validation issues (ERRORs or WARNINGs)
      exist, with validation summary and CSV attachment.

    Silently returns if no email config is present.
    SMTP errors are caught and logged — never raised to the pipeline.
    """
    email_cfg = config.get('email')
    if not email_cfg:
        return

    # Query once — used for both field email trigger and body
    report_df = _query_validation_report(engine)

    # Merge any stage-level warnings (e.g. corrupt archives) into the report.
    stage_warnings = [w for r in results.values() for w in r.warnings]
    if stage_warnings:
        warnings_df = pd.DataFrame(stage_warnings)
        report_df = pd.concat([warnings_df, report_df], ignore_index=True) if report_df is not None else warnings_df

    # Filter validation report to configured countries (if specified)
    notify_countries = email_cfg.get('notify_countries')
    if notify_countries and report_df is not None:
        report_df = report_df[report_df['country'].str.lower().isin(
            [c.lower() for c in notify_countries]
        )]

    has_failures = any(not r.success for r in results.values())
    has_issues = (
        report_df is not None
        and not report_df.empty
        and report_df['severity'].isin(['ERROR', 'WARNING']).any()
    )

    today = date.today().strftime('%d %b %Y')
    stage_section = _build_stage_summary(results, stages)

    # --- Pipeline recipients: always send ---
    pipeline_recipients = email_cfg.get('pipeline_recipients', [])
    if pipeline_recipients:
        if has_failures:
            pipeline_subject = f'IBIS Pipeline \u2014 FAILED ({today})'
        else:
            pipeline_subject = f'IBIS Pipeline \u2014 Run complete ({today})'

        pipeline_plain = stage_section
        pipeline_html = f'<pre style="font-family:monospace;font-size:13px">{_html.escape(pipeline_plain)}</pre>'
        try:
            _send(email_cfg, pipeline_recipients, pipeline_subject, pipeline_plain, pipeline_html)
            logger.info(f'Pipeline status email sent to {pipeline_recipients}.')
        except Exception as exc:
            logger.error(f'Notifier failed (pipeline recipients) \u2014 email not sent: {exc}')

    # --- Field recipients: per-country, only when that country has issues ---
    field_recipients_cfg = email_cfg.get('field_recipients', {})
    if isinstance(field_recipients_cfg, dict) and report_df is not None and not report_df.empty:
        for country, recipients in field_recipients_cfg.items():
            if not recipients:
                continue
            country_df = report_df[report_df['country'].str.lower() == country.lower()]
            if country_df.empty or not country_df['severity'].isin(['ERROR', 'WARNING']).any():
                continue
            field_subject = f'IBIS Data Quality \u2014 {country.title()} issues found ({today})'
            validation_section = _build_validation_summary(country_df)
            field_plain = f'{stage_section}\n\n{validation_section}'
            field_html = f'<pre style="font-family:monospace;font-size:13px">{_html.escape(field_plain)}</pre>'
            try:
                _send(email_cfg, recipients, field_subject, field_plain, field_html, attachment_df=country_df)
                logger.info(f'Field quality report ({country}) sent to {recipients}.')
            except Exception as exc:
                logger.error(f'Notifier failed (field recipients {country}) \u2014 email not sent: {exc}')
