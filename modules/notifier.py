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

_UG_SITE_NAMES: dict[str, str] = {
    '11': 'Bushenyi HCIV',
    '12': 'Ishaka Adv. Hosp',
    '13': 'Ishongororo HCIV',
    '14': 'Ruhoko HCIV',
    '99': 'Other',
}


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
        for col in safe_df.select_dtypes(include='object').columns:
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
        sms_summary = _build_sms_summary(results)
        if sms_summary:
            pipeline_plain = f"{pipeline_plain}\n\n{sms_summary}"
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


def _build_sms_summary(results: dict[str, 'StageResult']) -> str | None:
    """Build SMS summary section for the pipeline email. Returns None if send_sms didn't run."""
    sms_result = results.get('send_sms')
    if sms_result is None:
        return None
    meta = getattr(sms_result, 'metadata', {})
    if not meta:
        return None

    sent     = meta.get('sent', 0)
    failed   = meta.get('failed', 0)
    skipped  = meta.get('skipped', 0)
    failures = meta.get('failures', [])

    sep   = '─' * 47
    lines = ['SMS Summary', sep]
    lines.append(f'  Sent:     {sent}')
    lines.append(f'  Failed:   {failed}')
    lines.append(f'  Skipped:  {skipped}')

    if failures:
        lines.append('')
        lines.append('  Failed messages:')
        for failure in failures:
            lines.append(
                f"    subjid={failure['subjid']}  mobile={failure['mobile_number']}"
                f"  week={failure['week']}  error: {failure['error']}"
            )
    lines.append(sep)
    return '\n'.join(lines)


def _build_weekly_sms_table(rows: list[dict], title: str) -> str:
    """
    Build a transposed SMS stats table: sites as columns, weeks+metrics as rows.
    rows: list of dicts with keys health_facility_ug, week, submitted, delivered,
          undelivered, pending.
    """
    if not rows:
        return f'{title}\n  No activity.\n'

    # Determine which site codes appear in data, in canonical order
    all_sites = [
        code for code in _UG_SITE_NAMES
        if any(str(r['health_facility_ug']) == code for r in rows)
    ]
    all_weeks = sorted({r['week'] for r in rows})

    # Build lookup: (site_code, week) -> row dict
    lookup: dict[tuple, dict] = {}
    for r in rows:
        lookup[(str(r['health_facility_ug']), r['week'])] = r

    col_w = 14   # width of each site data column
    label_w = 16  # width of row label column

    sep = '─' * (label_w + col_w * len(all_sites) + col_w)

    # Header row
    header = f"{'':>{label_w}}"
    for code in all_sites:
        name = _UG_SITE_NAMES.get(code, code)
        header += f'{name:>{col_w}}'
    header += f'{"Total":>{col_w}}'

    lines = [title, sep, header, sep]

    metrics = [
        ('submitted',   'Submitted'),
        ('delivered',   'Delivered'),
        ('undelivered', 'Undelivered'),
        ('pending',     'Pending'),
    ]

    for week in all_weeks:
        first = True
        for key, label in metrics:
            site_vals = [lookup.get((code, week), {}).get(key, 0) for code in all_sites]
            total = sum(site_vals)
            week_label = f'Week {week}' if first else ''
            row_label = f'{week_label:<8}  {label}'
            first = False
            data_cols = ''.join(f'{v:>{col_w}}' for v in site_vals)
            lines.append(f'{row_label:<{label_w}}{data_cols}{total:>{col_w}}')
        lines.append(sep)

    return '\n'.join(lines)


def _build_weekly_sms_report(weekly_rows: list[dict], cumulative_rows: list[dict], week_ending: str) -> str:
    """Build full weekly SMS report: this-week table + cumulative table."""
    parts = [
        f'IBIS SMS Weekly Report — week ending {week_ending}',
        '',
        _build_weekly_sms_table(weekly_rows, 'This week'),
        '',
        _build_weekly_sms_table(cumulative_rows, 'Cumulative (all time)'),
    ]
    return '\n'.join(parts)


def send_sms_weekly_report(engine, config) -> None:
    """Send weekly SMS activity report to Uganda field recipients."""
    from datetime import date, timedelta
    email_cfg = config.get('email')
    if not email_cfg:
        return

    field_recipients_cfg = email_cfg.get('field_recipients', {})
    uganda_recipients = next(
        (v for k, v in field_recipients_cfg.items() if k.lower() == 'uganda'),
        [],
    )
    if not uganda_recipients:
        logger.warning("No Uganda field recipients configured — weekly SMS report not sent.")
        return

    # Thursday-to-Thursday window: last Thursday 00:00 to this Thursday 00:00
    today = date.today()
    days_since_thursday = (today.weekday() - 3) % 7
    this_thursday = today - timedelta(days=days_since_thursday)
    week_start = this_thursday - timedelta(days=7)
    week_end = this_thursday

    from modules.sms_processor import SmsProcessor
    processor = SmsProcessor(config=config, engine=engine)
    weekly_rows = processor.get_weekly_report_data(week_start=week_start, week_end=week_end)
    cumulative_rows = processor.get_cumulative_report_data()

    if not weekly_rows and not cumulative_rows:
        logger.info("No SMS activity — weekly report not sent.")
        return

    week_ending_str = this_thursday.strftime('%d %b %Y')
    subject = f'IBIS SMS Weekly Report \u2014 week ending {week_ending_str}'
    plain = _build_weekly_sms_report(weekly_rows, cumulative_rows, week_ending_str)
    html = f'<pre style="font-family:monospace;font-size:13px">{_html.escape(plain)}</pre>'

    try:
        _send(email_cfg, uganda_recipients, subject, plain, html)
        logger.info('Weekly SMS report sent to %s.', uganda_recipients)
    except Exception as exc:
        logger.error('Weekly SMS report email failed: %s', exc)


def send_sms_flagged_alert(flagged: list[dict], config, engine) -> None:
    """
    Send daily alert to data manager listing messages that never reached Blasta.
    Only called when flagged is non-empty.
    """
    from datetime import date
    email_cfg = config.get('email')
    if not email_cfg:
        return

    dm_recipients = email_cfg.get('sms_dm_recipients', [])
    if not dm_recipients:
        logger.warning("No sms_dm_recipients configured — flagged alert not sent.")
        return

    today_str = date.today().strftime('%d %b %Y')
    subject = f'IBIS SMS \u2014 Action Required: {today_str}'

    sep = '─' * 85
    lines = [
        f'IBIS SMS — Action Required: {today_str}',
        sep,
        'The following messages failed to reach Blasta and require manual resending.',
        '',
        f'{"Participant":<20} {"Site":<35} {"Week":>4}  Last Error',
        sep,
    ]

    for msg in flagged:
        site_name = _UG_SITE_NAMES.get(
            str(msg.get('health_facility_ug', '')),
            str(msg.get('health_facility_ug', 'Unknown')),
        )
        error = (msg.get('last_error') or 'unknown')[:40]
        lines.append(
            f"{msg['subjid']:<20} {site_name:<35} {msg['week']:>4}  {error}"
        )

    lines.append(sep)

    # Build resend SQL grouped by week
    by_week: dict[int, list[str]] = {}
    for msg in flagged:
        by_week.setdefault(msg['week'], []).append(msg['subjid'])

    lines.append('To resend, run:')
    for week, subjids in sorted(by_week.items()):
        id_list = ', '.join(f"'{s}'" for s in subjids)
        lines.append(f"  UPDATE sms.queue SET status = 'pending'")
        lines.append(f"  WHERE subjid IN ({id_list}) AND week = {week};")

    lines.append(sep)

    plain = '\n'.join(lines)
    html = f'<pre style="font-family:monospace;font-size:13px">{_html.escape(plain)}</pre>'

    try:
        _send(email_cfg, dm_recipients, subject, plain, html)
        logger.info('Flagged SMS alert sent to %s (%d messages).', dm_recipients, len(flagged))
    except Exception as exc:
        logger.error('Flagged SMS alert email failed: %s', exc)
