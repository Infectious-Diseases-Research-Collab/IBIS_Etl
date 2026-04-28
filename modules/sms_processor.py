from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests
from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

BLASTA_BASE_URL = "https://sms.dmarkmobile.com/v3/api"

# Maps raw values from ibis.baseline → canonical values in sms.templates
_LANGUAGE_MAP: dict[str, str] = {
    'english':    'English',
    'luganda':    'Luganda',
    'runyonkole': 'Runyankole',
    'runyankole': 'Runyankole',
}

_ARM_MAP: dict[str, str] = {
    'community benefits':                  'Community benefits',
    'education-based':                     'Education-based 1',
    'hiv risk assessment':                 'HIV Risk Assessment',
    'social norms - default':              'Social Norms',
    'social norms - sex-age-matched':      'Social Norms',
    'u=u messaging':                       'U=U Messaging',
    'reserved for you':                    '"Reserved for you" Messaging',
}


# ---------------------------------------------------------------------------
# Credential loading (follows existing Fernet .ini/.key pattern)
# ---------------------------------------------------------------------------

def _load_blasta_creds(ini_path: str, key_path: str) -> tuple[str, str]:
    """Load and decrypt BLASTA username and password from secrets files."""
    with open(key_path, encoding='utf-8') as f:
        key = f.read().strip().encode()
    cipher = Fernet(key)

    cfg: dict[str, str] = {}
    with open(ini_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('[') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            cfg[k.strip()] = v.strip()

    username = cfg.get('Username', '')
    encrypted_password = cfg.get('Password', '')
    if not username:
        raise KeyError("'Username' not found in BLASTA.ini")
    if not encrypted_password:
        raise KeyError("'Password' not found in BLASTA.ini")

    return username, cipher.decrypt(encrypted_password.encode()).decode()


# ---------------------------------------------------------------------------
# Placeholder substitution
# ---------------------------------------------------------------------------

def _substitute_placeholder(message: str, appointment_date) -> str:
    """Replace [...] in message with formatted appointment_date (DD/MM/YYYY)."""
    if appointment_date is None:
        return message
    try:
        if isinstance(appointment_date, str):
            d = datetime.strptime(appointment_date, '%d/%m/%Y').date()
        else:
            d = appointment_date  # already a date object
        return re.sub(r'\[.*?\]', d.strftime('%d/%m/%Y'), message)
    except (ValueError, TypeError):
        logger.warning("Invalid appointment date for placeholder substitution: %s", appointment_date)
        return message


# ---------------------------------------------------------------------------
# BLASTA API client
# ---------------------------------------------------------------------------

class BlastaClient:
    def __init__(self, username: str, password: str, max_retries: int = 3):
        self._username = username
        self._password = password
        self._max_retries = max_retries
        self._token: str | None = None

    def _get_token(self) -> str:
        resp = requests.post(
            f"{BLASTA_BASE_URL}/get_token/",
            json={"username": self._username, "password": self._password},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def send(self, phone_number: str, message: str) -> dict:
        """Send SMS. Returns provider response dict. Raises RequestException on permanent failure."""
        if self._token is None:
            self._token = self._get_token()

        for attempt in range(self._max_retries):
            try:
                resp = requests.post(
                    f"{BLASTA_BASE_URL}/send_sms/",
                    headers={"authToken": self._token},
                    json={"msg": message, "numbers": phone_number},
                    timeout=30,
                )
                if resp.status_code == 401:
                    logger.info("Token expired, refreshing...")
                    self._token = self._get_token()
                    continue
                resp.raise_for_status()
                body = resp.json()
                # Blasta returns msg_id nested in Detail[0]; fall back to top-level.
                detail = body.get('Detail', [])
                if detail and detail[0].get('msg_id'):
                    body['msg_id'] = detail[0]['msg_id']
                if not body.get('msg_id'):
                    logger.warning(
                        "No msg_id in send response for %s — raw body: %s",
                        phone_number, body,
                    )
                return body
            except requests.RequestException as exc:
                if attempt < self._max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        "Attempt %d/%d failed for %s, retrying in %ds: %s",
                        attempt + 1, self._max_retries, phone_number, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    raise
        raise requests.RequestException(f"All {self._max_retries} attempts failed for {phone_number}")

    def check_dlr(self, msg_id: str) -> str:
        """
        Poll delivery status for a sent message.
        Returns status string ('DELIVERED', 'PENDING', 'FAILED', 'NOT_FOUND').
        Raises requests.RequestException on network or server error.
        """
        if self._token is None:
            self._token = self._get_token()

        for attempt in range(2):  # one retry for token refresh
            resp = requests.post(
                f"{BLASTA_BASE_URL}/dlr/",
                headers={"authToken": self._token},
                json={"msg_id": str(msg_id)},
                timeout=30,
            )
            if resp.status_code == 401:
                logger.info("Token expired during DLR check, refreshing...")
                self._token = self._get_token()
                continue
            if resp.status_code == 404:
                return 'NOT_FOUND'
            resp.raise_for_status()
            return resp.json()['status']

        raise requests.RequestException(f"DLR check failed for msg_id={msg_id} after token refresh")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class SendResult:
    sent: int = 0
    failed: int = 0
    skipped: int = 0
    failures: list[dict] = field(default_factory=list)


@dataclass
class DlrResult:
    checked: int = 0
    updated: int = 0
    pending: int = 0
    errors: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SMS Processor
# ---------------------------------------------------------------------------

class SmsProcessor:
    def __init__(self, config, engine: Engine):
        self._config = config
        self._engine = engine
        sms_cfg = config.get('sms') or {}
        self._max_retries = sms_cfg.get('max_retries', 3)
        self._dry_run = sms_cfg.get('dry_run', False)
        self._countrycode = sms_cfg.get('countrycode', '1')
        self._client: BlastaClient | None = None

    def _get_client(self) -> BlastaClient:
        if self._client is None:
            sms_cfg = self._config.get('sms') or {}
            username, password = _load_blasta_creds(
                sms_cfg['blasta_ini'], sms_cfg['blasta_key']
            )
            self._client = BlastaClient(username, password, self._max_retries)
        return self._client

    # ------------------------------------------------------------------
    # Phase 1: sync queue from ibis.baseline
    # ------------------------------------------------------------------

    def sync_queue(self) -> int:
        """Upsert sms.queue from ibis.baseline (Uganda only). Returns rows inserted."""
        with self._engine.begin() as conn:
            r8 = conn.execute(text("""
                INSERT INTO sms.queue
                    (subjid, mobile_number, arm_text, language,
                     week, scheduled_date, appointment_date)
                SELECT
                    subjid,
                    mobile_number,
                    arm_text,
                    preferred_language_text,
                    8,
                    TO_DATE(sms_schedule_8weeks, 'DD/MM/YYYY HH24:MI:SS'),
                    CASE WHEN LEFT(dflt_appt_arm_schd_appt_date, 2) = '00' THEN NULL
                         ELSE TO_DATE(dflt_appt_arm_schd_appt_date, 'DD/MM/YYYY HH24:MI:SS')
                    END
                FROM ibis.baseline
                WHERE countrycode = :countrycode
                  AND sms_schedule_8weeks IS NOT NULL
                  AND LEFT(sms_schedule_8weeks, 2) != '00'
                  AND mobile_number IS NOT NULL
                  AND arm_text NOT IN ('Control (SOC)', 'Incentive')
                ON CONFLICT (subjid, week) DO NOTHING
            """), {"countrycode": self._countrycode})
            r11 = conn.execute(text("""
                INSERT INTO sms.queue
                    (subjid, mobile_number, arm_text, language,
                     week, scheduled_date, appointment_date)
                SELECT
                    subjid,
                    mobile_number,
                    arm_text,
                    preferred_language_text,
                    11,
                    TO_DATE(sms_schedule_11weeks, 'DD/MM/YYYY HH24:MI:SS'),
                    CASE WHEN LEFT(dflt_appt_arm_schd_appt_date, 2) = '00' THEN NULL
                         ELSE TO_DATE(dflt_appt_arm_schd_appt_date, 'DD/MM/YYYY HH24:MI:SS')
                    END
                FROM ibis.baseline
                WHERE countrycode = :countrycode
                  AND sms_schedule_11weeks IS NOT NULL
                  AND LEFT(sms_schedule_11weeks, 2) != '00'
                  AND mobile_number IS NOT NULL
                  AND arm_text NOT IN ('Control (SOC)', 'Incentive')
                ON CONFLICT (subjid, week) DO NOTHING
            """), {"countrycode": self._countrycode})
            conn.execute(text("""
                UPDATE sms.queue SET opted_out = TRUE
                WHERE subjid IN (SELECT subjid FROM sms.opt_outs)
                  AND opted_out = FALSE
            """))
            inserted = (r8.rowcount or 0) + (r11.rowcount or 0)
        logger.info("sync_queue: %d new row(s) inserted", inserted)
        return inserted

    # ------------------------------------------------------------------
    # Phase 2: find messages due today
    # ------------------------------------------------------------------

    def get_due_messages(self) -> list[dict]:
        """Return pending, non-opted-out queue rows scheduled for today."""
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT q.id, q.subjid, q.mobile_number, q.arm_text, q.language,
                       q.week, q.appointment_date
                FROM sms.queue q
                WHERE q.scheduled_date = CURRENT_DATE
                  AND q.status = 'pending'
                  AND q.opted_out = FALSE
                  AND NOT EXISTS (
                      SELECT 1 FROM sms.log l
                      WHERE l.subjid = q.subjid
                        AND l.week   = q.week
                        AND l.status = 'sent'
                  )
            """)).fetchall()
        return [row._asdict() for row in rows]

    # ------------------------------------------------------------------
    # Phase 3: resolve template → send → log
    # ------------------------------------------------------------------

    def _resolve_template(self, arm_text: str, language: str, week: int) -> tuple[str, bool] | None:
        """Return (message_text, has_placeholder) or None if not found."""
        canonical_language = _LANGUAGE_MAP.get(language.lower(), language)
        canonical_arm = _ARM_MAP.get(arm_text.lower(), arm_text)
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT message_text, has_placeholder
                FROM sms.templates
                WHERE arm = :arm AND language = :language AND week = :week
            """), {"arm": canonical_arm, "language": canonical_language, "week": week}).fetchone()
        if row is None:
            logger.warning(
                "No template for arm=%s language=%s week=%d — skipping",
                canonical_arm, canonical_language, week,
            )
            return None
        return row.message_text, row.has_placeholder

    def _log_attempt(self, *, queue_id: int, subjid: str, mobile_number: str,
                     week: int, message_text: str, attempt: int, status: str,
                     provider_message_id: str | None, error_message: str | None) -> None:
        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sms.log
                    (queue_id, subjid, mobile_number, week, message_text,
                     attempt, status, provider_message_id, error_message, sent_at)
                VALUES
                    (:queue_id, :subjid, :mobile_number, :week, :message_text,
                     :attempt, :status, :provider_message_id, :error_message,
                     CASE WHEN :status2 = 'sent' THEN NOW() ELSE NULL END)
            """), {
                "queue_id": queue_id, "subjid": subjid,
                "mobile_number": mobile_number, "week": week,
                "message_text": message_text, "attempt": attempt,
                "status": status, "status2": status,
                "provider_message_id": provider_message_id,
                "error_message": error_message,
            })

    def _update_queue_status(self, queue_id: int, status: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text("UPDATE sms.queue SET status = :status WHERE id = :id"),
                {"status": status, "id": queue_id},
            )

    def send_due_messages(self) -> SendResult:
        """Send all messages due today. Returns SendResult."""
        due = self.get_due_messages()
        logger.info("Found %d message(s) due today", len(due))

        result = SendResult()

        for row in due:
            template = self._resolve_template(row['arm_text'], row['language'], row['week'])
            if template is None:
                result.skipped += 1
                self._update_queue_status(row['id'], 'skipped')
                continue

            message, has_placeholder = template
            if has_placeholder:
                message = _substitute_placeholder(message, row.get('appointment_date'))

            if self._dry_run:
                logger.info(
                    "[DRY RUN] Would send to %s (week %d): %.60s",
                    row['mobile_number'], row['week'], message,
                )
                result.skipped += 1
                continue

            provider_msg_id = None
            error_msg = None
            success = False

            try:
                response = self._get_client().send(str(row['mobile_number']), message)
                provider_msg_id = response.get('msg_id')
                success = True
                result.sent += 1
                logger.info("Sent to %s (week %d) msg_id=%s", row['mobile_number'], row['week'], provider_msg_id)
            except Exception as exc:
                error_msg = str(exc)
                result.failed += 1
                result.failures.append({
                    'subjid': row['subjid'],
                    'mobile_number': str(row['mobile_number']),
                    'week': row['week'],
                    'error': error_msg,
                })
                logger.error("Failed to send to %s (week %d): %s", row['mobile_number'], row['week'], exc)

            self._log_attempt(
                queue_id=row['id'],
                subjid=row['subjid'],
                mobile_number=str(row['mobile_number']),
                week=row['week'],
                message_text=message,
                attempt=1,
                status='sent' if success else 'failed',
                provider_message_id=provider_msg_id,
                error_message=error_msg,
            )
            self._update_queue_status(row['id'], 'sent' if success else 'failed')

        return result

    # ------------------------------------------------------------------
    # Phase 4: poll DLR for unconfirmed sent messages
    # ------------------------------------------------------------------

    _TERMINAL_STATUSES = frozenset({'DELIVERED', 'FAILED', 'NOT_FOUND'})

    def fetch_delivery_statuses(self) -> DlrResult:
        """
        Poll Blasta /dlr/ for every sent log row that has a provider_message_id
        but no delivery_status yet. Terminal statuses are written back; PENDING
        rows are left as NULL to be re-checked on the next run.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, queue_id, subjid, provider_message_id
                FROM sms.log
                WHERE provider_message_id IS NOT NULL
                  AND delivery_status IS NULL
                  AND status = 'sent'
            """)).fetchall()

        result = DlrResult()

        for row in rows:
            row_dict = row._asdict()
            result.checked += 1
            try:
                status = self._get_client().check_dlr(row_dict['provider_message_id'])
            except Exception as exc:
                logger.warning(
                    "DLR check failed for log_id=%s msg_id=%s: %s",
                    row_dict['id'], row_dict['provider_message_id'], exc,
                )
                result.errors.append({
                    'log_id': row_dict['id'],
                    'subjid': row_dict['subjid'],
                    'provider_message_id': row_dict['provider_message_id'],
                    'error': str(exc),
                })
                continue

            if status in self._TERMINAL_STATUSES:
                with self._engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE sms.log
                        SET delivery_status = :status
                        WHERE id = :id
                    """), {"status": status, "id": row_dict['id']})
                result.updated += 1
                logger.info(
                    "DLR updated log_id=%s subjid=%s status=%s",
                    row_dict['id'], row_dict['subjid'], status,
                )
            else:
                result.pending += 1
                logger.debug(
                    "DLR pending for log_id=%s subjid=%s status=%s — will retry",
                    row_dict['id'], row_dict['subjid'], status,
                )

        return result

    def get_flagged_messages(self) -> list[dict]:
        """
        Return messages that failed to reach Blasta (no provider_message_id,
        queue status still 'failed'). These need manual resending by the data manager.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    l.subjid,
                    b.health_facility_ug,
                    l.week,
                    MAX(l.error_message) AS last_error
                FROM sms.log l
                JOIN sms.queue     q ON q.id      = l.queue_id
                JOIN ibis.baseline b ON b.subjid  = l.subjid
                WHERE q.status = 'failed'
                  AND l.provider_message_id IS NULL
                  AND b.countrycode = :countrycode
                GROUP BY l.subjid, b.health_facility_ug, l.week
                ORDER BY b.health_facility_ug, l.subjid
            """), {"countrycode": self._countrycode}).fetchall()
        return [row._asdict() for row in rows]

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> SendResult:
        """Full daily run: sync queue then send due messages."""
        self.sync_queue()
        return self.send_due_messages()

    # ------------------------------------------------------------------
    # Weekly report data
    # ------------------------------------------------------------------

    _WEEKLY_REPORT_SQL = """
    WITH reached AS (
        SELECT DISTINCT ON (l.queue_id)
            b.health_facility_ug,
            l.week,
            l.delivery_status
        FROM sms.log l
        JOIN sms.queue     q ON q.id      = l.queue_id
        JOIN ibis.baseline b ON b.subjid  = l.subjid
        WHERE l.status = 'sent'
          AND b.countrycode = :countrycode
          {date_filter}
        ORDER BY l.queue_id, l.sent_at DESC
    ),
    sent_counts AS (
        SELECT
            health_facility_ug,
            week,
            COUNT(*)                                                            AS submitted,
            COUNT(*) FILTER (WHERE delivery_status = 'DELIVERED')              AS delivered,
            COUNT(*) FILTER (WHERE delivery_status IN ('FAILED', 'NOT_FOUND')) AS undelivered,
            COUNT(*) FILTER (WHERE delivery_status IS NULL)                    AS pending
        FROM reached
        GROUP BY health_facility_ug, week
    ),
    due_counts AS (
        -- Participants with scheduled_date in the report window (Wed inclusive → Tue inclusive).
        -- For cumulative, no date filter is applied.
        SELECT b.health_facility_ug, q.week, COUNT(*) AS due
        FROM sms.queue q
        JOIN ibis.baseline b ON b.subjid = q.subjid
        WHERE b.countrycode = :countrycode
          {due_date_filter}
        GROUP BY b.health_facility_ug, q.week
    )
    SELECT
        s.health_facility_ug,
        s.week,
        COALESCE(d.due, 0) AS due,
        s.submitted,
        s.delivered,
        s.undelivered,
        s.pending
    -- LEFT JOIN: only show (site, week) pairs where at least one message was sent.
    -- (site, week) pairs with queued-but-never-sent messages are excluded intentionally.
    FROM sent_counts s
    LEFT JOIN due_counts d USING (health_facility_ug, week)
    ORDER BY s.health_facility_ug, s.week
"""

    def get_weekly_report_data(self, week_start, week_end) -> list[dict]:
        """
        Return SMS stats for the report window [week_start, week_end].
        week_start = last Wednesday, week_end = this Wednesday (report day).
        sent_at filter extends to end of Wednesday so today's sends are included.
        scheduled_date filter uses week_end inclusive (Wednesday) for Due count.
        """
        from datetime import timedelta
        sent_end = week_end + timedelta(days=1)  # Thursday — captures all Wednesday sends
        sql = self._WEEKLY_REPORT_SQL.format(
            date_filter="AND l.sent_at >= :week_start AND l.sent_at < :sent_end",
            due_date_filter="AND q.scheduled_date >= :week_start AND q.scheduled_date <= :week_end",
        )
        with self._engine.connect() as conn:
            rows = conn.execute(text(sql), {
                "countrycode": self._countrycode,
                "week_start": week_start,
                "week_end": week_end,
                "sent_end": sent_end,
            }).fetchall()
        return [row._asdict() for row in rows]

    def get_cumulative_report_data(self) -> list[dict]:
        """Return all-time SMS stats, same structure as get_weekly_report_data."""
        sql = self._WEEKLY_REPORT_SQL.format(date_filter="", due_date_filter="")
        with self._engine.connect() as conn:
            rows = conn.execute(text(sql), {
                "countrycode": self._countrycode,
            }).fetchall()
        return [row._asdict() for row in rows]
