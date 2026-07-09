# IBIS SMS Module

Automated SMS sending for Uganda study participants at week-8 and week-11 follow-up dates. Runs as a native ETL stage and as a standalone CLI.

---

## How it works

1. **Sync queue** — On each run, `ibis.baseline` is scanned for Uganda participants (`countrycode = '1'`) with valid SMS schedule dates. New participants are inserted into `sms.queue`. Already-queued participants are silently skipped.
2. **Find due messages** — Rows in `sms.queue` with `scheduled_date = CURRENT_DATE`, `status = 'pending'`, `opted_out = FALSE`, and no prior successful send in `sms.log`.
3. **Resolve template** — Look up `sms.templates` by `(arm, language, week)`. For the Default appointment arm, replace `[...]` in the message with the participant's appointment date.
4. **Send via BLASTA** — 3 attempts with exponential backoff (1s, 2s, 4s). On success, `sms.queue.status` → `sent`. On permanent failure, the row is auto-retried up to 3 times across subsequent runs (see [Retrying failed messages](#retrying-failed-messages)); after that it's left `failed` for manual review.
5. **Log** — Every attempt (success or failure) is written to `sms.log` with the provider message ID, timestamp, and error if any.

---

## Setup (run once)

### 1. Encrypt BLASTA credentials

```bash
python scripts/encrypt_blasta_creds.py
```

Creates `secrets/BLASTA.ini` (encrypted password, gitignored) and prints the Fernet key to your terminal. Set it as an environment variable rather than saving it to disk next to the ciphertext it decrypts:

```bash
export IBIS_BLASTA_FERNET_KEY='<the printed key>'
```

For docker compose, put it in a `.env` file (gitignored, auto-loaded by compose) instead of exporting it in your shell. `--write-key-file` recreates the old `secrets/BLASTA.key` fallback for local development only.

### 2. Add SMS config to `config.json`

```json
"sms": {
    "messages_dir": "data/sms_messages",
    "blasta_ini":   "secrets/BLASTA.ini",
    "max_retries":  3,
    "dry_run":      false,
    "countrycode":  "1"
}
```

`blasta_key` (a path to a key file) is no longer required — the Fernet key comes from `IBIS_BLASTA_FERNET_KEY`. It's still accepted as a local-dev fallback if you'd rather not set the env var.

### 3. Seed message templates

```bash
docker compose run --rm etl python3 scripts/seed_sms_templates.py
```

Reads `data/sms_messages/English.xlsx`, `Luganda.xlsx`, and `Runyankole.xlsx` and upserts into `sms.templates`. Re-run whenever message content changes.

### 4. SMS tables

Created automatically when the pipeline starts (via `modules/db.init_sms_tables`). No manual step needed.

---

## Running

### As part of the full pipeline

```bash
docker compose run --rm etl python ibis.py -a
```

`send_sms` runs after `promote_ibis`, ensuring `ibis.baseline` is fully current.

### Standalone

```bash
docker compose run --rm etl python sms.py                  # sync queue + send today's messages
docker compose run --rm etl python sms.py --dry-run        # log what would be sent, nothing sent
docker compose run --rm etl python sms.py --sync           # sync queue only, no sending
docker compose run --rm etl python sms.py --check-delivery # poll BLASTA for DLR status updates
docker compose run --rm etl python sms.py --weekly-report  # send weekly facility report email
docker compose run --rm etl python sms.py --resend --week <n> --subjid <id...> --actor <name> # audited manual resend, see below
```

---

## Delivery receipts (DLR)

After messages are sent, BLASTA asynchronously updates delivery status. The `--check-delivery` job polls BLASTA for each message that is still `pending` in `sms.log` and writes the result (`DELIVERED`, `FAILED`, or `NOT_FOUND`) back to `sms.log.delivery_status`.

Run this job one hour after the pipeline sends messages so most receipts have had time to arrive.

Exits non-zero only when every row it checked errored (a few transient lookup failures alongside otherwise-successful checks are not treated as a job failure) — useful if you wire cron exit codes into monitoring.

---

## Checking results

### Messages sent today

```sql
SELECT l.subjid, q.mobile_number, q.arm_text, q.language, q.week,
       q.scheduled_date, l.status, l.delivery_status, l.provider_message_id, l.sent_at
FROM sms.log l
JOIN sms.queue q ON q.id = l.queue_id
WHERE l.status = 'sent'
ORDER BY l.sent_at DESC;
```

### Failed messages

```sql
SELECT q.subjid, q.mobile_number, q.arm_text, q.week,
       l.error_message, l.created_at
FROM sms.log l
JOIN sms.queue q ON q.id = l.queue_id
WHERE l.status = 'failed'
ORDER BY l.created_at DESC;
```

### Queue status summary

```sql
SELECT status, COUNT(*) FROM sms.queue GROUP BY status ORDER BY status;
```

### Delivery status summary (current week)

```sql
SELECT delivery_status, COUNT(*)
FROM sms.log
WHERE status = 'sent'
  AND sent_at >= date_trunc('week', NOW())
GROUP BY delivery_status;
```

---

## Retrying failed messages

Failed messages retry automatically: each failed send is reset to `pending` so the next scheduled run picks it up again, up to 3 attempts total (tracked by counting `sms.log` failures for that participant/week). Once a message has been auto-retried 3 times without success, it's left `failed` and surfaces in the daily `--check-delivery` flagged-message alert for manual review — that cap resets after a manual `--resend`, so a message you've fixed and resent gets a fresh 3-attempt budget rather than being immediately re-flagged if it fails again.

For a message that needs manual attention (e.g., a corrected phone number), use `sms.py --resend` rather than a raw `UPDATE sms.queue` statement — it's audited (who requested it, when, and why is written to `sms.resend_log`), whereas a manual SQL update leaves no record:

```bash
docker compose run --rm etl python sms.py --resend \
  --week 8 --subjid IBIS1234-567 IBIS1234-568 \
  --actor "your name" --note "confirmed number by phone"
```

This resets the matching `sms.queue` rows to `pending`. Then run `python sms.py` or the full pipeline to actually resend them.

The daily `--check-delivery` job emails this exact command (with `--week`/`--subjid` pre-filled) to `sms_dm_recipients` whenever a message fails to reach BLASTA — just fill in `--actor`.

---

## Opt-outs

Managed manually by a data manager. Add a participant to `sms.opt_outs` to stop all future messages:

```sql
INSERT INTO sms.opt_outs (subjid, mobile_number, reason)
VALUES ('IBIS1234-567', '07XXXXXXXX', 'Participant requested removal');
```

The queue is updated on the next processor run.

---

## Message templates

Templates are stored in `data/sms_messages/` as Excel files — one file per language. Each file has three columns: `Arm`, `Wk 8 SMS`, `Wk 11 SMS`.

| Language | File |
|---|---|
| English | `English.xlsx` |
| Luganda | `Luganda.xlsx` |
| Runyankole | `Runyankole.xlsx` |

**Study arms with templates (9):**
- Community benefits
- Default appointment setting
- Education-based 1
- Empowerment/goal-setting
- Fresh Start Effect
- HIV Risk Assessment
- "Reserved for you" Messaging
- Social Norms
- U=U Messaging

Arms with no template (Control SOC, Incentive) are silently skipped.

**Placeholder messages:** Messages containing `[...]` have `has_placeholder = TRUE` in `sms.templates`. The processor substitutes the participant's appointment date (formatted `DD/MM/YYYY`) at send time.

---

## Database tables

| Table | Purpose |
|---|---|
| `sms.templates` | Message content seeded from Excel files |
| `sms.queue` | One row per participant × week. Idempotent — safe to re-sync. |
| `sms.log` | Append-only audit log of every send attempt, including delivery status |
| `sms.opt_outs` | Participants who should never receive messages |
| `sms.message_status` | View: one row per participant/week with consolidated delivery status |

---

## Scheduling (cron)

All times are **EAT (Uganda time, UTC+3)**. The container runs UTC; cron expressions are stored in UTC.

| Job | EAT time | UTC cron | Command |
|---|---|---|---|
| Full pipeline (includes SMS sending) | Daily 9:00 AM | `0 6 * * *` | `python ibis.py -a` |
| DLR check | Daily 10:00 AM | `0 7 * * *` | `python sms.py --check-delivery` |
| Weekly facility report | Wednesday 10:30 AM | `30 7 * * 3` | `python sms.py --weekly-report` |
| Store snapshot | Sunday 6:00 AM | `0 3 * * 0` | `python ibis.py -p store_ibis` |

Schedules are read from `config.json` at container startup. To change them, edit `config.json` and run `docker compose restart etl`.

---

## Weekly facility report

Sent every Wednesday morning to data managers (`sms_dm_recipients` in `config.json`). Covers the week ending the previous Tuesday (Wednesday–Tuesday).

The report contains two sheets:

**Weekly** — metrics for the current week only (Wed–Tue):

| Metric | Description | % denominator |
|---|---|---|
| Due | Participants with `scheduled_date` in the week window | — |
| Sent | Messages that reached BLASTA | % of Due |
| Delivered | DLR-confirmed delivery | % of Sent |
| Failed | DLR-confirmed failure | % of Sent |
| Pending | No DLR received yet | % of Sent |

**Cumulative** — same metrics for all messages sent to date, with `Due` showing all participants scheduled up to today.

Rows are grouped by health facility and study week (Wk 8 / Wk 11). Sites with no sends yet (e.g., earliest scheduled date in the future) do not appear until their first message is sent.

---

## Files

```
modules/sms_processor.py          Core SMS logic (queue sync, template resolution, BLASTA client, DLR check)
modules/notifier.py               Email notifications — includes weekly SMS report builder
stages/send_sms.py                ETL stage wrapper
sms.py                            Standalone CLI entry point
scripts/encrypt_blasta_creds.py   One-time credential encryption
scripts/seed_sms_templates.py     Load Excel templates into sms.templates
sql/sms/init_sms_schema.sql       CREATE TABLE statements
data/sms_messages/                Excel message files
secrets/BLASTA.ini                Encrypted BLASTA credentials (gitignored)
IBIS_BLASTA_FERNET_KEY            Fernet key (env var — not a file; see Setup above)
```
