# IBIS SMS Module

Automated SMS sending for Uganda study participants at week-8 and week-11 follow-up dates. Runs as a native ETL stage and as a standalone CLI.

---

## How it works

1. **Sync queue** — On each run, `ibis.baseline` is scanned for Uganda participants (`countrycode = '1'`) with valid SMS schedule dates. New participants are inserted into `sms.queue`. Already-queued participants are silently skipped.
2. **Find due messages** — Rows in `sms.queue` with `scheduled_date = CURRENT_DATE`, `status = 'pending'`, `opted_out = FALSE`, and no prior successful send in `sms.log`.
3. **Resolve template** — Look up `sms.templates` by `(arm, language, week)`. For the Default appointment arm, replace `[...]` in the message with the participant's appointment date.
4. **Send via BLASTA** — 3 attempts with exponential backoff (1s, 2s, 4s). On success, `sms.queue.status` → `sent`. On permanent failure → `failed`.
5. **Log** — Every attempt (success or failure) is written to `sms.log` with the provider message ID, timestamp, and error if any.

---

## Setup (run once)

### 1. Encrypt BLASTA credentials

```bash
python scripts/encrypt_blasta_creds.py
```

Creates `secrets/BLASTA.ini` and `secrets/BLASTA.key`. Keep both files secure — they are gitignored.

### 2. Add SMS config to `config.json`

```json
"sms": {
    "messages_dir": "data/sms_messages",
    "blasta_ini":   "secrets/BLASTA.ini",
    "blasta_key":   "secrets/BLASTA.key",
    "max_retries":  3,
    "dry_run":      false
}
```

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
docker compose run --rm etl python sms.py              # sync queue + send today's messages
docker compose run --rm etl python sms.py --dry-run    # log what would be sent, nothing sent
docker compose run --rm etl python sms.py --sync       # sync queue only, no sending
docker compose run --rm etl python sms.py --weekly-report  # send weekly facility report email
```

---

## Checking results

### Messages sent today

```sql
SELECT l.subjid, q.mobile_number, q.arm_text, q.language, q.week,
       q.scheduled_date, l.status, l.provider_message_id, l.sent_at
FROM sms.log l
JOIN sms.queue q ON q.subjid = l.subjid AND q.week = l.week
WHERE l.status = 'sent'
ORDER BY l.sent_at DESC;
```

### Failed messages

```sql
SELECT q.subjid, q.mobile_number, q.arm_text, q.week,
       l.error_message, l.created_at
FROM sms.log l
JOIN sms.queue q ON q.subjid = l.subjid AND q.week = l.week
WHERE l.status = 'failed'
ORDER BY l.created_at DESC;
```

### Queue status summary

```sql
SELECT status, COUNT(*) FROM sms.queue GROUP BY status ORDER BY status;
```

---

## Retrying failed messages

Failed messages are not automatically retried. To retry manually:

```sql
UPDATE sms.queue SET status = 'pending'
WHERE status = 'failed'
AND subjid IN ('IBIS1234-567', 'IBIS1234-568');
```

Then run `python sms.py` or the full pipeline.

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
| `sms.log` | Append-only audit log of every send attempt |
| `sms.opt_outs` | Participants who should never receive messages |

---

## Scheduling (cron)

| Job | Schedule | Command |
|---|---|---|
| Full pipeline (includes SMS) | Daily at 2 AM | `python ibis.py -a` |
| Standalone SMS | Daily at 9 AM | `python sms.py` |
| Weekly facility report | Monday at 7 AM | `python sms.py --weekly-report` |

The 9 AM standalone job is idempotent — if the 2 AM pipeline already sent messages, it finds nothing pending and exits cleanly.

---

## Files

```
modules/sms_processor.py          Core SMS logic (queue sync, template resolution, BLASTA client)
stages/send_sms.py                ETL stage wrapper
sms.py                            Standalone CLI entry point
scripts/encrypt_blasta_creds.py   One-time credential encryption
scripts/seed_sms_templates.py     Load Excel templates into sms.templates
sql/sms/init_sms_schema.sql       CREATE TABLE statements
data/sms_messages/                Excel message files
secrets/BLASTA.ini                Encrypted BLASTA credentials (gitignored)
secrets/BLASTA.key                Fernet key (gitignored)
```
