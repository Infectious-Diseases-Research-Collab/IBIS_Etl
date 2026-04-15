# Email Notification Module — Design Spec

**Date:** 2026-04-15
**Status:** Approved

---

## Goal

Send an email after a pipeline run completes reporting stage statuses and summarising major data-quality issues, so the data management team can act without logging into the server.

---

## Trigger Conditions

The email is sent only when at least one of the following is true:

1. Any stage returned `StageResult.success == False`
2. `gold_ibis.ds_validation_report` exists and contains at least one row with `severity = 'ERROR'`

If neither condition is met, the notifier returns silently. This means a fully clean run produces no email.

---

## Configuration

`config.json` gains a top-level `email` block. The pipeline operates normally without it (local dev / CI with no SMTP config).

```json
"email": {
  "smtp_host": "smtp.example.com",
  "smtp_port": 587,
  "sender": "ibis-etl@example.com",
  "recipients": ["datamanager@example.com", "pi@example.com"],
  "keyfiles": {
    "smtp_ini": "secrets/SMTP.ini",
    "smtp_key": "secrets/SMTP.key"
  }
}
```

| Field | Description |
|-------|-------------|
| `smtp_host` | SMTP server hostname |
| `smtp_port` | SMTP port (587 for STARTTLS, 465 for SSL) |
| `sender` | From address |
| `recipients` | List of To addresses (one or more) |
| `keyfiles.smtp_ini` | Path to Fernet-encrypted SMTP credentials file |
| `keyfiles.smtp_key` | Path to Fernet key file |

---

## Credentials

`secrets/SMTP.ini` holds the SMTP `username` and `password`, encrypted with the Fernet key stored in `secrets/SMTP.key`. This is identical to the pattern used for FTP credentials (`IBIS_ftp.ini` / `IBIS_ftp.key`).

The existing `load_credentials(ini_path, key_path)` function in `modules/utils.py` is reused with no changes.

Both files are covered by the existing `./secrets:/app/secrets:ro` Docker volume mount.

---

## Email Content

**Subject:** `IBIS Pipeline — Issues found (15 Apr 2026)`

**Format:** `multipart/alternative` — plain text + HTML. Plain text is the canonical version; HTML renders the same content with light formatting.

### Section 1 — Stage Summary

One line per stage showing status and rows written (or skipped):

```
Stage Results
─────────────────────────────────────────
✓ ftp_to_extracted    —
✓ mdb_to_bronze       — 5,416 rows
✓ bronze_to_silver    — 5,416 rows
✓ transform_ibis      —
✓ measures_ibis       — 47 issues logged
✗ promote_ibis        — FAILED
— store_ibis          — skipped
─────────────────────────────────────────
```

Status symbols: `✓` success, `✗` failed, `—` skipped (not run due to upstream failure).

Rows written is omitted for stages where `rows_written` is 0 or None, except `measures_ibis` where it represents validation issues logged.

### Section 2 — Validation ERRORs

Full list of rows from `gold_ibis.ds_validation_report` where `severity = 'ERROR'`, grouped by country then site:

```
Validation Errors
─────────────────────────────────────────
Kenya / 21 (Kisumu HCIV)
  • duplicate_uniqueid        2 records — IDs: P001, P002
  • missing_consent           1 record  — IDs: P003

Uganda / 11 (Bushenyi HCIV)
  • countrycode_mismatch      3 records — IDs: U010, U011, U012
─────────────────────────────────────────
```

Affected subject IDs (`affected_subjids`) are shown inline, truncated to 10 with "… and N more" if the list is longer.

If `ds_validation_report` does not exist (e.g. `measures_ibis` was skipped), this section is omitted with a note: *"Validation report unavailable — measures_ibis did not run."*

### Section 3 — Warning Summary

Count per check name only (not individual rows):

```
Warnings (summary)
─────────────────────────────────────────
  duplicate_name             3
  missing_next_appt         12
  sparse_column              1
─────────────────────────────────────────
Total: 16 warning(s)
```

---

## Module Design

### `modules/notifier.py`

Single public function:

```python
def send_pipeline_report(
    results: dict[str, StageResult],
    engine,
    config: dict,
) -> None:
```

Internal helpers (private):

- `_should_notify(results, engine) -> bool` — evaluates trigger conditions
- `_query_validation_report(engine) -> pd.DataFrame | None` — queries `gold_ibis.ds_validation_report`; returns `None` if table doesn't exist
- `_build_body(results, report_df) -> tuple[str, str]` — returns `(plain_text, html)`
- `_send(config, subject, plain, html) -> None` — loads credentials, opens SMTP connection, sends

### Error handling

`send_pipeline_report` catches all exceptions internally, logs them at ERROR level, and returns without raising. A broken SMTP configuration must never cause the pipeline to report failure.

---

## Integration with `ibis.py`

One import and one call at the end of the run function:

```python
from modules.notifier import send_pipeline_report

# ... existing stage execution loop ...

send_pipeline_report(results=stage_results, engine=engine, config=config)
```

Guarded by `config.get('email')` inside `send_pipeline_report`, so pipelines without an `email` block in `config.json` are unaffected.

---

## Files Changed

| File | Change |
|------|--------|
| `modules/notifier.py` | **Create** — new module |
| `ibis.py` | **Modify** — add import + one call at end of run |
| `config.json.example` | **Modify** — add `email` block example |
| `tests/test_notifier.py` | **Create** — unit tests |

`modules/utils.py`, `docker-compose.yml`, and `requirements.txt` (only `smtplib` from stdlib) require no changes.

---

## Out of Scope

- Per-country or per-site recipient routing
- Email scheduling independent of pipeline runs
- Retry logic on SMTP failure
- HTML-only email (plain text always included)
