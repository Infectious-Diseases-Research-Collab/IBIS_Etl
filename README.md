# IBIS ETL

A containerised data pipeline that downloads field tablet data from an SFTP server, ingests Microsoft Access (`.mdb`) databases, cleans and validates the data, and loads it into a PostgreSQL medallion data warehouse.

---

## Architecture

The pipeline follows a **medallion architecture** across five PostgreSQL schemas, executed as eight sequential stages:

```
SFTP server → Downloads/ → Extracted/  →  bronze_ibis  →  silver_ibis  →  gold_ibis  →    →  store_ibis
              (.7z files)  (MDB files)     (ingested)      (deduplicated)  (transformed)  (prod)  (snapshots)
```

| Stage | Class | What it does |
|-------|-------|--------------|
| 1 | `FtpToExtracted` | Downloads `.7z` archives from SFTP, extracts MDB files into `Extracted/{country}/`. Skips already-extracted tablets. Downloads in parallel (4 workers), retries on network errors. |
| 2 | `MdbToBronze` | Exports MDB tables via `mdb-export`, stores all columns as TEXT. Skips files already loaded (by path + last-modified). |
| 3 | `BronzeToSilver` | Deduplicates by `uniqueid`, filters cross-country contamination by `countrycode`. |
| 4 | `TransformIbis` | Executes SQL files in `sql/transform/` to build dimension tables in `gold_ibis`. |
| 5 | `MeasuresIbis` | Runs 24 data-quality checks via `DataValidator`, writes results to `gold_ibis.ds_validation_report`. Executes SQL files in `sql/measures/`. |
| 6 | `PromoteIbis` | Atomically copies all `gold_ibis` tables to the production `ibis` schema. |
| 7 | `SendSms` | Syncs `sms.queue` from `ibis.baseline`, then sends due SMS messages to Uganda participants via BLASTA. See [SMS.md](SMS.md). |
| 8 | `StoreIbis` | Appends a dated snapshot of each `ibis` table into `store_ibis` (idempotent — skips if today's snapshot already exists). |
| — | `ReconcileSilver` | Weekly drift-detection safety net: rebuilds `silver_ibis` from scratch into a throwaway shadow table and diffs it against the live incrementally-maintained table. Never auto-corrects — only reports drift. Deliberately **not** part of `-a`; runs on its own `reconcile_cron` schedule via `python ibis.py -p reconcile_silver`. |

The orchestrator (`ibis.py`) uses a topological sort (Kahn's algorithm) to derive execution order from stage dependencies, and skips downstream stages if an upstream stage fails. Partial success (some tablets failed, others succeeded) is supported — downstream stages run as long as at least one tablet was processed.

### `store_ibis` — Partitioning and Retention

`store_ibis` tables are partitioned monthly by `snapshot_date` (native Postgres `PARTITION BY RANGE`), not a single ever-growing table. An existing unpartitioned `store_ibis.<table>` is migrated automatically and losslessly the first time `store_ibis` runs after this change — no manual step, no downtime — via the same create-new/verify-row-count/rename blue-green pattern `promote_ibis` already uses. Snapshots older than 1 year are retired automatically: each retiring partition is exported to a gzip CSV under `/app/backups/store_ibis_archive/` (same volume `scripts/backup_db.sh` already uses) *before* being dropped — see `docs/superpowers/specs/2026-07-03-store-ibis-partitioning-design.md` for the full retention design.

**Before deploying this to a production database for the first time:** run `docker compose run --rm etl python ibis.py -p store_ibis` once against a copy of production data (or at minimum the largest `store_ibis` table) to confirm the migration completes within an acceptable time, that the row-count verification behaves as expected, and that the number of partitions archived/dropped on the first run matches how much data is actually older than 1 year — the migration copies every existing snapshot row into a new table structure before the automatic swap, and any pre-existing snapshots older than a year will be archived and dropped on that same first run. This first run processes every `store_ibis` table's full history inside one transaction, so also measure **wall-clock time and WAL volume** during the dry run, not just row counts — that transaction holds a lock on each table for the duration of its swap, and knowing the real duration up front lets you size the `store_cron` window (or run the first migration manually, outside cron) with confidence instead of guessing.

---

## Incremental Processing & History Tracking

`bronze_to_silver` processes only new bronze records each run. A `promoted_to_silver_at` timestamp in `bronze_ibis.meta` tracks which bronze records have been cleaned and moved to silver — unchanged records are never reprocessed.

The `silver_ibis.<table>_history` tables retain every cleaned version of every record forever, enabling audit trails and point-in-time recovery without re-running expensive upstream transformations.

To force a complete rebuild of `silver_ibis` from scratch (e.g., after correcting data in bronze), use:
```bash
docker compose run --rm etl python ibis.py -p bronze_to_silver --full-rebuild
```

The `ReconcileSilver` stage (described in the Architecture table above) runs weekly as a safety net, comparing the incrementally-maintained `silver_ibis` against a fresh rebuild to detect drift. For the full design rationale, see [Design Specification](docs/superpowers/specs/2026-07-03-incremental-silver-gold-design.md).

### Gold Layer Lineage

`gold_ibis.baseline`, `gold_ibis.followup`, and `gold_ibis.d_enrollment` carry a `run_uuid` column — a lineage join key back to `bronze_ibis.meta` or `silver_ibis.<table>_history` for tracing which extraction run produced a given row's current values. `ibis` picks this up automatically since it's rebuilt (blue-green) from `gold_ibis` every run. `store_ibis` is different: it's append-only and never rebuilt, so it can't just inherit new columns from a fresh copy the way `ibis` does. Instead, `StoreIbis._snapshot_table` calls a `_reconcile_columns` step before every snapshot that diffs `ibis.<table>`'s columns against `store_ibis.<table>`'s and adds (via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) any that are missing — including on already-partitioned tables, since Postgres applies a parent-table column addition to all existing child partitions automatically. This keeps `store_ibis` in sync with `ibis`'s schema (`run_uuid` included) without requiring a rebuild or a manual migration whenever a new column like this one is added upstream. Other ETL-internal tracking columns (`file_name`, `file_path`, `extracted_at`, `updated_at`) are still dropped at the gold layer to keep production tables free of raw file-path/timestamp noise for the PIs and analysts who query them directly.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- `mdbtools` — provided by the Docker image; not required on the host

---

## Quick start

**1. Create the secrets directory and add credentials:**
```bash
mkdir -p secrets

# Database password (used by postgres and the ETL app)
echo 'your_db_password' > secrets/db_password.txt

# FTP and 7zip credentials (Fernet-encrypted ciphertext only)
# Copy from a secure location — these are not generated automatically
cp /path/to/IBIS_ftp.ini secrets/
cp /path/to/Sevenz.ini   secrets/
```

**2. Set the matching Fernet decryption keys as environment variables** (in a gitignored `.env` file — docker compose loads it automatically):
```bash
cat >> .env <<'EOF'
IBIS_FTP_FERNET_KEY=<key for IBIS_ftp.ini>
IBIS_SEVENZ_FERNET_KEY=<key for Sevenz.ini>
EOF
```
Keys and ciphertext live in different places on purpose — see [Secrets](#secrets) below.

**3. Add your config:**
```bash
cp config.json.example config.json
# Edit config.json: FTP hostname, community names, country codes, cron schedules
```

**4. Run the full pipeline once:**
```bash
docker compose run --rm etl python ibis.py -a
```

**5. Start the scheduled service:**
```bash
docker compose up -d
```

The cron schedules are read from `config.json` at container startup. To change them, update `config.json` and run `docker compose restart etl`.

---

## Running a single stage

```bash
docker compose run --rm etl python ibis.py -p <stage_name>
```

Valid stage names: `ftp_to_extracted`, `mdb_to_bronze`, `bronze_to_silver`, `transform_ibis`, `measures_ibis`, `promote_ibis`, `send_sms`, `store_ibis`, `reconcile_silver`.

`reconcile_silver` is standalone (not reachable via `-a`) — see the table above.

For SMS-specific operations (DLR check, weekly report) see [SMS.md](SMS.md).

---

## Viewing the data

**pgAdmin (web UI)** — included in the compose stack:

```bash
docker compose up -d pgadmin
```

Open `http://localhost:5050` and log in with:
- Email: `admin@ibis.com`
- Password: contents of `secrets/db_password.txt`

Register the server: Host `db`, Port `5432`, Database `ibis`, Username `ibis_user`.

**Desktop client (DBeaver, TablePlus, DataGrip, etc.):**

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5433` |
| Database | `ibis` |
| Username | `ibis_user` |
| Password | contents of `secrets/db_password.txt` |

**psql (quick queries):**
```bash
docker compose exec db psql -U ibis_user -d ibis
```

`ibis_user` has full DDL/write access to every schema — it's the ETL's own role, not a reporting credential. For pgAdmin, BI tools, or any read-only use, set up the `ibis_readonly` role instead (see below) rather than handing out `ibis_user`'s password.

---

## Read-only reporting access

The pipeline can provision a second Postgres role, `ibis_readonly`, with `SELECT`-only access to every schema — so dashboards and reporting tools never need the same write/DDL privileges as the ETL itself. This is opt-in; nothing changes until you configure it:

**1. Create the password secret:**
```bash
echo 'a-different-password' > secrets/db_readonly_password.txt
```

**2. Add the secret to `docker-compose.yml`** (mirroring `db_password`):
```yaml
secrets:
  db_readonly_password:
    file: ./secrets/db_readonly_password.txt

services:
  etl:
    secrets:
      - db_password
      - db_readonly_password   # add this line
```

**3. Point `config.json` at it:**
```json
"db": {
  "...": "...",
  "readonly_password_secret_file": "/run/secrets/db_readonly_password"
}
```

The next pipeline run creates (or updates the password of) `ibis_readonly` and grants it `SELECT` on all current and future tables in every schema. Connect reporting tools with username `ibis_readonly` and this password instead of `ibis_user`'s.

---

## Configuration

`config.json` (gitignored — never commit) must contain:

| Key | Description |
|-----|-------------|
| `ftp` | SFTP hostname and username |
| `communities` | Per-country community name and remote path mapping |
| `keyfiles` | Paths to Fernet ciphertext files inside the container (`secrets/`) — decryption keys come from environment variables, see [Secrets](#secrets) |
| `access_table_name` | Name of the table to export from each MDB file |
| `excluded_tablets` | List of tablet IDs to skip during ingestion |
| `db` | PostgreSQL connection details (`host`, `port`, `name`, `user`, `password_secret_file`, optional `readonly_password_secret_file` — see [Read-only reporting access](#read-only-reporting-access)) |
| `trial` | `dedup_key`, `country_code_map` (country → integer countrycode) |
| `schedule` | `pipeline_cron`, `store_cron`, `dlr_cron`, `sms_weekly_report_cron`, `incentive_report_cron`, `backup_cron`, `reconcile_cron` in standard cron format (UTC) |
| `email` | *(optional)* SMTP settings for pipeline notifications — see below |

`password_secret_file` points to the Docker secret mounted at `/run/secrets/db_password` — the password never appears in `config.json` or environment variables.

---

## Email notifications

When an `email` block is present in `config.json`, the pipeline sends two types of emails after every run:

| Recipient list | When sent | Content |
|----------------|-----------|---------|
| `pipeline_recipients` | Every run | Stage summary (✓/✗/— per stage with row counts). Subject says **Run complete** or **FAILED**. |
| `field_recipients` | Only when validation issues exist | Stage summary + validation issue summary grouped by country/site/check, with a full CSV attachment. |

`field_recipients` is a country-keyed dict — each country's team receives only their own issues:

```json
"email": {
  "smtp_host": "smtp.gmail.com",
  "smtp_port": 587,
  "sender": "ibis-etl@example.com",
  "smtp_username": "ibis-etl@example.com",
  "pipeline_recipients": ["pi@example.com"],
  "field_recipients": {
    "uganda": ["dm-uganda@example.com", "coordinator@example.com"],
    "kenya":  ["dm-kenya@example.com"]
  },
  "notify_countries": ["uganda", "kenya"],
  "keyfiles": {
    "smtp_ini": "secrets/SMTP.ini"
  }
}
```

`notify_countries` filters the validation report before triggering field emails — useful to suppress noise from countries not yet in active data collection.

SMTP credentials are Fernet-encrypted. Add `secrets/SMTP.ini` (ciphertext) and set `IBIS_SMTP_FERNET_KEY` (the decryption key) as an environment variable — see [Secrets](#secrets) below for why these live in different places.

The SMTP username is stored in `config.json` as `smtp_username` (not encrypted). For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833) rather than your account password.

---

## Secrets

Encrypted credential files live in `secrets/` (gitignored, never committed). The Fernet key that decrypts each one is deliberately **not** stored there — a key sitting next to the ciphertext it protects means anyone with read access to the directory holds both the lock and the key. Keys are read from environment variables instead (e.g. via a gitignored `.env` file that docker compose loads automatically, or your platform's own secret store):

| File in `secrets/` | Purpose | Decryption key |
|---------------------|---------|----------------|
| `db_password.txt` | PostgreSQL password — read by postgres via `POSTGRES_PASSWORD_FILE` and by the ETL app | *(not encrypted — plain password, mounted as a Docker secret)* |
| `IBIS_ftp.ini` | Fernet-encrypted FTP credentials | `IBIS_FTP_FERNET_KEY` env var |
| `Sevenz.ini` | Fernet-encrypted 7zip password | `IBIS_SEVENZ_FERNET_KEY` env var |
| `SMTP.ini` | Fernet-encrypted SMTP password (optional — only needed if `email` is configured) | `IBIS_SMTP_FERNET_KEY` env var |
| `BLASTA.ini` | Fernet-encrypted BLASTA credentials (optional — only needed if `sms` is configured) | `IBIS_BLASTA_FERNET_KEY` env var |

Each key-loading function (`get_decrypted_password`, `_load_smtp_password`, `_load_blasta_creds`) still accepts an optional key-file path as a local-development fallback — it logs a warning when used, since that path recreates the co-location problem. Production and any shared environment should always set the env var.

The `db_password.txt` file is mounted as a Docker secret (tmpfs inside the container — never written to disk).

---

## Project layout

```
.
├── ibis.py                  # Orchestrator — DAG, CLI entry point
├── sms.py                   # Standalone SMS CLI (--check-delivery, --weekly-report, --sync, --dry-run)
├── conftest.py              # Pytest path setup
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh            # Writes crontab from config.json at startup
├── requirements.txt
│
├── stages/                  # One class per pipeline stage
│   ├── base.py              # BaseStage, StageResult
│   ├── ftp_to_extracted.py  # SFTP download + 7zip extraction
│   ├── mdb_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── transform_ibis.py
│   ├── measures_ibis.py
│   ├── promote_ibis.py
│   ├── send_sms.py          # SMS queue sync + sending (Uganda week-8/11 follow-ups)
│   ├── store_ibis.py
│   └── reconcile_silver.py  # Weekly drift-detection safety net (standalone, not part of -a)
│
├── modules/                 # Shared utilities
│   ├── access_reader.py     # mdb-export wrapper, tablet snapshot selection
│   ├── config.py            # ConfigLoader, path helpers
│   ├── data_cleaner.py      # Deduplication, country-code filtering
│   ├── data_validator.py    # 24 data-quality checks
│   ├── db.py                # SQLAlchemy engine factory, schema init
│   ├── notifier.py          # Email notifications (pipeline status + field data quality + SMS report)
│   ├── sftp_client.py       # Paramiko SFTP wrapper, latest-per-tablet selection
│   ├── sms_processor.py     # SMS queue sync, template resolution, BLASTA client, DLR check
│   └── utils.py             # Fernet credential decryption
│
├── sql/
│   ├── transform/           # DDL for gold_ibis dimension tables
│   │   ├── d_enrollment.sql
│   │   └── d_participant.sql
│   └── measures/            # DDL for gold_ibis summary/QC tables
│       └── qc_checks.sql
│
├── secrets/                 # Gitignored — credentials and DB password
│
└── tests/                   # 80 unit tests (pytest)
```

---

## Development

**Run tests:**
```bash
python -m pytest tests/ -v
```

**Run the pipeline locally (requires Docker):**
```bash
docker compose run --rm etl python ibis.py -a        # all stages
docker compose run --rm etl python ibis.py -p store_ibis  # single stage
docker compose run --rm etl python ibis.py -a -v     # verbose logging
```

---

## Integration tests

Alongside the fast, fully-mocked default test suite (`pytest -q`, no external dependencies), `tests/integration/` contains real-Postgres integration tests: DB bootstrap, a full end-to-end pipeline run (seeding `bronze_ibis` directly and running the real `BronzeToSilver → TransformIbis → MeasuresIbis → PromoteIbis → StoreIbis` stages), and data-contract tests asserting `gold_ibis`/`ibis` column shape. These are excluded from the default `pytest` run and require Docker (they provision a throwaway `postgres:16` container automatically via `testcontainers-python` — no manual setup beyond having Docker running):

```bash
pip install -r requirements-dev.txt
pytest -m integration
```

`FtpToExtracted` (SFTP), `MdbToBronze` (the `mdb-export` subprocess), and `SendSms` (the BLASTA API) are deliberately not exercised here — they have real external dependencies that don't fit a hermetic test, and already have solid mocked-unit-test coverage in the default suite.

---

## Deployment notes

- The `db` service uses a named Docker volume (`pgdata`) so data persists across container restarts.
- Logs are written inside the container under `/var/log/ibis/`: `pipeline.log`, `store.log`, `dlr.log`, `sms_report.log`, `incentive_report.log`, `backup.log`, `reconcile.log`. Mount a host volume or use `docker compose logs` to access them.
- To change the cron schedule, edit `config.json` and run `docker compose restart etl`.
- The pipeline is idempotent: re-running after a partial failure will skip already-extracted tablets, already-loaded MDB files, and already-snapshotted store tables.
- Tablet archives (`.7z`) are deleted from `Downloads/` after successful extraction. The originals on the SFTP server are never modified.
