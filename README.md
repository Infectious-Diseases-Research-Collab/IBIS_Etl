# IBIS ETL

A containerised data pipeline that downloads field tablet data from an SFTP server, ingests Microsoft Access (`.mdb`) databases, cleans and validates the data, and loads it into a PostgreSQL medallion data warehouse.

---

## Architecture

The pipeline follows a **medallion architecture** across five PostgreSQL schemas, executed as seven sequential stages:

```
SFTP server → Downloads/ → Extracted/  →  bronze_ibis  →  silver_ibis  →  gold_ibis  →  ibis  →  store_ibis
              (.7z files)  (MDB files)     (ingested)      (deduplicated)  (transformed)  (prod)  (snapshots)
```

| Stage | Class | What it does |
|-------|-------|--------------|
| 1 | `FtpToExtracted` | Downloads `.7z` archives from SFTP, extracts MDB files into `Extracted/{country}/`. Skips already-extracted tablets. Downloads in parallel (4 workers), retries on network errors. |
| 2 | `MdbToBronze` | Exports MDB tables via `mdb-export`, stores all columns as TEXT. Skips files already loaded (by path + last-modified). |
| 3 | `BronzeToSilver` | Deduplicates by `uniqueid`, filters cross-country contamination by `countrycode`. |
| 4 | `TransformIbis` | Executes SQL files in `sql/transform/` to build dimension tables in `gold_ibis`. |
| 5 | `MeasuresIbis` | Runs 23 data-quality checks via `DataValidator`, writes results to `gold_ibis.ds_validation_report`. Executes SQL files in `sql/measures/`. |
| 6 | `PromoteIbis` | Atomically copies all `gold_ibis` tables to the production `ibis` schema. |
| 7 | `StoreIbis` | Appends a dated snapshot of each `ibis` table into `store_ibis` (idempotent — skips if today's snapshot already exists). |

The orchestrator (`ibis.py`) uses a topological sort (Kahn's algorithm) to derive execution order from stage dependencies, and skips downstream stages if an upstream stage fails. Partial success (some tablets failed, others succeeded) is supported — downstream stages run as long as at least one tablet was processed.

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

# FTP and 7zip credentials (Fernet-encrypted)
# Copy from a secure location — these are not generated automatically
cp /path/to/IBIS_ftp.ini secrets/
cp /path/to/IBIS_ftp.key secrets/
cp /path/to/Sevenz.ini   secrets/
cp /path/to/Sevenz.key   secrets/
```

**2. Add your config:**
```bash
cp config.json.example config.json
# Edit config.json: FTP hostname, community names, country codes, cron schedules
```

**3. Run the full pipeline once:**
```bash
docker compose run --rm etl python ibis.py -a
```

**4. Start the scheduled service:**
```bash
docker compose up -d
```

The cron schedules are read from `config.json` at container startup. To change them, update `config.json` and run `docker compose restart etl`.

---

## Running a single stage

```bash
docker compose run --rm etl python ibis.py -p <stage_name>
```

Valid stage names: `ftp_to_extracted`, `mdb_to_bronze`, `bronze_to_silver`, `transform_ibis`, `measures_ibis`, `promote_ibis`, `store_ibis`.

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

---

## Configuration

`config.json` (gitignored — never commit) must contain:

| Key | Description |
|-----|-------------|
| `ftp` | SFTP hostname and username |
| `communities` | Per-country community name and remote path mapping |
| `keyfiles` | Paths to Fernet credential files inside the container (`secrets/`) |
| `access_table_name` | Name of the table to export from each MDB file |
| `excluded_tablets` | List of tablet IDs to skip during ingestion |
| `db` | PostgreSQL connection details (`host`, `port`, `name`, `user`, `password_secret_file`) |
| `trial` | `dedup_key`, `country_code_map` (country → integer countrycode) |
| `schedule` | `pipeline_cron` and `store_cron` in standard cron format |
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
    "smtp_ini": "secrets/SMTP.ini",
    "smtp_key": "secrets/SMTP.key"
  }
}
```

`notify_countries` filters the validation report before triggering field emails — useful to suppress noise from countries not yet in active data collection.

SMTP credentials are Fernet-encrypted. Add the credential files to `secrets/`:

| File | Purpose |
|------|---------|
| `secrets/SMTP.ini` | Contains `Password=<fernet-encrypted-value>` |
| `secrets/SMTP.key` | Fernet key for the SMTP password |

The SMTP username is stored in `config.json` as `smtp_username` (not encrypted). For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833) rather than your account password.

---

## Secrets

All sensitive files live in `secrets/` (gitignored, never committed):

| File | Purpose |
|------|---------|
| `db_password.txt` | PostgreSQL password — read by postgres via `POSTGRES_PASSWORD_FILE` and by the ETL app |
| `IBIS_ftp.ini` | Fernet-encrypted FTP credentials |
| `IBIS_ftp.key` | Fernet key for FTP credentials |
| `Sevenz.ini` | Fernet-encrypted 7zip password |
| `Sevenz.key` | Fernet key for 7zip password |
| `SMTP.ini` | Fernet-encrypted SMTP password (optional — only needed if `email` is configured) |
| `SMTP.key` | Fernet key for SMTP password |

The `db_password.txt` file is mounted as a Docker secret (tmpfs inside the container — never written to disk).

---

## Project layout

```
.
├── ibis.py                  # Orchestrator — DAG, CLI entry point
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
│   └── store_ibis.py
│
├── modules/                 # Shared utilities
│   ├── access_reader.py     # mdb-export wrapper, tablet snapshot selection
│   ├── config.py            # ConfigLoader, path helpers
│   ├── data_cleaner.py      # Deduplication, country-code filtering
│   ├── data_validator.py    # 24 data-quality checks
│   ├── db.py                # SQLAlchemy engine factory, schema init
│   ├── notifier.py          # Email notifications (pipeline status + field data quality)
│   ├── sftp_client.py       # Paramiko SFTP wrapper, latest-per-tablet selection
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

## Deployment notes

- The `db` service uses a named Docker volume (`pgdata`) so data persists across container restarts.
- Logs are written inside the container at `/var/log/ibis_pipeline.log` and `/var/log/ibis_store.log`. Mount a host volume or use `docker compose logs` to access them.
- To change the cron schedule, edit `config.json` and run `docker compose restart etl`.
- The pipeline is idempotent: re-running after a partial failure will skip already-extracted tablets, already-loaded MDB files, and already-snapshotted store tables.
- Tablet archives (`.7z`) are deleted from `Downloads/` after successful extraction. The originals on the SFTP server are never modified.
