# IBIS ETL

A containerised data pipeline that ingests Microsoft Access (`.mdb`) databases from field tablets, cleans and validates the data, and loads it into a PostgreSQL medallion data warehouse.

---

## Architecture

The pipeline follows a **medallion architecture** across five PostgreSQL schemas, executed as six sequential stages:

```
MDB files  →  bronze_ibis  →  silver_ibis  →  gold_ibis  →  ibis  →  store_ibis
(raw text)    (ingested)      (deduplicated)  (transformed)  (prod)  (snapshots)
```

| Stage | Class | What it does |
|-------|-------|--------------|
| 1 | `MdbToBronze` | Exports MDB tables via `mdb-export`, stores all columns as TEXT. Skips files already loaded (content-hash by path + last-modified). |
| 2 | `BronzeToSilver` | Deduplicates by `uniqueid`, filters cross-country contamination by `countrycode`. |
| 3 | `TransformIbis` | Executes SQL files in `sql/transform/` to build dimension tables in `gold_ibis`. |
| 4 | `MeasuresIbis` | Runs 23 data-quality checks via `DataValidator`, writes results to `gold_ibis.ds_validation_report`. Executes SQL files in `sql/measures/`. |
| 5 | `PromoteIbis` | Atomically copies all `gold_ibis` tables to the production `ibis` schema. |
| 6 | `StoreIbis` | Appends a dated snapshot of each `ibis` table into `store_ibis` (idempotent — skips if today's snapshot already exists). |

The orchestrator (`ibis.py`) uses a topological sort (Kahn's algorithm) to derive execution order from stage dependencies, and skips downstream stages if an upstream stage fails.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- `mdbtools` — provided by the Docker image; not required on the host

---

## Quick start

**1. Copy and configure secrets:**
```bash
cp .env.example .env
# Edit .env and set a real IBIS_DB_PASSWORD
```

**2. Add your config:**
```bash
# Copy config.json.example to config.json and fill in your values
# (FTP credentials, community names, country codes, cron schedules)
cp config.json.example config.json
```

**3. Run the full pipeline once:**
```bash
docker compose run --rm etl python ibis.py -a
```

**4. Start the scheduled service:**
```bash
docker compose up -d
```

The cron schedules are read from `config.json` at container startup. To change them, update `config.json` and restart the container.

---

## Running a single stage

```bash
docker compose run --rm etl python ibis.py -p <stage_name>
```

Valid stage names: `mdb_to_bronze`, `bronze_to_silver`, `transform_ibis`, `measures_ibis`, `promote_ibis`, `store_ibis`.

---

## Configuration

`config.json` (gitignored — never commit) must contain:

| Key | Description |
|-----|-------------|
| `ftp` | SFTP host, port, username, credential file paths |
| `communities` | Per-country community name mapping |
| `keyfiles` | Path to encryption key files |
| `access_table_name` | Name of the table to export from each MDB file |
| `db` | PostgreSQL connection details (`host`, `port`, `name`, `user`, `password_env`) |
| `trial` | `dedup_key`, `country_code_map` (country → integer countrycode) |
| `schedule` | `pipeline_cron` and `store_cron` in standard cron format |

`password_env` names the environment variable that holds the database password (default: `IBIS_DB_PASSWORD`), so the password itself never appears in `config.json`.

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
├── .env.example
│
├── stages/                  # One class per pipeline stage
│   ├── base.py              # BaseStage, StageResult
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
│   ├── data_validator.py    # 23 data-quality checks
│   └── db.py                # SQLAlchemy engine factory, schema init
│
├── sql/
│   ├── transform/           # DDL for gold_ibis dimension tables
│   │   ├── d_enrollment.sql
│   │   └── d_participant.sql
│   └── measures/            # DDL for gold_ibis summary/QC tables
│       └── qc_checks.sql
│
└── tests/                   # 64 unit tests (pytest)
```

---

## Data quality checks

`DataValidator` runs 23 checks on each country's silver data:

1. Missing required values on core identifier fields
2. Age bounds (10–110 or −7)
3. Cross-country field contamination
4. Duplicate `uniqueid` values
5. Duplicate `screening_id` values
6. Consented participants lacking a `subjid`
7. Missing `interviewer_id`
8. `countrycode` mismatch vs. country folder
9. Duplicate `subjid` among consented participants
10. Duplicate phone numbers (normalised)
11. Phone numbers differing by one digit (likely transposition)
12. Duplicate participant names (case-insensitive)
13. Highly similar names (possible data-entry error)
14. Interview duration anomalies (impossible / too short / too long)
15. Date of birth / age consistency
16. Visit date validity
17. Appointment date logic
18. Consent flow integrity
19. Client sex coding
20. Interviewer productivity (excessive daily interviews, unusual hours)
21. Screening ID format and country-prefix correctness
22. Tablet record counts (suspiciously few records)
23. Overall record completeness (high null-rate columns)

Results are written to `gold_ibis.ds_validation_report` and promoted to `ibis.ds_validation_report` each run.

---

## Development

**Run tests:**
```bash
python -m pytest tests/ -v
```

**Run the pipeline locally (requires PostgreSQL running):**
```bash
python ibis.py -a           # all stages
python ibis.py -p store_ibis  # single stage
python ibis.py -a -v          # verbose logging
```

---

## Deployment notes

- The `db` service uses a named Docker volume (`pgdata`) so data persists across container restarts.
- Logs are written inside the container at `/var/log/ibis_pipeline.log` and `/var/log/ibis_store.log`. Mount a host volume or use `docker compose logs` to access them.
- To change the cron schedule, edit `config.json` and run `docker compose restart etl`.
- The pipeline is idempotent: re-running after a partial failure will skip already-loaded MDB files and already-snapshotted store tables.
