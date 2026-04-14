# FtpToExtracted Stage Design

**Goal:** Add a new first stage that downloads the latest tablet archive per tablet from the FTP server, extracts it, and deletes the compressed file — so the full pipeline runs end-to-end without manual intervention.

**Architecture:** One new stage (`FtpToExtracted`) inserted at the front of the DAG with no upstream dependencies. Two supporting modules are added: `modules/utils.py` (restored `get_decrypted_password`) and `modules/sftp_client.py` (SFTP wrapper). No existing stages are modified.

**Tech Stack:** paramiko (SFTP), py7zr (7z extraction), Fernet (credential decryption via cryptography)

---

## Stage placement

```
FtpToExtracted → mdb_to_bronze → bronze_to_silver → transform_ibis → measures_ibis → promote_ibis → store_ibis
```

`FtpToExtracted` has `dependencies = []`. It is the only stage with no upstream dependency.

---

## Modules

### `modules/utils.py`

Restored with a single function:

```python
def get_decrypted_password(cred_filename: str, key_file: str) -> str
```

Reads a Fernet key from `key_file`, decrypts the `Password` value from `cred_filename` (INI-style key=value format). Raises `KeyError` if `Password` is missing. Used by both the SFTP client and the 7z extractor.

### `modules/sftp_client.py`

A minimal wrapper around paramiko. Exposes:

```python
class SFTPClient:
    def __init__(self, hostname: str, username: str, password: str): ...
    def __enter__(self) -> SFTPClient: ...   # connects
    def __exit__(self, *args): ...           # closes
    def list_files(self, remote_path: str) -> list[str]: ...   # filenames only
    def download_file(self, remote_path: str, local_path: str) -> None: ...

def select_latest_remote_per_tablet(filenames: list[str]) -> dict[str, str]:
    # Returns {tablet_id: filename} for the latest .7z per tablet.
    # Uses the same Tablet\d+_YYYY_MM_DD-HH_MM_SS pattern as access_reader.py.
    # Ignores filenames that don't match the pattern.
```

Context manager ensures the SFTP connection is always closed.

---

## Stage: `FtpToExtracted`

**File:** `stages/ftp_to_extracted.py`
**Class:** `FtpToExtracted`
**Dependencies:** `[]`

### Run logic

```
for each community in config['communities']:
    country = community['country']
    remote_path = community['remotefilepath_download']

    decrypt FTP password
    decrypt 7z password

    with SFTPClient(hostname, username, ftp_password) as sftp:
        filenames = sftp.list_files(remote_path)
        latest = select_latest_remote_per_tablet(filenames)

        for tablet_id, filename in latest.items():
            folder_name = filename[:-3]   # strip .7z
            extract_dir = Extracted/{country}/{folder_name}/

            if extract_dir exists → skip (idempotent)

            local_archive = Downloads/{country}/{filename}
            sftp.download_file(remote_path + filename, local_archive)
            extract archive to Extracted/{country}/ using 7z password
            delete local_archive
```

### Idempotency

A tablet is skipped if `Extracted/{country}/{TabletXXX_timestamp}/` already exists. The folder name is derived from the archive filename by stripping `.7z`. This mirrors `mdb_to_bronze`'s skip logic using file path + last-modified.

### Error handling

- Per-tablet errors are caught, logged, and non-fatal. Other tablets in the same country continue.
- SFTP connection failure for a country is caught, logged, and non-fatal. Other countries continue.
- `StageResult(success=False)` is returned only if every tablet across all countries failed or errored.
- Errors are collected into `StageResult.errors` and surfaced in the pipeline summary.

### Return value

```python
StageResult(
    success=len(errors) == 0,
    rows_written=total_downloaded,   # count of archives downloaded (not rows)
    errors=errors,
)
```

`rows_written` counts archives downloaded, consistent with how other stages report units of work.

---

## Config dependencies

The stage reads from `config.json`:

| Key | Used for |
|-----|----------|
| `communities[*].country` | Destination subfolder under `Extracted/` and `Downloads/` |
| `communities[*].remotefilepath_download` | Remote FTP path to list |
| `ftp.hostname` | SFTP server address |
| `ftp.username_ibis` | SFTP username |
| `keyfiles.ftp_cred_filename_IBIS` | Path to encrypted FTP password file |
| `keyfiles.ftp_key_file_IBIS` | Path to Fernet key for FTP password |
| `keyfiles.sevenz_cred_filename` | Path to encrypted 7z password file |
| `keyfiles.sevenz_key_file` | Path to Fernet key for 7z password |

---

## File layout changes

```
modules/
    utils.py          ← restored (get_decrypted_password only)
    sftp_client.py    ← new

stages/
    ftp_to_extracted.py  ← new

tests/
    test_sftp_client.py      ← new
    test_ftp_to_extracted.py ← new
```

`ibis.py` is modified to import and register `FtpToExtracted` in `STAGE_CLASSES`.

---

## Testing strategy

### `test_sftp_client.py`
- `test_list_files_returns_filenames` — mock paramiko SFTP, verify `list_files` returns name list
- `test_download_file_writes_to_path` — mock paramiko, verify file is written locally
- `test_connection_error_propagates` — mock paramiko to raise, verify exception surfaces
- `test_select_latest_remote_per_tablet_picks_newest` — pure function, no mocks
- `test_select_latest_remote_per_tablet_ignores_non_matching` — filenames without tablet pattern are ignored

### `test_ftp_to_extracted.py`
- `test_skips_already_extracted_tablet` — if extract dir exists, no download or extraction
- `test_downloads_and_extracts_new_tablet` — mock SFTP + py7zr, verify extract called and archive deleted
- `test_sftp_error_for_one_country_continues_others` — SFTP raises for country1, country2 still runs
- `test_per_tablet_error_continues_other_tablets` — extraction fails for one tablet, others succeed
- `test_all_failed_returns_success_false` — all tablets fail, stage returns `success=False`
