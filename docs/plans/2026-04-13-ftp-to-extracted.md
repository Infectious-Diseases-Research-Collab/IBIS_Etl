# FtpToExtracted Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `FtpToExtracted` stage that downloads the latest `.7z` archive per tablet from the FTP server, extracts it, and deletes the compressed file — making the pipeline fully automated end-to-end.

**Architecture:** One new stage inserted at the front of the DAG (`dependencies = []`). Two supporting modules: `modules/utils.py` (Fernet credential decryption) and `modules/sftp_client.py` (paramiko SFTP wrapper + tablet selection logic). `MdbToBronze.dependencies` is updated to `['ftp_to_extracted']` so extraction always precedes ingestion.

**Tech Stack:** paramiko (SFTP), py7zr (7z extraction), cryptography/Fernet (credential decryption)

---

## File structure

| File | Action | Responsibility |
|------|--------|----------------|
| `modules/utils.py` | Create | `get_decrypted_password` — decrypt Fernet-encrypted INI credentials |
| `modules/sftp_client.py` | Create | `SFTPClient` context manager + `select_latest_remote_per_tablet` |
| `stages/ftp_to_extracted.py` | Create | `FtpToExtracted` stage — orchestrates download → extract → delete |
| `stages/mdb_to_bronze.py` | Modify line 22 | Add `'ftp_to_extracted'` to `dependencies` |
| `ibis.py` | Modify lines 12-32 | Import and register `FtpToExtracted` first in `STAGE_CLASSES` |
| `tests/test_utils.py` | Create | Tests for `get_decrypted_password` |
| `tests/test_sftp_client.py` | Create | Tests for `SFTPClient` and `select_latest_remote_per_tablet` |
| `tests/test_ftp_to_extracted.py` | Create | Tests for `FtpToExtracted` stage |
| `tests/test_orchestrator.py` | Modify | Add `ftp_to_extracted` to `_STAGE_DEPS` fixture |

---

## Task 1: `modules/utils.py` — credential decryption

**Files:**
- Create: `modules/utils.py`
- Create: `tests/test_utils.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_utils.py
import pytest
from cryptography.fernet import Fernet
from modules.utils import get_decrypted_password


def test_get_decrypted_password_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted = cipher.encrypt(b'mysecret').decode()

    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text(f'# comment\nPassword={encrypted}\n')

    result = get_decrypted_password(str(cred_file), str(key_file))
    assert result == 'mysecret'


def test_get_decrypted_password_missing_key_raises(tmp_path):
    key = Fernet.generate_key()
    key_file = tmp_path / 'test.key'
    cred_file = tmp_path / 'test.ini'
    key_file.write_text(key.decode())
    cred_file.write_text('OtherKey=something\n')

    with pytest.raises(KeyError, match="Password"):
        get_decrypted_password(str(cred_file), str(key_file))
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_utils.py -v
```
Expected: FAIL with `ModuleNotFoundError: No module named 'modules.utils'`

- [ ] **Step 3: Implement `modules/utils.py`**

```python
# modules/utils.py
from __future__ import annotations

from cryptography.fernet import Fernet


def get_decrypted_password(cred_filename: str, key_file: str) -> str:
    """
    Decrypt a Fernet-encrypted password from a credential file.
    File format: key=value lines; lines starting with # and +++ are ignored.
    Raises KeyError if 'Password' key is absent.
    """
    with open(key_file, 'r') as f:
        key = f.read().strip().encode()

    cipher = Fernet(key)

    config: dict[str, str] = {}
    with open(cred_filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            config[k.strip()] = v.strip()

    if 'Password' not in config:
        raise KeyError(
            f"'Password' key not found in credential file: {cred_filename}"
        )

    return cipher.decrypt(config['Password'].encode()).decode()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_utils.py -v
```
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add modules/utils.py tests/test_utils.py
git commit -m "feat: restore get_decrypted_password utility"
```

---

## Task 2: `modules/sftp_client.py` — tablet selection (pure function)

**Files:**
- Create: `modules/sftp_client.py` (partial — function only)
- Create: `tests/test_sftp_client.py` (partial)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_sftp_client.py
import pytest
from modules.sftp_client import select_latest_remote_per_tablet


def test_select_latest_picks_newest_for_same_tablet():
    filenames = [
        'Tablet221_2026_02_01-10_00_00.7z',
        'Tablet221_2026_04_01-10_00_00.7z',
    ]
    result = select_latest_remote_per_tablet(filenames)
    assert result == {'Tablet221': 'Tablet221_2026_04_01-10_00_00.7z'}


def test_select_latest_handles_multiple_tablets():
    filenames = [
        'Tablet221_2026_04_01-10_00_00.7z',
        'Tablet222_2026_04_01-10_00_00.7z',
    ]
    result = select_latest_remote_per_tablet(filenames)
    assert result['Tablet221'] == 'Tablet221_2026_04_01-10_00_00.7z'
    assert result['Tablet222'] == 'Tablet222_2026_04_01-10_00_00.7z'


def test_select_latest_ignores_non_matching_filenames():
    filenames = [
        'README.txt',
        'Tablet221_2026_04_01-10_00_00.7z',
        'some_other_file.7z',
    ]
    result = select_latest_remote_per_tablet(filenames)
    assert list(result.keys()) == ['Tablet221']


def test_select_latest_returns_empty_for_no_matches():
    result = select_latest_remote_per_tablet(['README.txt', 'notes.pdf'])
    assert result == {}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_sftp_client.py -v
```
Expected: FAIL with `ImportError: cannot import name 'select_latest_remote_per_tablet'`

- [ ] **Step 3: Implement `select_latest_remote_per_tablet` in `modules/sftp_client.py`**

```python
# modules/sftp_client.py
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_TABLET_ARCHIVE_RE = re.compile(
    r'^(Tablet\d+)_(\d{4}_\d{2}_\d{2}-\d{2}_\d{2}_\d{2})\.7z$',
    re.IGNORECASE,
)


def select_latest_remote_per_tablet(filenames: list[str]) -> dict[str, str]:
    """
    Return {tablet_id: filename} for the latest .7z archive per tablet.
    Filenames not matching the Tablet###_YYYY_MM_DD-HH_MM_SS.7z pattern
    are silently ignored.
    """
    latest: dict[str, tuple[datetime, str]] = {}
    for name in filenames:
        m = _TABLET_ARCHIVE_RE.match(name)
        if not m:
            continue
        tablet_id = m.group(1)
        ts = datetime.strptime(m.group(2), '%Y_%m_%d-%H_%M_%S')
        existing_ts, _ = latest.get(tablet_id, (datetime.min, ''))
        if ts > existing_ts:
            latest[tablet_id] = (ts, name)
    return {tablet_id: name for tablet_id, (_, name) in latest.items()}
```

(Leave the rest of the file to be added in Task 3 — the class comes next.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_sftp_client.py -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add modules/sftp_client.py tests/test_sftp_client.py
git commit -m "feat: add select_latest_remote_per_tablet"
```

---

## Task 3: `modules/sftp_client.py` — SFTPClient class

**Files:**
- Modify: `modules/sftp_client.py` (add class)
- Modify: `tests/test_sftp_client.py` (add class tests)

- [ ] **Step 1: Add failing tests to `tests/test_sftp_client.py`**

Append these tests to the existing file:

```python
import paramiko
from unittest.mock import MagicMock, patch
from modules.sftp_client import SFTPClient


def _make_mock_sftp():
    """Return a connected mock SFTPClient context manager."""
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    return mock_sftp, mock_transport


def test_sftp_client_list_files_returns_filenames():
    mock_sftp, mock_transport = _make_mock_sftp()
    attr1, attr2 = MagicMock(), MagicMock()
    attr1.filename = 'Tablet221_2026_04_01-10_00_00.7z'
    attr2.filename = 'README.txt'
    mock_sftp.listdir_attr.return_value = [attr1, attr2]

    with patch('modules.sftp_client.paramiko.Transport', return_value=mock_transport):
        with patch('modules.sftp_client.paramiko.SFTPClient.from_transport',
                   return_value=mock_sftp):
            with SFTPClient('host', 'user', 'pass') as client:
                result = client.list_files('/Kenya/Sindo/')

    assert result == ['Tablet221_2026_04_01-10_00_00.7z', 'README.txt']
    mock_sftp.listdir_attr.assert_called_once_with('/Kenya/Sindo/')


def test_sftp_client_download_file_calls_get():
    mock_sftp, mock_transport = _make_mock_sftp()

    with patch('modules.sftp_client.paramiko.Transport', return_value=mock_transport):
        with patch('modules.sftp_client.paramiko.SFTPClient.from_transport',
                   return_value=mock_sftp):
            with SFTPClient('host', 'user', 'pass') as client:
                client.download_file(
                    '/Kenya/Sindo/Tablet221_2026_04_01-10_00_00.7z',
                    '/local/Downloads/Kenya/Tablet221_2026_04_01-10_00_00.7z',
                )

    mock_sftp.get.assert_called_once_with(
        '/Kenya/Sindo/Tablet221_2026_04_01-10_00_00.7z',
        '/local/Downloads/Kenya/Tablet221_2026_04_01-10_00_00.7z',
    )


def test_sftp_client_closes_on_exit():
    mock_sftp, mock_transport = _make_mock_sftp()

    with patch('modules.sftp_client.paramiko.Transport', return_value=mock_transport):
        with patch('modules.sftp_client.paramiko.SFTPClient.from_transport',
                   return_value=mock_sftp):
            with SFTPClient('host', 'user', 'pass'):
                pass

    mock_sftp.close.assert_called_once()
    mock_transport.close.assert_called_once()


def test_sftp_client_connection_error_propagates():
    with patch('modules.sftp_client.paramiko.Transport',
               side_effect=Exception('connection refused')):
        with pytest.raises(Exception, match='connection refused'):
            with SFTPClient('host', 'user', 'pass'):
                pass
```

- [ ] **Step 2: Run tests to verify the new ones fail**

```bash
python -m pytest tests/test_sftp_client.py -v
```
Expected: 4 pass (existing), 4 fail with `AttributeError: __enter__`

- [ ] **Step 3: Add `SFTPClient` class to `modules/sftp_client.py`**

Append after `select_latest_remote_per_tablet`:

```python
import paramiko


class SFTPClient:
    """
    Minimal paramiko SFTP wrapper. Use as a context manager — the connection
    is opened on entry and closed on exit regardless of exceptions.
    """

    def __init__(self, hostname: str, username: str, password: str) -> None:
        self._hostname = hostname
        self._username = username
        self._password = password
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def __enter__(self) -> 'SFTPClient':
        self._transport = paramiko.Transport((self._hostname, 22))
        self._transport.connect(username=self._username, password=self._password)
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        logger.info(f"Connected to SFTP: {self._hostname}")
        return self

    def __exit__(self, *args) -> None:
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        logger.info(f"Disconnected from SFTP: {self._hostname}")

    def list_files(self, remote_path: str) -> list[str]:
        """Return filenames (not full paths) in remote_path."""
        return [attr.filename for attr in self._sftp.listdir_attr(remote_path)]

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download remote_path to local_path."""
        self._sftp.get(remote_path, local_path)
        logger.info(f"Downloaded: {remote_path} → {local_path}")
```

Also add `import paramiko` and `import pytest` to the test file imports.

- [ ] **Step 4: Run all sftp_client tests**

```bash
python -m pytest tests/test_sftp_client.py -v
```
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add modules/sftp_client.py tests/test_sftp_client.py
git commit -m "feat: add SFTPClient wrapper"
```

---

## Task 4: `stages/ftp_to_extracted.py`

**Files:**
- Create: `stages/ftp_to_extracted.py`
- Create: `tests/test_ftp_to_extracted.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_ftp_to_extracted.py
import os
import pytest
from unittest.mock import MagicMock, patch, call

from stages.ftp_to_extracted import FtpToExtracted


def _make_config(communities=None):
    if communities is None:
        communities = {
            'kenya_nakuru': {
                'community_name': 'Sindo',
                'country': 'kenya',
                'remotefilepath_download': '/Kenya/Sindo/',
            }
        }
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {
        'ftp': {'hostname': 'ftp.example.com', 'username_ibis': 'user'},
        'communities': communities,
        'keyfiles': {
            'ftp_cred_filename_IBIS': 'keyFiles/IBIS_ftp.ini',
            'ftp_key_file_IBIS': 'keyFiles/IBIS_ftp.key',
            'sevenz_cred_filename': 'keyFiles/Sevenz.ini',
            'sevenz_key_file': 'keyFiles/Sevenz.key',
        },
    }.get(key, default)
    return config


def _mock_sftp(filenames):
    """Return a mock SFTPClient context manager that lists the given filenames."""
    instance = MagicMock()
    instance.__enter__ = MagicMock(return_value=instance)
    instance.__exit__ = MagicMock(return_value=False)
    instance.list_files.return_value = filenames
    return instance


def test_skips_already_extracted_tablet(tmp_path):
    extract_dir = tmp_path / 'Extracted' / 'Kenya' / 'Tablet221_2026_04_01-10_00_00'
    extract_dir.mkdir(parents=True)

    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient',
                   return_value=_mock_sftp(['Tablet221_2026_04_01-10_00_00.7z'])):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                result = stage.run()

    assert result.success
    assert result.rows_written == 0


def test_downloads_extracts_and_deletes_archive(tmp_path):
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    mock_archive = MagicMock()
    mock_archive.__enter__ = MagicMock(return_value=mock_archive)
    mock_archive.__exit__ = MagicMock(return_value=False)

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient',
                   return_value=_mock_sftp(['Tablet221_2026_04_01-10_00_00.7z'])):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                with patch('stages.ftp_to_extracted.py7zr.SevenZipFile',
                           return_value=mock_archive):
                    with patch('os.remove') as mock_remove:
                        result = stage.run()

    assert result.success
    assert result.rows_written == 1
    mock_archive.extractall.assert_called_once()
    mock_remove.assert_called_once()


def test_sftp_connection_failure_is_non_fatal():
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient',
                   side_effect=Exception('connection refused')):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': '/fake/Downloads/Kenya',
                'extract_path': '/fake/Extracted/Kenya',
            }):
                with patch('os.makedirs'):
                    result = stage.run()

    assert not result.success
    assert any('connection refused' in e for e in result.errors)


def test_sftp_failure_for_one_country_continues_others(tmp_path):
    communities = {
        'kenya_nakuru': {
            'country': 'kenya',
            'remotefilepath_download': '/Kenya/Sindo/',
        },
        'uganda_mbarara': {
            'country': 'uganda',
            'remotefilepath_download': '/Uganda/Mbarara/',
        },
    }
    stage = FtpToExtracted(config=_make_config(communities), engine=MagicMock())

    call_count = {'n': 0}

    def sftp_side_effect(*args, **kwargs):
        call_count['n'] += 1
        if call_count['n'] == 1:
            raise Exception('kenya SFTP failed')
        return _mock_sftp([])

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient', side_effect=sftp_side_effect):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads'),
                'extract_path': str(tmp_path / 'Extracted'),
            }):
                with patch('os.makedirs'):
                    result = stage.run()

    # One country failed, but the other ran — errors collected, not fatal to whole stage
    assert len(result.errors) == 1
    assert 'kenya SFTP failed' in result.errors[0]


def test_per_tablet_extraction_failure_continues_other_tablets(tmp_path):
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    sftp_mock = _mock_sftp([
        'Tablet221_2026_04_01-10_00_00.7z',
        'Tablet222_2026_04_01-10_00_00.7z',
    ])

    extract_calls = {'n': 0}

    def fake_sevenz(*args, **kwargs):
        extract_calls['n'] += 1
        if extract_calls['n'] == 1:
            raise Exception('corrupt archive')
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=m)
        m.__exit__ = MagicMock(return_value=False)
        return m

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient', return_value=sftp_mock):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                with patch('stages.ftp_to_extracted.py7zr.SevenZipFile',
                           side_effect=fake_sevenz):
                    with patch('os.remove'):
                        result = stage.run()

    assert not result.success
    assert result.rows_written == 1   # second tablet succeeded
    assert len(result.errors) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_ftp_to_extracted.py -v
```
Expected: FAIL with `ModuleNotFoundError: No module named 'stages.ftp_to_extracted'`

- [ ] **Step 3: Implement `stages/ftp_to_extracted.py`**

```python
# stages/ftp_to_extracted.py
from __future__ import annotations

import logging
import os

import py7zr

from modules.config import get_country_paths
from modules.sftp_client import SFTPClient, select_latest_remote_per_tablet
from modules.utils import get_decrypted_password
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class FtpToExtracted(BaseStage):
    name = 'ftp_to_extracted'
    dependencies: list[str] = []

    def run(self) -> StageResult:
        ftp = self.config.get('ftp')
        communities = self.config.get('communities')
        keyfiles = self.config.get('keyfiles')

        hostname = ftp['hostname']
        username = ftp['username_ibis']
        ftp_password = get_decrypted_password(
            keyfiles['ftp_cred_filename_IBIS'],
            keyfiles['ftp_key_file_IBIS'],
        )
        sevenz_password = get_decrypted_password(
            keyfiles['sevenz_cred_filename'],
            keyfiles['sevenz_key_file'],
        )

        total_downloaded = 0
        errors: list[str] = []

        for community_key, community in communities.items():
            country = community['country']
            remote_path = community['remotefilepath_download']
            paths = get_country_paths(country)
            download_dir = paths['download_path']
            extract_dir = paths['extract_path']

            os.makedirs(download_dir, exist_ok=True)
            os.makedirs(extract_dir, exist_ok=True)

            try:
                with SFTPClient(hostname, username, ftp_password) as sftp:
                    filenames = sftp.list_files(remote_path)
                    latest = select_latest_remote_per_tablet(filenames)
                    logger.info(
                        f"[{country}] {len(latest)} latest archive(s) found on FTP."
                    )

                    for tablet_id, filename in sorted(latest.items()):
                        folder_name = filename[:-3]  # strip .7z
                        tablet_extract_dir = os.path.join(extract_dir, folder_name)

                        if os.path.exists(tablet_extract_dir):
                            logger.info(
                                f"[{country}] Skipping {filename} — already extracted."
                            )
                            continue

                        local_archive = os.path.join(download_dir, filename)
                        try:
                            sftp.download_file(remote_path + filename, local_archive)
                            with py7zr.SevenZipFile(
                                local_archive, mode='r', password=sevenz_password
                            ) as archive:
                                archive.extractall(path=extract_dir)
                            os.remove(local_archive)
                            logger.info(
                                f"[{country}] Extracted {filename} → {tablet_extract_dir}"
                            )
                            total_downloaded += 1
                        except Exception as exc:
                            msg = f"[{country}] Failed to process '{filename}': {exc}"
                            logger.error(msg)
                            errors.append(msg)
                            if os.path.exists(local_archive):
                                os.remove(local_archive)

            except Exception as exc:
                msg = f"[{country}] SFTP connection failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        # Partial success: if at least one tablet was downloaded, the stage is
        # considered successful even if others failed. success=False only when
        # every tablet across every country failed (nothing was processed).
        return StageResult(
            success=total_downloaded > 0 or len(errors) == 0,
            rows_written=total_downloaded,
            errors=errors,
        )
```

- [ ] **Step 4: Add the all-failed test to `tests/test_ftp_to_extracted.py`**

Append this test to the file:

```python
def test_all_tablets_failed_returns_success_false(tmp_path):
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    sftp_mock = _mock_sftp(['Tablet221_2026_04_01-10_00_00.7z'])

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient', return_value=sftp_mock):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                with patch('stages.ftp_to_extracted.py7zr.SevenZipFile',
                           side_effect=Exception('corrupt')):
                    with patch('os.remove'):
                        result = stage.run()

    assert not result.success
    assert result.rows_written == 0
    assert len(result.errors) == 1
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_ftp_to_extracted.py -v
```
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add stages/ftp_to_extracted.py tests/test_ftp_to_extracted.py
git commit -m "feat: add FtpToExtracted stage"
```

---

## Task 5: Wire into orchestrator

**Files:**
- Modify: `ibis.py`
- Modify: `stages/mdb_to_bronze.py:22`
- Modify: `tests/test_orchestrator.py`

- [ ] **Step 1: Update `ibis.py` — add import and register stage first**

Replace lines 12–32 of `ibis.py`:

```python
from stages.ftp_to_extracted import FtpToExtracted
from stages.mdb_to_bronze import MdbToBronze
from stages.bronze_to_silver import BronzeToSilver
from stages.transform_ibis import TransformIbis
from stages.measures_ibis import MeasuresIbis
from stages.promote_ibis import PromoteIbis
from stages.store_ibis import StoreIbis

# ...

STAGE_CLASSES = {
    'ftp_to_extracted': FtpToExtracted,
    'mdb_to_bronze':    MdbToBronze,
    'bronze_to_silver': BronzeToSilver,
    'transform_ibis':   TransformIbis,
    'measures_ibis':    MeasuresIbis,
    'promote_ibis':     PromoteIbis,
    'store_ibis':       StoreIbis,
}
```

- [ ] **Step 2: Update `MdbToBronze.dependencies` in `stages/mdb_to_bronze.py`**

Change line 22 from:

```python
    dependencies: list[str] = []
```

to:

```python
    dependencies: list[str] = ['ftp_to_extracted']
```

- [ ] **Step 3: Update `_STAGE_DEPS` in `tests/test_orchestrator.py`**

Replace the `_STAGE_DEPS` dict at the top of the file:

```python
_STAGE_DEPS = {
    'ftp_to_extracted': [],
    'mdb_to_bronze':    ['ftp_to_extracted'],
    'bronze_to_silver': ['mdb_to_bronze'],
    'transform_ibis':   ['bronze_to_silver'],
    'measures_ibis':    ['transform_ibis'],
    'promote_ibis':     ['measures_ibis'],
    'store_ibis':       ['promote_ibis'],
}
```

Also update `test_topological_sort_full_order` and `test_build_run_list_all` to include `ftp_to_extracted` first:

```python
def test_topological_sort_full_order():
    order = topological_sort(_STAGE_DEPS)
    assert order == [
        'ftp_to_extracted', 'mdb_to_bronze', 'bronze_to_silver', 'transform_ibis',
        'measures_ibis', 'promote_ibis', 'store_ibis',
    ]

def test_build_run_list_all():
    stages = build_run_list(_STAGE_DEPS, run_all=True)
    assert stages == [
        'ftp_to_extracted', 'mdb_to_bronze', 'bronze_to_silver', 'transform_ibis',
        'measures_ibis', 'promote_ibis', 'store_ibis',
    ]
```

- [ ] **Step 4: Run the full test suite**

```bash
python -m pytest tests/ -v
```
Expected: all tests pass (previously 64, now ~78 with new tests)

- [ ] **Step 5: Commit**

```bash
git add ibis.py stages/mdb_to_bronze.py tests/test_orchestrator.py
git commit -m "feat: wire FtpToExtracted into pipeline DAG"
```
