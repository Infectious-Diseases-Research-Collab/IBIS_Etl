# Email Notification Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `modules/notifier.py` module that sends an email summary of stage results and validation issues after a pipeline run that has failures or ERROR-severity data problems.

**Architecture:** A single public function `send_pipeline_report()` is called at the end of `run_pipeline()` in `ibis.py`. It queries `gold_ibis.ds_validation_report`, builds a plain-text + HTML email, and sends via SMTP with Fernet-encrypted credentials. All exceptions are caught internally so a broken SMTP config never fails the pipeline.

**Tech Stack:** Python stdlib `smtplib`, `email.mime`; `cryptography.fernet` (already in requirements); `pandas` (already in use); SQLAlchemy engine (already in use).

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `modules/notifier.py` | Create | All notification logic — trigger check, body building, SMTP send |
| `tests/test_notifier.py` | Create | Unit tests for notifier (no real SMTP or DB) |
| `ibis.py` | Modify (lines 77–107) | Import notifier, call `send_pipeline_report` at end of `run_pipeline` |
| `config.json.example` | Modify | Add `email` block showing expected structure |

---

## Task 1: Credential loading

**Files:**
- Create: `modules/notifier.py`
- Create: `tests/test_notifier.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_notifier.py
from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from modules.notifier import _load_smtp_credentials


def test_load_smtp_credentials_roundtrip(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(
        f"Username={cipher.encrypt(b'user@example.com').decode()}\n"
        f"Password={cipher.encrypt(b's3cr3t').decode()}\n"
    )

    username, password = _load_smtp_credentials(str(ini_file), str(key_file))
    assert username == 'user@example.com'
    assert password == 's3cr3t'


def test_load_smtp_credentials_missing_username_raises(tmp_path):
    key = Fernet.generate_key()
    cipher = Fernet(key)

    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(f"Password={cipher.encrypt(b's3cr3t').decode()}\n")

    with pytest.raises(KeyError, match='Username'):
        _load_smtp_credentials(str(ini_file), str(key_file))
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_notifier.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` (module doesn't exist yet).

- [ ] **Step 3: Create `modules/notifier.py` with credential loader**

```python
# modules/notifier.py
from __future__ import annotations

import logging
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


def _load_smtp_credentials(ini_path: str, key_path: str) -> tuple[str, str]:
    """
    Read Fernet-encrypted SMTP credentials from ini_path using the key in key_path.
    Returns (username, password).
    Raises KeyError if 'Username' or 'Password' is absent from the ini file.
    """
    with open(key_path) as f:
        key = f.read().strip().encode()
    cipher = Fernet(key)

    cfg: dict[str, str] = {}
    with open(ini_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            cfg[k.strip()] = v.strip()

    if 'Username' not in cfg:
        raise KeyError(f"'Username' key not found in credential file: {ini_path}")
    if 'Password' not in cfg:
        raise KeyError(f"'Password' key not found in credential file: {ini_path}")

    return cipher.decrypt(cfg['Username'].encode()).decode(), \
           cipher.decrypt(cfg['Password'].encode()).decode()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_notifier.py -v
```

Expected: 2 PASSED.

- [ ] **Step 5: Commit**

```bash
git add modules/notifier.py tests/test_notifier.py
git commit -m "feat: add notifier module with SMTP credential loader"
```

---

## Task 2: Trigger logic

**Files:**
- Modify: `modules/notifier.py`
- Modify: `tests/test_notifier.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_notifier.py`:

```python
import pandas as pd
from unittest.mock import MagicMock, patch
from stages.base import StageResult
from modules.notifier import _should_notify


def _make_engine(report_df):
    """Return a mock SQLAlchemy engine that yields report_df from read_sql."""
    engine = MagicMock()
    return engine, report_df


def test_should_notify_true_on_stage_failure():
    results = {
        'mdb_to_bronze': StageResult(success=False, errors=['boom']),
        'bronze_to_silver': StageResult(success=True, rows_written=100),
    }
    engine = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, engine) is True


def test_should_notify_true_on_error_in_report():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['ERROR', 'WARNING'], 'check': ['a', 'b']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is True


def test_should_notify_false_on_clean_run():
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['WARNING'], 'check': ['a']})
    with patch('modules.notifier._query_validation_report', return_value=report):
        assert _should_notify(results, MagicMock()) is False


def test_should_notify_false_when_report_is_none():
    results = {'mdb_to_bronze': StageResult(success=True)}
    with patch('modules.notifier._query_validation_report', return_value=None):
        assert _should_notify(results, MagicMock()) is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_notifier.py::test_should_notify_true_on_stage_failure -v
```

Expected: `ImportError` — `_should_notify` not defined.

- [ ] **Step 3: Add `_query_validation_report` and `_should_notify` to `modules/notifier.py`**

Append to `modules/notifier.py` after the credential loader:

```python
import pandas as pd


def _query_validation_report(engine) -> pd.DataFrame | None:
    """
    Query gold_ibis.ds_validation_report.
    Returns None if the table does not exist or any error occurs.
    """
    try:
        return pd.read_sql('SELECT * FROM gold_ibis.ds_validation_report', engine)
    except Exception:
        return None


def _should_notify(results: dict, engine) -> bool:
    """
    Return True if the run has any failures OR any ERROR-severity validation rows.
    """
    if any(not r.success for r in results.values()):
        return True
    report = _query_validation_report(engine)
    if report is not None and not report.empty:
        if (report['severity'] == 'ERROR').any():
            return True
    return False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_notifier.py -v
```

Expected: 6 PASSED.

- [ ] **Step 5: Commit**

```bash
git add modules/notifier.py tests/test_notifier.py
git commit -m "feat: add trigger logic to notifier"
```

---

## Task 3: Email body builders

**Files:**
- Modify: `modules/notifier.py`
- Modify: `tests/test_notifier.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_notifier.py`:

```python
from modules.notifier import _build_stage_summary, _build_validation_section


def test_build_stage_summary_shows_all_statuses():
    results = {
        'mdb_to_bronze':    StageResult(success=True, rows_written=5416),
        'bronze_to_silver': StageResult(success=False, errors=['err']),
    }
    stages = ['mdb_to_bronze', 'bronze_to_silver', 'transform_ibis']
    text = _build_stage_summary(results, stages)
    assert '✓' in text and 'mdb_to_bronze' in text
    assert '✗' in text and 'bronze_to_silver' in text
    assert '—' in text and 'transform_ibis' in text
    assert '5,416' in text


def test_build_stage_summary_no_rows_for_zero():
    results = {'transform_ibis': StageResult(success=True, rows_written=0)}
    stages = ['transform_ibis']
    text = _build_stage_summary(results, stages)
    assert '✓' in text
    assert '0' not in text


def test_build_validation_section_none_engine():
    text = _build_validation_section(None)
    assert 'unavailable' in text.lower()


def test_build_validation_section_errors_and_warnings():
    report = pd.DataFrame({
        'severity':        ['ERROR', 'ERROR', 'WARNING', 'WARNING', 'WARNING'],
        'check':           ['dup_id', 'dup_id', 'missing_appt', 'missing_appt', 'sparse_col'],
        'country':         ['kenya', 'kenya', 'uganda', 'uganda', 'kenya'],
        'site':            ['21 (X)', '21 (X)', '11 (Y)', '11 (Y)', '21 (X)'],
        'record_count':    [2, 1, 3, 1, 5],
        'affected_subjids':['P001,P002', 'P003', 'U001,U002,U003', 'U004', ''],
    })
    text = _build_validation_section(report)
    assert 'Validation Errors' in text
    assert 'kenya / 21 (X)' in text
    assert 'P001, P002' in text
    assert 'Warnings (summary)' in text
    assert 'missing_appt' in text
    assert '3 warning(s)' in text


def test_build_validation_section_truncates_ids():
    ids = ','.join([f'P{i:03d}' for i in range(15)])
    report = pd.DataFrame({
        'severity':        ['ERROR'],
        'check':           ['dup_id'],
        'country':         ['kenya'],
        'site':            ['21 (X)'],
        'record_count':    [15],
        'affected_subjids':[ids],
    })
    text = _build_validation_section(report)
    assert '… and 5 more' in text
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_notifier.py::test_build_stage_summary_shows_all_statuses -v
```

Expected: `ImportError` — `_build_stage_summary` not defined.

- [ ] **Step 3: Add body builders to `modules/notifier.py`**

Append to `modules/notifier.py`:

```python
from stages.base import StageResult


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


def _build_validation_section(report_df: pd.DataFrame | None) -> str:
    if report_df is None:
        return 'Validation report unavailable — measures_ibis did not run.'

    errors = report_df[report_df['severity'] == 'ERROR']
    warnings = report_df[report_df['severity'] == 'WARNING']
    sep = '─' * 47
    lines: list[str] = []

    # --- Errors ---
    if errors.empty:
        lines.append('No validation errors.')
    else:
        lines += ['Validation Errors', sep]
        for (country, site), group in errors.groupby(['country', 'site'], sort=True):
            header = f'{country} / {site}' if site else str(country)
            lines.append(header)
            for _, row in group.iterrows():
                subjids = str(row.get('affected_subjids') or '')
                id_list = [s.strip() for s in subjids.split(',') if s.strip()]
                if len(id_list) > 10:
                    id_str = ', '.join(id_list[:10]) + f'  … and {len(id_list) - 10} more'
                else:
                    id_str = ', '.join(id_list)
                count = row.get('record_count', '')
                detail = f"  •  {row['check']:<32}  {count} record(s)"
                if id_str:
                    detail += f'  — IDs: {id_str}'
                lines.append(detail)
        lines.append(sep)

    lines.append('')

    # --- Warnings ---
    if warnings.empty:
        lines.append('No warnings.')
    else:
        warn_counts = warnings.groupby('check').size().sort_values(ascending=False)
        lines += ['Warnings (summary)', sep]
        for check, count in warn_counts.items():
            lines.append(f'  {check:<36}  {count}')
        lines.append(sep)
        lines.append(f'Total: {len(warnings)} warning(s)')

    return '\n'.join(lines)


def _build_body(
    results: dict[str, StageResult],
    stages: list[str],
    report_df: pd.DataFrame | None,
) -> tuple[str, str]:
    stage_section = _build_stage_summary(results, stages)
    validation_section = _build_validation_section(report_df)
    plain = f'{stage_section}\n\n{validation_section}'
    html = f'<pre style="font-family:monospace;font-size:13px">{plain}</pre>'
    return plain, html
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_notifier.py -v
```

Expected: 10 PASSED.

- [ ] **Step 5: Commit**

```bash
git add modules/notifier.py tests/test_notifier.py
git commit -m "feat: add email body builders to notifier"
```

---

## Task 4: SMTP send and public API

**Files:**
- Modify: `modules/notifier.py`
- Modify: `tests/test_notifier.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_notifier.py`:

```python
import smtplib
from unittest.mock import patch, MagicMock, call
from modules.notifier import send_pipeline_report


def _make_email_cfg(tmp_path):
    """Build a minimal email config with real Fernet credentials."""
    key = Fernet.generate_key()
    cipher = Fernet(key)
    key_file = tmp_path / 'smtp.key'
    ini_file = tmp_path / 'smtp.ini'
    key_file.write_text(key.decode())
    ini_file.write_text(
        f"Username={cipher.encrypt(b'user@example.com').decode()}\n"
        f"Password={cipher.encrypt(b's3cr3t').decode()}\n"
    )
    return {
        'smtp_host': 'smtp.example.com',
        'smtp_port': 587,
        'sender': 'ibis@example.com',
        'recipients': ['dm@example.com', 'pi@example.com'],
        'keyfiles': {
            'smtp_ini': str(ini_file),
            'smtp_key': str(key_file),
        },
    }


class _FakeConfig:
    def __init__(self, email_cfg):
        self._email_cfg = email_cfg

    def get(self, key, default=None):
        return self._email_cfg if key == 'email' else default


def test_send_pipeline_report_no_config_is_silent():
    """No email config → function returns without error."""
    config = _FakeConfig(None)
    send_pipeline_report(
        results={'mdb_to_bronze': StageResult(success=False)},
        stages=['mdb_to_bronze'],
        engine=MagicMock(),
        config=config,
    )
    # No exception = pass


def test_send_pipeline_report_clean_run_no_email(tmp_path):
    """Clean run with no ERRORs → no email sent."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=True)}
    report = pd.DataFrame({'severity': ['WARNING'], 'check': ['a']})

    with patch('modules.notifier._query_validation_report', return_value=report):
        with patch('smtplib.SMTP') as mock_smtp:
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )
    mock_smtp.assert_not_called()


def test_send_pipeline_report_sends_on_failure(tmp_path):
    """Stage failure → email is sent to all recipients."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False, errors=['boom'])}

    mock_smtp_instance = MagicMock()
    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP', return_value=mock_smtp_instance) as mock_smtp_cls:
            mock_smtp_cls.return_value.__enter__ = lambda s: mock_smtp_instance
            mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )

    mock_smtp_instance.starttls.assert_called_once()
    mock_smtp_instance.login.assert_called_once_with('user@example.com', 's3cr3t')
    sendmail_args = mock_smtp_instance.sendmail.call_args
    assert 'dm@example.com' in sendmail_args[0][1]
    assert 'pi@example.com' in sendmail_args[0][1]


def test_send_pipeline_report_does_not_raise_on_smtp_error(tmp_path):
    """SMTP failure is logged and swallowed — pipeline is unaffected."""
    config = _FakeConfig(_make_email_cfg(tmp_path))
    results = {'mdb_to_bronze': StageResult(success=False)}

    with patch('modules.notifier._query_validation_report', return_value=None):
        with patch('smtplib.SMTP', side_effect=smtplib.SMTPException('conn refused')):
            # Must not raise
            send_pipeline_report(
                results=results, stages=['mdb_to_bronze'],
                engine=MagicMock(), config=config,
            )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_notifier.py::test_send_pipeline_report_no_config_is_silent -v
```

Expected: `ImportError` — `send_pipeline_report` not defined.

- [ ] **Step 3: Add SMTP send and public API to `modules/notifier.py`**

Append to `modules/notifier.py`:

```python
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def _send(email_cfg: dict, subject: str, plain: str, html: str) -> None:
    ini_path = email_cfg['keyfiles']['smtp_ini']
    key_path = email_cfg['keyfiles']['smtp_key']
    username, password = _load_smtp_credentials(ini_path, key_path)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email_cfg['sender']
    msg['To'] = ', '.join(email_cfg['recipients'])
    msg.attach(MIMEText(plain, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    with smtplib.SMTP(email_cfg['smtp_host'], email_cfg['smtp_port']) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(email_cfg['sender'], email_cfg['recipients'], msg.as_string())


def send_pipeline_report(
    results: dict[str, StageResult],
    stages: list[str],
    engine,
    config,
) -> None:
    """
    Send an email summary of the pipeline run if any stage failed or validation
    ERRORs exist. Silently returns if no email config is present or the run is clean.
    Never raises — SMTP errors are logged and swallowed.
    """
    email_cfg = config.get('email')
    if not email_cfg:
        return
    try:
        if not _should_notify(results, engine):
            logger.info('Notifier: clean run — no email sent.')
            return
        report_df = _query_validation_report(engine)
        today = date.today().strftime('%d %b %Y')
        subject = f'IBIS Pipeline \u2014 Issues found ({today})'
        plain, html = _build_body(results, stages, report_df)
        _send(email_cfg, subject, plain, html)
        logger.info(f'Pipeline report sent to {email_cfg["recipients"]}.')
    except Exception as exc:
        logger.error(f'Notifier failed \u2014 email not sent: {exc}')
```

- [ ] **Step 4: Run all notifier tests**

```bash
python -m pytest tests/test_notifier.py -v
```

Expected: 14 PASSED.

- [ ] **Step 5: Commit**

```bash
git add modules/notifier.py tests/test_notifier.py
git commit -m "feat: add SMTP send and public API to notifier"
```

---

## Task 5: Integrate into `ibis.py`

**Files:**
- Modify: `ibis.py` (lines 1–10 for import, lines 77–107 for `run_pipeline`)
- Modify: `tests/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_orchestrator.py`:

```python
from unittest.mock import patch, MagicMock
from stages.base import StageResult
from ibis import run_pipeline, STAGE_CLASSES


def test_run_pipeline_calls_notifier_on_failure():
    """send_pipeline_report is called after a failed run."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=False, errors=['boom'])):
        with patch('ibis.send_pipeline_report') as mock_notify:
            with patch('sys.exit'):
                run_pipeline(['mdb_to_bronze'], config, engine)

    mock_notify.assert_called_once()
    call_kwargs = mock_notify.call_args.kwargs
    assert 'mdb_to_bronze' in call_kwargs['results']
    assert call_kwargs['stages'] == ['mdb_to_bronze']


def test_run_pipeline_calls_notifier_on_success():
    """send_pipeline_report is called even on a clean run (it decides internally)."""
    config = MagicMock()
    engine = MagicMock()

    with patch.object(STAGE_CLASSES['mdb_to_bronze'], 'run',
                      return_value=StageResult(success=True, rows_written=10)):
        with patch('ibis.send_pipeline_report') as mock_notify:
            run_pipeline(['mdb_to_bronze'], config, engine)

    mock_notify.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_orchestrator.py::test_run_pipeline_calls_notifier_on_failure -v
```

Expected: FAIL — `send_pipeline_report` not imported in `ibis.py`.

- [ ] **Step 3: Modify `ibis.py`**

Add import at the top of `ibis.py` (after the existing stage imports, line 18):

```python
from modules.notifier import send_pipeline_report
```

Replace the body of `run_pipeline` in `ibis.py` (lines 77–107). The only change is adding the `send_pipeline_report` call before `sys.exit`:

```python
def run_pipeline(stages: list[str], config: ConfigLoader, engine) -> None:
    results: dict[str, StageResult] = {}
    failed: set[str] = set()

    for name in stages:
        cls = STAGE_CLASSES[name]
        blocked_by = [d for d in cls.dependencies if d in failed]
        if blocked_by:
            logger.warning(f"Skipping '{name}' — upstream failure(s): {blocked_by}")
            failed.add(name)
            continue

        logger.info(f"=== Running stage: {name} ===")
        stage = cls(config=config, engine=engine)
        try:
            result = stage.run()
        except Exception as exc:
            result = StageResult(success=False, errors=[str(exc)])
            logger.exception(f"Stage '{name}' raised an unexpected exception.")

        results[name] = result
        if not result.success:
            failed.add(name)
            for err in result.errors:
                logger.error(f"  [{name}] {err}")
        else:
            logger.info(f"  [{name}] OK — {result.rows_written} row(s) written.")

    _log_summary(results, failed)
    send_pipeline_report(results=results, stages=stages, engine=engine, config=config)
    if failed:
        sys.exit(1)
```

- [ ] **Step 4: Run all orchestrator tests**

```bash
python -m pytest tests/test_orchestrator.py -v
```

Expected: all existing tests + 2 new = all PASSED.

- [ ] **Step 5: Run full test suite**

```bash
python -m pytest tests/ -v
```

Expected: all tests PASSED (no regressions).

- [ ] **Step 6: Commit**

```bash
git add ibis.py tests/test_orchestrator.py
git commit -m "feat: call send_pipeline_report at end of pipeline run"
```

---

## Task 6: Update `config.json.example`

**Files:**
- Modify: `config.json.example`

- [ ] **Step 1: Add email block to `config.json.example`**

Open `config.json.example` and add an `"email"` key after `"schedule"`:

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

The full `config.json.example` should be valid JSON after the edit. Verify:

```bash
python -c "import json; json.load(open('config.json.example')); print('valid')"
```

Expected: `valid`

- [ ] **Step 2: Commit**

```bash
git add config.json.example
git commit -m "docs: add email block to config.json.example"
```

---

## Final check

```bash
python -m pytest tests/ -v
```

Expected: all tests PASSED.
