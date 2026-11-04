# stages/ftp_to_extracted.py
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import py7zr

from modules.config import get_country_paths
from modules.sftp_client import SFTPClient, group_remote_archives_by_tablet
from modules.utils import get_decrypted_password
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

_MAX_DOWNLOAD_RETRIES = 3
_MAX_WORKERS = 4


def _download_with_retry(
    hostname: str,
    username: str,
    ftp_password: str,
    remote_path: str,
    filename: str,
    local_archive: str,
) -> None:
    """Download a single archive, retrying up to _MAX_DOWNLOAD_RETRIES times on network errors."""
    last_exc: Exception = Exception('no attempts made')
    for attempt in range(1, _MAX_DOWNLOAD_RETRIES + 1):
        try:
            with SFTPClient(hostname, username, ftp_password) as sftp:
                sftp.download_file(remote_path + filename, local_archive)
            return
        except Exception as exc:
            last_exc = exc
            logger.warning(
                f"Download attempt {attempt}/{_MAX_DOWNLOAD_RETRIES} failed "
                f"for '{filename}': {exc}"
            )
            # Do not remove the partial file between retries — paramiko opens
            # in write mode and will overwrite it. Cleaning up here caused
            # spurious "No such file or directory" errors on subsequent attempts.
    raise last_exc


def _process_tablet(
    hostname: str,
    username: str,
    ftp_password: str,
    sevenz_password: str,
    remote_path: str,
    filename: str,
    download_dir: str,
    extract_dir: str,
    country: str,
) -> tuple[int, str | None, dict | None]:
    """
    Download, extract and delete one tablet archive.
    Returns:
      (1, None, None)           — success
      (0, None, None)           — skipped (already extracted)
      (0, error_msg, None)      — fatal: SFTP/download failure
      (0, None, warning_dict)   — non-fatal: corrupt archive (bad header etc.)
    Each call uses its own SFTP connection — safe to run concurrently.
    """
    folder_name = filename[:-3]  # strip .7z
    tablet_extract_dir = os.path.join(extract_dir, folder_name)

    if os.path.exists(tablet_extract_dir):
        logger.info(f"[{country}] Skipping {filename} — already extracted.")
        return 0, None, None

    local_archive = os.path.join(download_dir, filename)
    try:
        # Retry download on network errors (e.g. Mismatched MAC).
        _download_with_retry(
            hostname, username, ftp_password,
            remote_path, filename, local_archive,
        )
    except Exception as exc:
        msg = f"[{country}] Failed to download '{filename}': {exc}"
        logger.error(msg)
        if os.path.exists(local_archive):
            os.remove(local_archive)
        return 0, msg, None

    try:
        with py7zr.SevenZipFile(
            local_archive, mode='r', password=sevenz_password
        ) as archive:
            archive.extractall(path=tablet_extract_dir)
        os.remove(local_archive)
        logger.info(f"[{country}] Extracted {filename} → {tablet_extract_dir}")
        return 1, None, None
    except Exception as exc:
        logger.warning(f"[{country}] Corrupt archive '{filename}': {exc} — skipping.")
        if os.path.exists(local_archive):
            os.remove(local_archive)
        warning = dict(
            check='corrupt_archive',
            severity='ERROR',
            country=country,
            site=None,
            field='archive',
            record_count=1,
            detail=f"Archive '{filename}' could not be extracted: {exc}",
            affected_subjids=None,
            affected_tablets=filename,
        )
        return 0, None, warning


def _process_tablet_with_fallback(
    hostname: str,
    username: str,
    ftp_password: str,
    sevenz_password: str,
    remote_path: str,
    candidates: list[str],
    download_dir: str,
    extract_dir: str,
    country: str,
) -> tuple[int, str | None, dict | None]:
    """
    Try one tablet's archives newest first, stopping at the first that
    extracts cleanly. Snapshots are cumulative, so when the newest archive is
    corrupt (e.g. truncated mid-upload) the next-newest good one is enough.

    Stops without extracting anything when it reaches an archive that is
    already extracted locally — nothing older than that is newer than the data
    we already have — or on a download failure. Returns the same tuple as
    _process_tablet; the warning, if any, is the newest corrupt archive's,
    with the fallback outcome appended to its detail.
    """
    warning: dict | None = None
    corrupt: list[str] = []

    for filename in candidates:
        downloaded, error_msg, attempt_warning = _process_tablet(
            hostname, username, ftp_password, sevenz_password,
            remote_path, filename, download_dir, extract_dir, country,
        )
        if attempt_warning is None:
            if warning is not None:
                if downloaded:
                    outcome = f"used older archive '{filename}' instead"
                    logger.info(f"[{country}] Fell back to {filename} after corrupt {corrupt[0]}.")
                elif error_msg:
                    outcome = f"older archive '{filename}' failed to download"
                else:
                    outcome = (
                        f"older archive '{filename}' is already extracted; "
                        "no newer data loaded"
                    )
                warning['detail'] += _fallback_note(corrupt, outcome)
            return downloaded, error_msg, warning

        corrupt.append(filename)
        if warning is None:
            warning = attempt_warning

    if warning is not None and len(corrupt) > 1:
        warning['detail'] += _fallback_note(corrupt, 'all older archives were also corrupt')
    elif warning is not None:
        warning['detail'] += '; no older archive available'
    return 0, None, warning


def _fallback_note(corrupt: list[str], outcome: str) -> str:
    older_corrupt = corrupt[1:]
    note = f"; {outcome}"
    if older_corrupt:
        note += f" (also corrupt: {', '.join(older_corrupt)})"
    return note


class FtpToExtracted(BaseStage):
    name = 'ftp_to_extracted'
    dependencies: list[str] = []

    def run(self) -> StageResult:
        ftp = self.config.get('ftp') or {}
        communities = self.config.get('communities') or {}
        keyfiles = self.config.get('keyfiles') or {}

        hostname = ftp['hostname']
        username = ftp['username_ibis']
        ftp_password = get_decrypted_password(
            keyfiles['ftp_cred_filename_IBIS'],
            'IBIS_FTP_FERNET_KEY',
            keyfiles.get('ftp_key_file_IBIS'),
        )
        sevenz_password = get_decrypted_password(
            keyfiles['sevenz_cred_filename'],
            'IBIS_SEVENZ_FERNET_KEY',
            keyfiles.get('sevenz_key_file'),
        )

        total_downloaded = 0
        errors: list[str] = []
        warnings: list[dict] = []

        for community_key, community in communities.items():
            country = community['country']
            remote_path = community['remotefilepath_download']
            paths = get_country_paths(country)
            download_dir = paths['download_path']
            extract_dir = paths['extract_path']

            try:
                os.makedirs(download_dir, exist_ok=True)
                os.makedirs(extract_dir, exist_ok=True)

                # List files with a short-lived connection, then close it.
                with SFTPClient(hostname, username, ftp_password) as sftp:
                    filenames = sftp.list_files(remote_path)

                by_tablet = group_remote_archives_by_tablet(filenames)
                logger.info(
                    f"[{country}] {len(by_tablet)} tablet(s) with archives found on FTP."
                )

                # Download tablets in parallel — each worker uses its own connection.
                # Each tablet starts at its newest archive and falls back to
                # older ones only if that archive is corrupt.
                with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                    futures = {
                        executor.submit(
                            _process_tablet_with_fallback,
                            hostname, username, ftp_password, sevenz_password,
                            remote_path, candidates, download_dir, extract_dir, country,
                        ): tablet_id
                        for tablet_id, candidates in sorted(by_tablet.items())
                    }
                    for future in as_completed(futures):
                        downloaded, error_msg, warning = future.result()
                        total_downloaded += downloaded
                        if error_msg:
                            errors.append(error_msg)
                        if warning:
                            warnings.append(warning)

            except Exception as exc:
                msg = f"[{country}] SFTP connection failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        # Stage succeeds unless there are fatal (download/SFTP) errors.
        # Corrupt archives are non-blocking and surfaced via warnings.
        return StageResult(
            success=len(errors) == 0,
            rows_written=total_downloaded,
            errors=errors,
            warnings=warnings,
        )
