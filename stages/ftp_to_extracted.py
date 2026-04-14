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
        ftp = self.config.get('ftp') or {}
        communities = self.config.get('communities') or {}
        keyfiles = self.config.get('keyfiles') or {}

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

            try:
                os.makedirs(download_dir, exist_ok=True)
                os.makedirs(extract_dir, exist_ok=True)

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

        return StageResult(
            success=len(errors) == 0,
            rows_written=total_downloaded,
            errors=errors,
        )
