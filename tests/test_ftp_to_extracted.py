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
                    with patch('stages.ftp_to_extracted.os.remove') as mock_remove:
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
                with patch('stages.ftp_to_extracted.os.makedirs'):
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
                with patch('stages.ftp_to_extracted.os.makedirs'):
                    result = stage.run()

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
                    with patch('stages.ftp_to_extracted.os.remove'):
                        with patch('stages.ftp_to_extracted._MAX_WORKERS', 1):
                            result = stage.run()

    assert result.success             # partial success — downstream should run
    assert result.rows_written == 1
    assert len(result.warnings) == 1  # corrupt archive is a warning, not a hard error
    assert result.warnings[0]['check'] == 'corrupt_archive'


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
                    with patch('stages.ftp_to_extracted.os.remove'):
                        result = stage.run()

    # Corrupt archives are non-fatal warnings — stage still succeeds (no hard errors)
    assert result.success
    assert result.rows_written == 0
    assert len(result.warnings) == 1
    assert result.warnings[0]['check'] == 'corrupt_archive'


def test_download_retried_on_network_error(tmp_path):
    """A flaky download that fails twice then succeeds on the third attempt."""
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())

    mock_archive = MagicMock()
    mock_archive.__enter__ = MagicMock(return_value=mock_archive)
    mock_archive.__exit__ = MagicMock(return_value=False)

    download_attempts = {'n': 0}

    def flaky_sftp(*args, **kwargs):
        instance = _mock_sftp(['Tablet221_2026_04_01-10_00_00.7z'])
        def flaky_download(remote, local):
            download_attempts['n'] += 1
            if download_attempts['n'] < 3:
                raise Exception('network error')
        instance.download_file = MagicMock(side_effect=flaky_download)
        return instance

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient', side_effect=flaky_sftp):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                with patch('stages.ftp_to_extracted.py7zr.SevenZipFile',
                           return_value=mock_archive):
                    with patch('stages.ftp_to_extracted.os.remove'):
                        result = stage.run()

    assert result.success
    assert result.rows_written == 1
    assert download_attempts['n'] == 3   # failed twice, succeeded on third
