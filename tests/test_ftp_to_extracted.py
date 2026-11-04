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


def _run_with_archives(tmp_path, filenames, sevenz_side_effect):
    """Run the stage against one Kenya community listing `filenames`."""
    stage = FtpToExtracted(config=_make_config(), engine=MagicMock())
    downloads = []

    def sftp_factory(*args, **kwargs):
        instance = _mock_sftp(filenames)
        instance.download_file = MagicMock(
            side_effect=lambda remote, local: downloads.append(os.path.basename(remote))
        )
        return instance

    with patch('stages.ftp_to_extracted.get_decrypted_password', return_value='s'):
        with patch('stages.ftp_to_extracted.SFTPClient', side_effect=sftp_factory):
            with patch('stages.ftp_to_extracted.get_country_paths', return_value={
                'download_path': str(tmp_path / 'Downloads' / 'Kenya'),
                'extract_path': str(tmp_path / 'Extracted' / 'Kenya'),
            }):
                with patch('stages.ftp_to_extracted.py7zr.SevenZipFile',
                           side_effect=sevenz_side_effect):
                    with patch('stages.ftp_to_extracted.os.remove'):
                        result = stage.run()
    return result, downloads


def _sevenz_corrupt_for(corrupt_names):
    """SevenZipFile stand-in that fails for the named archives, extracts the rest."""
    extracted = []

    def fake_sevenz(path, *args, **kwargs):
        name = os.path.basename(path)
        if name in corrupt_names:
            raise Exception('invalid header data')
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=m)
        m.__exit__ = MagicMock(return_value=False)
        m.extractall.side_effect = lambda path: extracted.append(name)
        return m

    return fake_sevenz, extracted


def test_corrupt_newest_falls_back_to_previous_archive(tmp_path):
    newest = 'Tablet41_2026_11_04-17_10_10.7z'
    previous = 'Tablet41_2026_11_04-09_20_33.7z'
    oldest = 'Tablet41_2026_11_03-12_00_00.7z'
    fake_sevenz, extracted = _sevenz_corrupt_for({newest})

    result, downloads = _run_with_archives(
        tmp_path, [oldest, newest, previous], fake_sevenz,
    )

    assert result.success
    assert result.rows_written == 1
    assert downloads == [newest, previous]     # never reaches the oldest
    assert extracted == [previous]
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert warning['check'] == 'corrupt_archive'
    assert warning['affected_tablets'] == newest
    assert f"used older archive '{previous}' instead" in warning['detail']


def test_corrupt_newest_with_previous_already_extracted_loads_nothing(tmp_path):
    newest = 'Tablet41_2026_11_04-17_10_10.7z'
    previous = 'Tablet41_2026_11_04-09_20_33.7z'
    oldest = 'Tablet41_2026_11_03-12_00_00.7z'
    (tmp_path / 'Extracted' / 'Kenya' / previous[:-3]).mkdir(parents=True)
    fake_sevenz, extracted = _sevenz_corrupt_for({newest})

    result, downloads = _run_with_archives(
        tmp_path, [oldest, newest, previous], fake_sevenz,
    )

    assert result.success
    assert result.rows_written == 0
    assert downloads == [newest]               # stops at the extracted one
    assert extracted == []
    assert len(result.warnings) == 1
    assert result.warnings[0]['check'] == 'corrupt_archive'
    assert 'already extracted' in result.warnings[0]['detail']


def test_all_candidate_archives_corrupt_is_warning_not_error(tmp_path):
    names = [
        'Tablet41_2026_11_04-17_10_10.7z',
        'Tablet41_2026_11_04-09_20_33.7z',
        'Tablet41_2026_11_03-12_00_00.7z',
    ]
    fake_sevenz, extracted = _sevenz_corrupt_for(set(names))

    result, downloads = _run_with_archives(tmp_path, names, fake_sevenz)

    assert result.success
    assert result.errors == []
    assert result.rows_written == 0
    assert downloads == names                  # newest first, all tried
    assert extracted == []
    assert len(result.warnings) == 1           # one warning per tablet
    warning = result.warnings[0]
    assert warning['affected_tablets'] == names[0]
    assert 'all older archives were also corrupt' in warning['detail']


def test_partial_extraction_folder_removed_so_next_run_retries(tmp_path):
    """A failure partway through extractall leaves files behind. That folder
    must be removed, or the next run skips the archive as 'already extracted'
    and the ETL ingests a partial database."""
    newest = 'Tablet41_2026_11_04-17_10_10.7z'
    previous = 'Tablet41_2026_11_04-09_20_33.7z'
    partial_dir = tmp_path / 'Extracted' / 'Kenya' / newest[:-3]

    def fake_sevenz(path, *args, **kwargs):
        name = os.path.basename(path)
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=m)
        m.__exit__ = MagicMock(return_value=False)

        def extractall(path):
            os.makedirs(path, exist_ok=True)
            if name == newest:
                open(os.path.join(path, 'IBIS_pilot.mdb'), 'w').close()
                raise Exception('CRC error')
        m.extractall.side_effect = extractall
        return m

    result, downloads = _run_with_archives(tmp_path, [newest, previous], fake_sevenz)

    assert not partial_dir.exists()
    assert downloads == [newest, previous]
    assert result.rows_written == 1
    assert result.warnings[0]['affected_tablets'] == newest
