# tests/test_sftp_client.py
import pytest
from unittest.mock import MagicMock, patch

from modules.sftp_client import select_latest_remote_per_tablet, SFTPClient


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
