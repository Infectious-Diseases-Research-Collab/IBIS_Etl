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
