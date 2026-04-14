import pytest
from modules.config import ConfigLoader
import json

def _write_config(tmp_path, data):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(data))
    return str(p)

BASE = {
    "communities": {"k": {"community_name": "Sindo", "country": "kenya", "remotefilepath_download": "/Kenya/Sindo/"}},
    "ftp": {"hostname": "h", "username_ibis": "u"},
    "keyfiles": {"ftp_cred_filename_IBIS": "a", "ftp_key_file_IBIS": "b", "sevenz_cred_filename": "c", "sevenz_key_file": "d"},
    "access_table_name": "baseline",
}

def test_config_requires_db_block(tmp_path):
    data = {**BASE,
            "trial": {"name": "ibis", "dedup_key": "uniqueid", "dedup_strategy": "latest_snapshot", "country_code_map": {}},
            "schedule": {"pipeline_cron": "0 2 * * *", "store_cron": "0 3 * * 0"}}
    cfg = _write_config(tmp_path, data)
    with pytest.raises(ValueError, match="db"):
        ConfigLoader(cfg)

def test_config_requires_trial_block(tmp_path):
    data = {**BASE, "db": {"host": "db", "port": 5432, "name": "ibis", "user": "u", "password_env": "PW"}}
    cfg = _write_config(tmp_path, data)
    with pytest.raises(ValueError, match="trial"):
        ConfigLoader(cfg)

def test_config_requires_schedule_block(tmp_path):
    data = {**BASE,
            "db": {"host": "db", "port": 5432, "name": "ibis", "user": "u", "password_env": "PW"},
            "trial": {"name": "ibis", "dedup_key": "uniqueid", "dedup_strategy": "latest_snapshot", "country_code_map": {}}}
    cfg = _write_config(tmp_path, data)
    with pytest.raises(ValueError, match="schedule"):
        ConfigLoader(cfg)

def test_config_valid_full(tmp_path):
    data = {**BASE,
            "db": {"host": "db", "port": 5432, "name": "ibis", "user": "u", "password_env": "PW"},
            "trial": {"name": "ibis", "dedup_key": "uniqueid", "dedup_strategy": "latest_snapshot", "country_code_map": {}},
            "schedule": {"pipeline_cron": "0 2 * * *", "store_cron": "0 3 * * 0"}}
    cfg = _write_config(tmp_path, data)
    loader = ConfigLoader(cfg)
    assert loader.get('trial')['name'] == 'ibis'
