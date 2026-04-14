from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

BASE_DOWNLOAD_PATH = "Downloads"
BASE_EXTRACT_PATH = "Extracted"

REQUIRED_CONFIG_KEYS = ["ftp", "communities", "keyfiles", "access_table_name", "db", "trial", "schedule"]


def get_country_paths(country: str) -> dict:
    """
    Return download and extract directory paths for any country name.
    Replaces the old hardcoded COUNTRY_PATHS dict — any country now works
    without a code change.
    """
    country_dir = country.title()
    return {
        "download_path": f"{BASE_DOWNLOAD_PATH}/{country_dir}",
        "extract_path": f"{BASE_EXTRACT_PATH}/{country_dir}",
    }


class ConfigLoader:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self._validate()

    def _load_config(self, path: str) -> dict:
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: '{path}'")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config '{path}': {e}")

    def _validate(self) -> None:
        """Fail fast at startup if required config keys are missing."""
        missing = [k for k in REQUIRED_CONFIG_KEYS if k not in self.config]
        if missing:
            raise ValueError(
                f"Config is missing required key(s): {missing}. "
                f"Keys present: {list(self.config.keys())}"
            )
        logger.debug("Config validation passed.")

    def get(self, key: str, default=None):
        return self.config.get(key, default)
