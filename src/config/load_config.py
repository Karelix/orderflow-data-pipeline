"""Load project configuration from YAML."""

from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("config/dataset.yaml")


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the dataset configuration file."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Config file did not contain a YAML mapping: {path}")

    return config
