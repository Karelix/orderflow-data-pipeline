"""Inspect the configured raw Sierra test file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.ingest import format_inspection_report, inspect_raw_file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="config/dataset.yaml",
        help="Path to dataset YAML config.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Raw Sierra file to inspect. Defaults to raw_data.sample_file in config.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Inspect only the first N rows for a quick sanity check.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]
    inspection = inspect_raw_file(input_path, config, max_rows=args.max_rows)

    print(format_inspection_report(inspection))


if __name__ == "__main__":
    main()
