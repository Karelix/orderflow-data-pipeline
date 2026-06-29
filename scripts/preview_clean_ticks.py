"""Preview cleaned tick rows without writing large outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.ingest import iter_clean_tick_rows


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
        help="Raw Sierra file to preview. Defaults to raw_data.sample_file in config.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=10,
        help="Read only the first N raw rows.",
    )
    parser.add_argument(
        "--show-rows",
        type=int,
        default=5,
        help="Number of cleaned rows to print.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]

    scanned_clean_rows = 0
    preview_rows = []

    for row in iter_clean_tick_rows(input_path, config, max_rows=args.max_rows):
        scanned_clean_rows += 1
        if len(preview_rows) < args.show_rows:
            preview_rows.append(row.to_display_dict())

    print(f"Input: {input_path}")
    print(f"Raw rows scanned limit: {args.max_rows}")
    print(f"Clean rows emitted: {scanned_clean_rows}")
    print(json.dumps(preview_rows, indent=2))


if __name__ == "__main__":
    main()
