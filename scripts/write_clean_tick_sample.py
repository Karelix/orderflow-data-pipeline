"""Write a capped local Parquet sample of cleaned tick rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.ingest.write_parquet import write_clean_tick_sample_parquet


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
        help="Raw Sierra file to convert. Defaults to raw_data.sample_file in config.",
    )
    parser.add_argument(
        "--output",
        default="data_local/tmp/clean_ticks_sample.parquet",
        help="Output Parquet path for the local sample.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100000,
        help="Maximum raw rows to scan for the sample output.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]

    result = write_clean_tick_sample_parquet(
        input_path=input_path,
        output_path=args.output,
        config=config,
        max_rows=args.max_rows,
    )

    print(f"Wrote: {result.path}")
    print(f"Rows: {result.rows}")
    print(f"File size bytes: {result.file_size_bytes}")


if __name__ == "__main__":
    main()
