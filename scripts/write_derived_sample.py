"""Write capped local Parquet samples for derived datasets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.ingest.write_parquet import write_derived_sample_parquets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output-root", default="data_local/tmp/derived_sample")
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument(
        "--timeframe",
        action="append",
        default=None,
        help="Timeframe to write. Can be repeated. Defaults to config timeframes.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]
    results = write_derived_sample_parquets(
        input_path=input_path,
        output_root=args.output_root,
        config=config,
        max_rows=args.max_rows,
        timeframes=args.timeframe,
    )

    print(f"Input: {input_path}")
    print(f"Output root: {args.output_root}")
    print(f"Raw rows scanned limit: {args.max_rows}")
    for result in results:
        print(f"{result.path} | rows={result.rows} | bytes={result.file_size_bytes}")


if __name__ == "__main__":
    main()
