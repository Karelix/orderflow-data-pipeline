"""Write full-style partitioned derived Parquet datasets locally."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.streaming import write_partitioned_derived_parquets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output-root", default="data_local/tmp/partitioned_derived")
    parser.add_argument("--chunk-size", type=int, default=None)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--timeframe",
        action="append",
        default=None,
        help="Timeframe to write. Can be repeated. Defaults to config timeframes.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]
    results = write_partitioned_derived_parquets(
        input_path=input_path,
        output_root=args.output_root,
        config=config,
        chunk_size_rows=args.chunk_size,
        max_rows=args.max_rows,
        timeframes=args.timeframe,
    )

    print(f"Input: {input_path}")
    print(f"Output root: {args.output_root}")
    print(f"Partitions written: {len(results)}")
    for result in results:
        print(
            f"{result.dataset_type} | session_date={result.session_date} "
            f"| timeframe={result.timeframe or ''} | rows={result.rows} "
            f"| bytes={result.file_size_bytes} | path={result.path}"
        )


if __name__ == "__main__":
    main()
