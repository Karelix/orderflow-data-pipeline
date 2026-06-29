"""Stream-validate a full raw Sierra file without writing derived data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.validation import validate_raw_stream
from src.validation.raw_stream import RawStreamChunkSummary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--chunk-size", type=int, default=100000)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-chunk progress output.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]

    def report_progress(summary: RawStreamChunkSummary) -> None:
        if args.quiet:
            return

        print(
            "chunk={chunk} rows={rows} first={first} last={last} volume={volume} delta={delta}".format(
                chunk=summary.chunk_index,
                rows=summary.rows,
                first=summary.first_timestamp,
                last=summary.last_timestamp,
                volume=summary.total_volume,
                delta=summary.total_delta,
            ),
            flush=True,
        )

    report = validate_raw_stream(
        path=input_path,
        config=config,
        chunk_size=args.chunk_size,
        max_rows=args.max_rows,
        progress_callback=report_progress,
    )

    print(report.format())

    if not report.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
