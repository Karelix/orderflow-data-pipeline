"""Validate derived datasets built from a capped raw sample."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bars import build_time_bars
from src.config import load_config
from src.ingest import iter_clean_tick_rows
from src.profiles import build_footprint_clusters, build_volume_profiles
from src.sessions.session_summary import build_session_summaries
from src.validation import validate_derived_datasets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument(
        "--timeframe",
        action="append",
        default=None,
        help="Timeframe to validate. Can be repeated. Defaults to config timeframes.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]
    tick_size = Decimal(str(config["market"]["tick_size"]))
    timeframes = args.timeframe or config["derived_datasets"]["timeframes"]

    clean_rows = list(iter_clean_tick_rows(input_path, config, max_rows=args.max_rows))
    summaries = build_session_summaries(clean_rows)
    profiles = build_volume_profiles(clean_rows, tick_size=tick_size)

    bars = []
    footprints = []
    for timeframe in timeframes:
        bars.extend(build_time_bars(clean_rows, timeframe=timeframe))
        footprints.extend(
            build_footprint_clusters(
                clean_rows,
                timeframe=timeframe,
                tick_size=tick_size,
            )
        )

    report = validate_derived_datasets(
        clean_rows=clean_rows,
        bars=bars,
        session_summaries=summaries,
        footprint_clusters=footprints,
        volume_profiles=profiles,
        tick_size=tick_size,
    )

    print(f"Input: {input_path}")
    print(f"Raw rows scanned limit: {args.max_rows}")
    print(f"Clean rows: {len(clean_rows)}")
    print(report.format())

    if not report.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
