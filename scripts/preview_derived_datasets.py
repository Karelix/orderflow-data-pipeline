"""Preview bars, summaries, footprints, and profiles from a capped raw sample."""

from __future__ import annotations

import argparse
import json
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--show-rows", type=int, default=3)
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]
    tick_size = Decimal(str(config["market"]["tick_size"]))

    clean_rows = list(iter_clean_tick_rows(input_path, config, max_rows=args.max_rows))
    bars = build_time_bars(clean_rows, timeframe=args.timeframe)
    summaries = build_session_summaries(clean_rows)
    clusters = build_footprint_clusters(clean_rows, timeframe=args.timeframe, tick_size=tick_size)
    profiles = build_volume_profiles(clean_rows, tick_size=tick_size)

    payload = {
        "input": str(input_path),
        "raw_rows_scanned_limit": args.max_rows,
        "clean_rows": len(clean_rows),
        "bars": {
            "count": len(bars),
            "preview": [row.to_display_dict() for row in bars[: args.show_rows]],
        },
        "session_summaries": {
            "count": len(summaries),
            "preview": [row.to_display_dict() for row in summaries[: args.show_rows]],
        },
        "footprint_clusters": {
            "count": len(clusters),
            "preview": [row.to_display_dict() for row in clusters[: args.show_rows]],
        },
        "volume_profiles": {
            "count": len(profiles),
            "preview": [row.to_display_dict() for row in profiles[: args.show_rows]],
        },
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
