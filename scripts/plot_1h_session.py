"""Plot one session date's 1h bars as an interactive HTML chart."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.visualization import load_1h_bars_for_session, write_1h_session_plot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--session-date", required=True, help="YYYY-MM-DD session date.")
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--data-tier", default=None, help="Defaults to config value/main.")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--contract", default=None)
    parser.add_argument("--input-parquet", default=None)
    parser.add_argument("--download-dir", default="data_local/tmp/hf_downloads")
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--remote-path-contains",
        default=None,
        help="Optional substring to disambiguate multiple manifest matches.",
    )
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    selected_session_date = date.fromisoformat(args.session_date)
    selected_contract = args.contract or config["dataset"]["default_contract"]
    selected_data_tier = _select_data_tier(args.data_tier, config)
    output_path = Path(
        args.output
        or (
            "data_local/plots/"
            f"{selected_contract}_1h_{selected_session_date.isoformat()}.html"
        )
    )

    loaded_bars = load_1h_bars_for_session(
        config=config,
        session_date=selected_session_date,
        manifest_path=args.manifest,
        data_tier=selected_data_tier,
        symbol=args.symbol,
        contract=selected_contract,
        download_dir=args.download_dir,
        env_file=args.env_file,
        input_parquet=args.input_parquet,
        remote_path_contains=args.remote_path_contains,
    )
    written_path = write_1h_session_plot(
        loaded_bars=loaded_bars,
        session_date=selected_session_date,
        output_path=output_path,
        title=f"{selected_contract} 1h bars - {selected_session_date.isoformat()}",
    )

    print(f"Rows plotted: {len(loaded_bars.rows)}")
    print(f"Source Parquet: {loaded_bars.parquet_path}")

    if loaded_bars.manifest_entry is not None:
        print(f"Repo: {loaded_bars.manifest_entry.repo_id}")
        print(f"Remote: {loaded_bars.manifest_entry.remote_path}")
        print(f"Data tier: {loaded_bars.manifest_entry.data_tier}")

    print(f"Plot: {written_path}")
    return 0


def _select_data_tier(value: str | None, config: dict) -> str | None:
    selected = value or config.get("manifest", {}).get("default_query_data_tier", "main")

    if selected.lower() == "all":
        return None

    return selected


if __name__ == "__main__":
    raise SystemExit(main())
