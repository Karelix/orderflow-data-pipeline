"""Query the dataset manifest for files that contain a requested slice."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.manifest import find_manifest_entries, read_manifest_parquet


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--dataset-type", default=None)
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--contract", default=None)
    parser.add_argument("--timeframe", default=None)
    parser.add_argument("--session-type", default=None)
    parser.add_argument("--session-date", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    manifest_path = Path(args.manifest or config["paths"]["manifest_path"])

    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}")
        return 1

    session_date = _parse_date(args.session_date) if args.session_date else None
    entries = read_manifest_parquet(manifest_path)
    matches = find_manifest_entries(
        entries=entries,
        dataset_type=args.dataset_type,
        symbol=args.symbol,
        contract=args.contract,
        timeframe=args.timeframe,
        session_type=args.session_type,
        session_date=session_date,
    )

    if not matches:
        print("No matching manifest entries found.")
        return 2

    for entry in matches:
        session_types = entry.session_type_values or entry.session_type or ""
        session_range = _format_range(entry.session_date_min, entry.session_date_max)
        size = _format_bytes(entry.file_size_bytes)
        print(
            " | ".join(
                [
                    f"repo={entry.repo_id}",
                    f"seq={entry.repo_sequence}",
                    f"type={entry.dataset_type}",
                    f"timeframe={entry.timeframe or ''}",
                    f"sessions={session_types}",
                    f"dates={session_range}",
                    f"rows={entry.rows}",
                    f"size={size}",
                    f"remote={entry.remote_path}",
                ]
            )
        )

    return 0


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _format_range(start: date | None, end: date | None) -> str:
    if start is None or end is None:
        return ""

    if start == end:
        return start.isoformat()

    return f"{start.isoformat()}..{end.isoformat()}"


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}"

        size /= 1024

    return f"{value}B"


if __name__ == "__main__":
    raise SystemExit(main())
