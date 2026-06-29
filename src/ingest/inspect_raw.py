"""Inspect raw Sierra Chart CSV exports before conversion."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class RawFileInspection:
    """Summary statistics for one raw Sierra export."""

    path: Path
    file_size_bytes: int
    columns: list[str]
    row_count: int
    is_partial: bool
    max_rows: int | None
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    duplicate_timestamp_count: int
    min_last_price: Decimal | None
    max_last_price: Decimal | None
    total_volume: int
    total_bid_volume: int
    total_ask_volume: int


def inspect_raw_file(
    path: str | Path,
    config: Mapping[str, Any],
    max_rows: int | None = None,
) -> RawFileInspection:
    """Read a Sierra CSV/text export and return basic data-quality stats."""
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    raw_config = config["raw_data"]
    datetime_config = raw_config["datetime"]
    columns_config = raw_config["columns"]

    date_column = datetime_config["date_column"]
    time_column = datetime_config["time_column"]
    date_format = datetime_config["date_format"]
    time_formats = datetime_config.get(
        "time_formats",
        [datetime_config["time_format"]],
    )

    last_column = columns_config["last"]
    volume_column = columns_config["volume"]
    bid_volume_column = columns_config["bid_volume"]
    ask_volume_column = columns_config["ask_volume"]

    required_columns = [
        date_column,
        time_column,
        last_column,
        volume_column,
        bid_volume_column,
        ask_volume_column,
    ]

    row_count = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    seen_timestamps: set[datetime] = set()
    duplicate_timestamp_count = 0
    min_last_price: Decimal | None = None
    max_last_price: Decimal | None = None
    total_volume = 0
    total_bid_volume = 0
    total_ask_volume = 0

    with file_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, skipinitialspace=True)

        if reader.fieldnames is None:
            raise ValueError(f"Raw file is empty or missing a header: {file_path}")

        reader.fieldnames = [column.strip() for column in reader.fieldnames]
        columns = list(reader.fieldnames)
        _require_columns(columns, required_columns)

        for row in reader:
            if max_rows is not None and row_count >= max_rows:
                break

            timestamp = parse_sierra_timestamp(
                row[date_column],
                row[time_column],
                date_format,
                time_formats,
            )
            last_price = Decimal(row[last_column])
            volume = int(row[volume_column])
            bid_volume = int(row[bid_volume_column])
            ask_volume = int(row[ask_volume_column])

            row_count += 1
            first_timestamp = first_timestamp or timestamp
            last_timestamp = timestamp

            if timestamp in seen_timestamps:
                duplicate_timestamp_count += 1
            else:
                seen_timestamps.add(timestamp)

            min_last_price = _min_decimal(min_last_price, last_price)
            max_last_price = _max_decimal(max_last_price, last_price)
            total_volume += volume
            total_bid_volume += bid_volume
            total_ask_volume += ask_volume

    return RawFileInspection(
        path=file_path,
        file_size_bytes=file_path.stat().st_size,
        columns=columns,
        row_count=row_count,
        is_partial=max_rows is not None,
        max_rows=max_rows,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        duplicate_timestamp_count=duplicate_timestamp_count,
        min_last_price=min_last_price,
        max_last_price=max_last_price,
        total_volume=total_volume,
        total_bid_volume=total_bid_volume,
        total_ask_volume=total_ask_volume,
    )


def format_inspection_report(inspection: RawFileInspection) -> str:
    """Format inspection stats for terminal output."""
    lines = [
        "Raw Sierra File Inspection",
        "==========================",
        f"Path: {inspection.path}",
        f"File size bytes: {inspection.file_size_bytes}",
        f"Columns: {', '.join(inspection.columns)}",
        _format_row_count(inspection),
        f"First timestamp: {_format_optional(inspection.first_timestamp)}",
        f"Last timestamp: {_format_optional(inspection.last_timestamp)}",
        f"Duplicate timestamp rows: {inspection.duplicate_timestamp_count}",
        f"Min Last price: {_format_optional(inspection.min_last_price)}",
        f"Max Last price: {_format_optional(inspection.max_last_price)}",
        f"Total volume: {inspection.total_volume}",
        f"Total bid volume: {inspection.total_bid_volume}",
        f"Total ask volume: {inspection.total_ask_volume}",
    ]
    return "\n".join(lines)


def parse_sierra_timestamp(
    date_value: str,
    time_value: str,
    date_format: str,
    time_formats: Iterable[str],
) -> datetime:
    if date_format == "%Y/%m/%d":
        try:
            return _parse_sierra_timestamp_fast(date_value, time_value)
        except ValueError:
            pass

    timestamp_value = f"{date_value.strip()} {time_value.strip()}"

    for time_format in time_formats:
        try:
            return datetime.strptime(timestamp_value, f"{date_format} {time_format}")
        except ValueError:
            continue

    raise ValueError(f"Could not parse timestamp: {timestamp_value}")


def _parse_sierra_timestamp_fast(date_value: str, time_value: str) -> datetime:
    year_text, month_text, day_text = date_value.strip().split("/")
    clean_time = time_value.strip()
    time_text, dot, fraction_text = clean_time.partition(".")
    hour_text, minute_text, second_text = time_text.split(":")

    microsecond = 0
    if dot:
        microsecond = int(fraction_text[:6].ljust(6, "0"))

    return datetime(
        int(year_text),
        int(month_text),
        int(day_text),
        int(hour_text),
        int(minute_text),
        int(second_text),
        microsecond,
    )


def _require_columns(columns: Iterable[str], required_columns: Iterable[str]) -> None:
    available = set(columns)
    missing = [column for column in required_columns if column not in available]

    if missing:
        raise ValueError(f"Raw file is missing required columns: {missing}")


def _min_decimal(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return min(current, value)


def _max_decimal(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return max(current, value)


def _format_optional(value: object | None) -> str:
    if value is None:
        return "None"

    return str(value)


def _format_row_count(inspection: RawFileInspection) -> str:
    if inspection.is_partial:
        return f"Rows scanned: {inspection.row_count} (partial, max_rows={inspection.max_rows})"

    return f"Row count: {inspection.row_count}"
