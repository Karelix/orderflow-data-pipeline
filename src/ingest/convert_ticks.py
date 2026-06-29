"""Convert Sierra Chart rows into cleaned tick records."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.ingest.inspect_raw import parse_sierra_timestamp
from src.sessions.session_calendar import CLOSED_SESSION_TYPE, SessionConfig, classify_timestamp


@dataclass(frozen=True)
class CleanTickRow:
    """Cleaned tick row ready for downstream datasets."""

    symbol: str
    contract: str
    sequence_id: int
    timestamp_utc: datetime
    timestamp_ny: datetime
    session_date: date | None
    session_type: str
    open: Decimal
    high: Decimal
    low: Decimal
    last: Decimal
    volume: int
    number_of_trades: int
    bid_volume: int
    ask_volume: int
    delta: int
    price_ticks: int

    def to_display_dict(self) -> dict[str, object]:
        """Return JSON/CSV friendly values for previews and logs."""
        return {
            "symbol": self.symbol,
            "contract": self.contract,
            "sequence_id": self.sequence_id,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "timestamp_ny": self.timestamp_ny.isoformat(),
            "session_date": self.session_date.isoformat()
            if self.session_date is not None
            else None,
            "session_type": self.session_type,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "last": str(self.last),
            "volume": self.volume,
            "number_of_trades": self.number_of_trades,
            "bid_volume": self.bid_volume,
            "ask_volume": self.ask_volume,
            "delta": self.delta,
            "price_ticks": self.price_ticks,
        }


def iter_clean_tick_rows(
    path: str | Path,
    config: Mapping[str, Any],
    max_rows: int | None = None,
) -> Iterable[CleanTickRow]:
    """Yield cleaned tick rows from a raw Sierra CSV/text export."""
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    raw_config = config["raw_data"]
    datetime_config = raw_config["datetime"]
    columns_config = raw_config["columns"]
    processing_config = config["processing"]

    date_column = datetime_config["date_column"]
    time_column = datetime_config["time_column"]
    date_format = datetime_config["date_format"]
    time_formats = datetime_config.get(
        "time_formats",
        [datetime_config["time_format"]],
    )

    open_column = columns_config["open"]
    high_column = columns_config["high"]
    low_column = columns_config["low"]
    last_column = columns_config["last"]
    volume_column = columns_config["volume"]
    number_of_trades_column = columns_config["number_of_trades"]
    bid_volume_column = columns_config["bid_volume"]
    ask_volume_column = columns_config["ask_volume"]

    required_columns = [
        date_column,
        time_column,
        open_column,
        high_column,
        low_column,
        last_column,
        volume_column,
        number_of_trades_column,
        bid_volume_column,
        ask_volume_column,
    ]

    symbol = config["dataset"]["symbol"]
    contract = config["dataset"]["default_contract"]
    tick_size = Decimal(str(config["market"]["tick_size"]))
    session_config = SessionConfig.from_project_config(config)
    exclude_closed = bool(processing_config["exclude_maintenance_break"])

    with file_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, skipinitialspace=True)

        if reader.fieldnames is None:
            raise ValueError(f"Raw file is empty or missing a header: {file_path}")

        reader.fieldnames = [column.strip() for column in reader.fieldnames]
        _require_columns(reader.fieldnames, required_columns)

        for sequence_id, row in enumerate(reader):
            if max_rows is not None and sequence_id >= max_rows:
                break

            timestamp = parse_sierra_timestamp(
                row[date_column],
                row[time_column],
                date_format,
                time_formats,
            )
            session_info = classify_timestamp(timestamp, session_config)

            if exclude_closed and session_info.session_type == CLOSED_SESSION_TYPE:
                continue

            last = Decimal(row[last_column])
            bid_volume = int(row[bid_volume_column])
            ask_volume = int(row[ask_volume_column])

            yield CleanTickRow(
                symbol=symbol,
                contract=contract,
                sequence_id=sequence_id,
                timestamp_utc=session_info.timestamp_utc,
                timestamp_ny=session_info.timestamp_ny,
                session_date=session_info.session_date,
                session_type=session_info.session_type,
                open=Decimal(row[open_column]),
                high=Decimal(row[high_column]),
                low=Decimal(row[low_column]),
                last=last,
                volume=int(row[volume_column]),
                number_of_trades=int(row[number_of_trades_column]),
                bid_volume=bid_volume,
                ask_volume=ask_volume,
                delta=ask_volume - bid_volume,
                price_ticks=calculate_price_ticks(last, tick_size),
            )


def calculate_price_ticks(price: Decimal, tick_size: Decimal) -> int:
    """Convert a price into integer ticks."""
    return int((price / tick_size).to_integral_value(rounding=ROUND_HALF_UP))


def _require_columns(columns: Iterable[str], required_columns: Iterable[str]) -> None:
    available = set(columns)
    missing = [column for column in required_columns if column not in available]

    if missing:
        raise ValueError(f"Raw file is missing required columns: {missing}")
