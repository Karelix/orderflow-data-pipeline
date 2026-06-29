"""Streaming validation for raw Sierra Chart exports."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from src.ingest.convert_ticks import calculate_price_ticks
from src.ingest.inspect_raw import parse_sierra_timestamp
from src.sessions.session_calendar import CLOSED_SESSION_TYPE, SessionConfig, classify_timestamp


ProgressCallback = Callable[["RawStreamChunkSummary"], None]


@dataclass(frozen=True)
class RawStreamChunkSummary:
    """Summary for one streamed chunk."""

    chunk_index: int
    rows: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    total_volume: int
    total_bid_volume: int
    total_ask_volume: int
    total_delta: int


@dataclass(frozen=True)
class RawStreamValidationReport:
    """Full streaming validation report."""

    path: Path
    file_size_bytes: int
    chunk_size: int
    rows_scanned: int
    chunks: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    total_volume: int
    total_bid_volume: int
    total_ask_volume: int
    total_delta: int
    min_last_price: Decimal | None
    max_last_price: Decimal | None
    parse_error_count: int
    out_of_order_timestamp_count: int
    adjacent_duplicate_timestamp_count: int
    volume_bid_ask_mismatch_count: int
    price_tick_mismatch_count: int
    non_tick_like_row_count: int
    closed_row_count: int
    session_type_counts: dict[str, int]
    session_date_counts: dict[str, int]
    chunk_summaries: list[RawStreamChunkSummary] = field(default_factory=list)
    error_samples: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return (
            self.parse_error_count == 0
            and self.out_of_order_timestamp_count == 0
            and self.volume_bid_ask_mismatch_count == 0
            and self.price_tick_mismatch_count == 0
        )

    @property
    def tick_like_ratio(self) -> float:
        if self.rows_scanned == 0:
            return 0.0

        return (self.rows_scanned - self.non_tick_like_row_count) / self.rows_scanned

    def format(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            f"Raw Stream Validation: {status}",
            f"Path: {self.path}",
            f"File size bytes: {self.file_size_bytes}",
            f"Chunk size: {self.chunk_size}",
            f"Rows scanned: {self.rows_scanned}",
            f"Chunks: {self.chunks}",
            f"First timestamp: {self.first_timestamp}",
            f"Last timestamp: {self.last_timestamp}",
            f"Total volume: {self.total_volume}",
            f"Total bid volume: {self.total_bid_volume}",
            f"Total ask volume: {self.total_ask_volume}",
            f"Total delta: {self.total_delta}",
            f"Min Last price: {self.min_last_price}",
            f"Max Last price: {self.max_last_price}",
            f"Parse errors: {self.parse_error_count}",
            f"Out-of-order timestamps: {self.out_of_order_timestamp_count}",
            f"Adjacent duplicate timestamp rows: {self.adjacent_duplicate_timestamp_count}",
            f"Volume != BidVolume + AskVolume rows: {self.volume_bid_ask_mismatch_count}",
            f"Price tick mismatches: {self.price_tick_mismatch_count}",
            f"Non tick-like OHLC rows: {self.non_tick_like_row_count}",
            f"Tick-like OHLC ratio: {self.tick_like_ratio:.6f}",
            f"Closed/maintenance rows: {self.closed_row_count}",
            f"Session type counts: {dict(self.session_type_counts)}",
            f"Session date count: {len(self.session_date_counts)}",
        ]

        if self.error_samples:
            lines.append("Error samples:")
            lines.extend(f"- {sample}" for sample in self.error_samples)

        return "\n".join(lines)


@dataclass
class _RunningTotals:
    rows_scanned: int = 0
    chunks: int = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    previous_timestamp: datetime | None = None
    total_volume: int = 0
    total_bid_volume: int = 0
    total_ask_volume: int = 0
    total_delta: int = 0
    min_last_price: Decimal | None = None
    max_last_price: Decimal | None = None
    parse_error_count: int = 0
    out_of_order_timestamp_count: int = 0
    adjacent_duplicate_timestamp_count: int = 0
    volume_bid_ask_mismatch_count: int = 0
    price_tick_mismatch_count: int = 0
    non_tick_like_row_count: int = 0
    closed_row_count: int = 0
    session_type_counts: Counter[str] = field(default_factory=Counter)
    session_date_counts: Counter[str] = field(default_factory=Counter)
    chunk_summaries: list[RawStreamChunkSummary] = field(default_factory=list)
    error_samples: list[str] = field(default_factory=list)


def validate_raw_stream(
    path: str | Path,
    config: Mapping[str, Any],
    chunk_size: int = 100000,
    max_rows: int | None = None,
    progress_callback: ProgressCallback | None = None,
) -> RawStreamValidationReport:
    """Validate a raw Sierra export by streaming it in chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

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

    open_column = columns_config["open"]
    high_column = columns_config["high"]
    low_column = columns_config["low"]
    last_column = columns_config["last"]
    volume_column = columns_config["volume"]
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
        bid_volume_column,
        ask_volume_column,
    ]

    tick_size = Decimal(str(config["market"]["tick_size"]))
    session_config = SessionConfig.from_project_config(config)
    totals = _RunningTotals()
    chunk_state = _new_chunk_state()

    with file_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, skipinitialspace=True)
        if reader.fieldnames is None:
            raise ValueError(f"Raw file is empty or missing a header: {file_path}")

        reader.fieldnames = [column.strip() for column in reader.fieldnames]
        _require_columns(reader.fieldnames, required_columns)

        for raw_index, row in enumerate(reader):
            if max_rows is not None and raw_index >= max_rows:
                break

            try:
                timestamp = parse_sierra_timestamp(
                    row[date_column],
                    row[time_column],
                    date_format,
                    time_formats,
                )
                open_price = Decimal(row[open_column])
                high_price = Decimal(row[high_column])
                low_price = Decimal(row[low_column])
                last_price = Decimal(row[last_column])
                volume = int(row[volume_column])
                bid_volume = int(row[bid_volume_column])
                ask_volume = int(row[ask_volume_column])
            except Exception as exc:  # noqa: BLE001 - collect row samples and continue.
                totals.parse_error_count += 1
                _add_error_sample(totals, f"row={raw_index + 1}: {exc}")
                continue

            _validate_row(
                totals=totals,
                timestamp=timestamp,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                last_price=last_price,
                volume=volume,
                bid_volume=bid_volume,
                ask_volume=ask_volume,
                tick_size=tick_size,
                session_config=session_config,
                raw_index=raw_index,
            )
            _update_chunk(
                chunk_state=chunk_state,
                timestamp=timestamp,
                volume=volume,
                bid_volume=bid_volume,
                ask_volume=ask_volume,
            )

            if chunk_state["rows"] == chunk_size:
                totals.chunks += 1
                summary = _finish_chunk(totals.chunks, chunk_state)
                totals.chunk_summaries.append(summary)
                if progress_callback is not None:
                    progress_callback(summary)
                chunk_state = _new_chunk_state()

    if chunk_state["rows"]:
        totals.chunks += 1
        summary = _finish_chunk(totals.chunks, chunk_state)
        totals.chunk_summaries.append(summary)
        if progress_callback is not None:
            progress_callback(summary)

    return RawStreamValidationReport(
        path=file_path,
        file_size_bytes=file_path.stat().st_size,
        chunk_size=chunk_size,
        rows_scanned=totals.rows_scanned,
        chunks=totals.chunks,
        first_timestamp=totals.first_timestamp,
        last_timestamp=totals.last_timestamp,
        total_volume=totals.total_volume,
        total_bid_volume=totals.total_bid_volume,
        total_ask_volume=totals.total_ask_volume,
        total_delta=totals.total_delta,
        min_last_price=totals.min_last_price,
        max_last_price=totals.max_last_price,
        parse_error_count=totals.parse_error_count,
        out_of_order_timestamp_count=totals.out_of_order_timestamp_count,
        adjacent_duplicate_timestamp_count=totals.adjacent_duplicate_timestamp_count,
        volume_bid_ask_mismatch_count=totals.volume_bid_ask_mismatch_count,
        price_tick_mismatch_count=totals.price_tick_mismatch_count,
        non_tick_like_row_count=totals.non_tick_like_row_count,
        closed_row_count=totals.closed_row_count,
        session_type_counts=dict(totals.session_type_counts),
        session_date_counts=dict(totals.session_date_counts),
        chunk_summaries=totals.chunk_summaries,
        error_samples=totals.error_samples,
    )


def _validate_row(
    totals: _RunningTotals,
    timestamp: datetime,
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    last_price: Decimal,
    volume: int,
    bid_volume: int,
    ask_volume: int,
    tick_size: Decimal,
    session_config: SessionConfig,
    raw_index: int,
) -> None:
    totals.rows_scanned += 1
    totals.first_timestamp = totals.first_timestamp or timestamp
    totals.last_timestamp = timestamp

    if totals.previous_timestamp is not None:
        if timestamp < totals.previous_timestamp:
            totals.out_of_order_timestamp_count += 1
            _add_error_sample(
                totals,
                f"row={raw_index + 1}: timestamp {timestamp} before {totals.previous_timestamp}",
            )
        elif timestamp == totals.previous_timestamp:
            totals.adjacent_duplicate_timestamp_count += 1

    totals.previous_timestamp = timestamp

    totals.total_volume += volume
    totals.total_bid_volume += bid_volume
    totals.total_ask_volume += ask_volume
    totals.total_delta += ask_volume - bid_volume
    totals.min_last_price = _min_decimal(totals.min_last_price, last_price)
    totals.max_last_price = _max_decimal(totals.max_last_price, last_price)

    if volume != bid_volume + ask_volume:
        totals.volume_bid_ask_mismatch_count += 1
        _add_error_sample(
            totals,
            f"row={raw_index + 1}: volume {volume} != bid+ask {bid_volume + ask_volume}",
        )

    price_ticks = calculate_price_ticks(last_price, tick_size)
    if Decimal(price_ticks) * tick_size != last_price:
        totals.price_tick_mismatch_count += 1
        _add_error_sample(
            totals,
            f"row={raw_index + 1}: last {last_price} not aligned to tick_size {tick_size}",
        )

    if not (open_price == high_price == low_price == last_price):
        totals.non_tick_like_row_count += 1

    session_info = classify_timestamp(timestamp, session_config)
    totals.session_type_counts[session_info.session_type] += 1
    if session_info.session_type == CLOSED_SESSION_TYPE:
        totals.closed_row_count += 1

    if session_info.session_date is not None:
        totals.session_date_counts[session_info.session_date.isoformat()] += 1


def _new_chunk_state() -> dict[str, object]:
    return {
        "rows": 0,
        "first_timestamp": None,
        "last_timestamp": None,
        "total_volume": 0,
        "total_bid_volume": 0,
        "total_ask_volume": 0,
        "total_delta": 0,
    }


def _update_chunk(
    chunk_state: dict[str, object],
    timestamp: datetime,
    volume: int,
    bid_volume: int,
    ask_volume: int,
) -> None:
    if chunk_state["first_timestamp"] is None:
        chunk_state["first_timestamp"] = timestamp

    chunk_state["last_timestamp"] = timestamp
    chunk_state["rows"] = int(chunk_state["rows"]) + 1
    chunk_state["total_volume"] = int(chunk_state["total_volume"]) + volume
    chunk_state["total_bid_volume"] = int(chunk_state["total_bid_volume"]) + bid_volume
    chunk_state["total_ask_volume"] = int(chunk_state["total_ask_volume"]) + ask_volume
    chunk_state["total_delta"] = int(chunk_state["total_delta"]) + ask_volume - bid_volume


def _finish_chunk(
    chunk_index: int,
    chunk_state: dict[str, object],
) -> RawStreamChunkSummary:
    return RawStreamChunkSummary(
        chunk_index=chunk_index,
        rows=int(chunk_state["rows"]),
        first_timestamp=chunk_state["first_timestamp"],
        last_timestamp=chunk_state["last_timestamp"],
        total_volume=int(chunk_state["total_volume"]),
        total_bid_volume=int(chunk_state["total_bid_volume"]),
        total_ask_volume=int(chunk_state["total_ask_volume"]),
        total_delta=int(chunk_state["total_delta"]),
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


def _add_error_sample(totals: _RunningTotals, message: str) -> None:
    if len(totals.error_samples) < 20:
        totals.error_samples.append(message)
