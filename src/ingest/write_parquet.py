"""Write local Parquet previews for cleaned and derived datasets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from src.bars import TimeBar, build_time_bars
from src.ingest.convert_ticks import CleanTickRow, iter_clean_tick_rows
from src.profiles import (
    FootprintClusterRow,
    VolumeProfileRow,
    build_footprint_clusters,
    build_volume_profiles,
)
from src.sessions.session_summary import SessionSummary, build_session_summaries


@dataclass(frozen=True)
class ParquetWriteResult:
    """Summary of a local Parquet write."""

    path: Path
    rows: int
    file_size_bytes: int


TICK_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("sequence_id", pa.int64()),
        ("timestamp_utc", pa.timestamp("us", tz="UTC")),
        ("timestamp_ny", pa.timestamp("us", tz="America/New_York")),
        ("session_date", pa.date32()),
        ("session_type", pa.string()),
        ("open", pa.decimal128(12, 2)),
        ("high", pa.decimal128(12, 2)),
        ("low", pa.decimal128(12, 2)),
        ("last", pa.decimal128(12, 2)),
        ("volume", pa.int64()),
        ("number_of_trades", pa.int64()),
        ("bid_volume", pa.int64()),
        ("ask_volume", pa.int64()),
        ("delta", pa.int64()),
        ("price_ticks", pa.int32()),
    ]
)

BAR_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("timestamp_utc", pa.timestamp("us", tz="UTC")),
        ("timestamp_ny", pa.timestamp("us", tz="America/New_York")),
        ("session_date", pa.date32()),
        ("session_type", pa.string()),
        ("open", pa.decimal128(12, 2)),
        ("high", pa.decimal128(12, 2)),
        ("low", pa.decimal128(12, 2)),
        ("close", pa.decimal128(12, 2)),
        ("volume", pa.int64()),
        ("buying_volume", pa.int64()),
        ("selling_volume", pa.int64()),
        ("delta", pa.int64()),
        ("cumulative_delta", pa.int64()),
        ("number_of_trades", pa.int64()),
        ("vwap", pa.decimal128(18, 6)),
    ]
)

SESSION_SUMMARY_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("session_date", pa.date32()),
        ("open", pa.decimal128(12, 2)),
        ("high", pa.decimal128(12, 2)),
        ("low", pa.decimal128(12, 2)),
        ("close", pa.decimal128(12, 2)),
        ("total_volume", pa.int64()),
        ("buying_volume", pa.int64()),
        ("selling_volume", pa.int64()),
        ("delta", pa.int64()),
        ("cumulative_delta", pa.int64()),
        ("number_of_trades", pa.int64()),
        ("globex_high", pa.decimal128(12, 2)),
        ("globex_low", pa.decimal128(12, 2)),
        ("globex_volume", pa.int64()),
        ("globex_delta", pa.int64()),
        ("rth_open", pa.decimal128(12, 2)),
        ("rth_high", pa.decimal128(12, 2)),
        ("rth_low", pa.decimal128(12, 2)),
        ("rth_close", pa.decimal128(12, 2)),
        ("rth_volume", pa.int64()),
        ("rth_delta", pa.int64()),
        ("range_points", pa.decimal128(12, 2)),
        ("rth_range_points", pa.decimal128(12, 2)),
    ]
)

FOOTPRINT_CLUSTER_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("timestamp_utc", pa.timestamp("us", tz="UTC")),
        ("timestamp_ny", pa.timestamp("us", tz="America/New_York")),
        ("session_date", pa.date32()),
        ("session_type", pa.string()),
        ("price_ticks", pa.int32()),
        ("price", pa.decimal128(12, 2)),
        ("volume", pa.int64()),
        ("buying_volume", pa.int64()),
        ("selling_volume", pa.int64()),
        ("delta", pa.int64()),
        ("number_of_trades", pa.int64()),
    ]
)

VOLUME_PROFILE_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("session_date", pa.date32()),
        ("session_type", pa.string()),
        ("price_ticks", pa.int32()),
        ("price", pa.decimal128(12, 2)),
        ("volume", pa.int64()),
        ("buying_volume", pa.int64()),
        ("selling_volume", pa.int64()),
        ("delta", pa.int64()),
        ("number_of_trades", pa.int64()),
    ]
)


def write_clean_tick_sample_parquet(
    input_path: str | Path,
    output_path: str | Path,
    config: Mapping[str, Any],
    max_rows: int,
) -> ParquetWriteResult:
    """Write a capped local Parquet preview of cleaned tick rows."""
    if max_rows <= 0:
        raise ValueError("max_rows must be greater than zero for a sample write")

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    records = [
        _row_to_parquet_record(row)
        for row in iter_clean_tick_rows(input_path, config, max_rows=max_rows)
    ]
    table = pa.Table.from_pylist(records, schema=TICK_SCHEMA)

    pq.write_table(
        table,
        output_file,
        compression=config["parquet"]["compression"],
    )

    return ParquetWriteResult(
        path=output_file,
        rows=table.num_rows,
        file_size_bytes=output_file.stat().st_size,
    )


def write_derived_sample_parquets(
    input_path: str | Path,
    output_root: str | Path,
    config: Mapping[str, Any],
    max_rows: int,
    timeframes: list[str] | None = None,
) -> list[ParquetWriteResult]:
    """Write capped local Parquet samples for all current derived datasets."""
    if max_rows <= 0:
        raise ValueError("max_rows must be greater than zero for a sample write")

    output_dir = Path(output_root)
    configured_timeframes = config["derived_datasets"]["timeframes"]
    selected_timeframes = timeframes or list(configured_timeframes)
    compression = config["parquet"]["compression"]
    tick_size = Decimal(str(config["market"]["tick_size"]))

    clean_rows = list(iter_clean_tick_rows(input_path, config, max_rows=max_rows))
    results: list[ParquetWriteResult] = []

    summaries = build_session_summaries(clean_rows)
    results.append(
        _write_records(
            records=[_summary_to_parquet_record(row) for row in summaries],
            schema=SESSION_SUMMARY_SCHEMA,
            output_file=output_dir / "session_summaries" / "part.parquet",
            compression=compression,
        )
    )

    profiles = build_volume_profiles(clean_rows, tick_size=tick_size)
    results.append(
        _write_records(
            records=[_volume_profile_to_parquet_record(row) for row in profiles],
            schema=VOLUME_PROFILE_SCHEMA,
            output_file=output_dir / "volume_profiles" / "part.parquet",
            compression=compression,
        )
    )

    for timeframe in selected_timeframes:
        bars = build_time_bars(clean_rows, timeframe=timeframe)
        results.append(
            _write_records(
                records=[_bar_to_parquet_record(row) for row in bars],
                schema=BAR_SCHEMA,
                output_file=output_dir
                / "bars"
                / f"timeframe={timeframe}"
                / "part.parquet",
                compression=compression,
            )
        )

        clusters = build_footprint_clusters(
            clean_rows,
            timeframe=timeframe,
            tick_size=tick_size,
        )
        results.append(
            _write_records(
                records=[_footprint_to_parquet_record(row) for row in clusters],
                schema=FOOTPRINT_CLUSTER_SCHEMA,
                output_file=output_dir
                / "footprint_clusters"
                / f"timeframe={timeframe}"
                / "part.parquet",
                compression=compression,
            )
        )

    return results


def _row_to_parquet_record(row: CleanTickRow) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "contract": row.contract,
        "sequence_id": row.sequence_id,
        "timestamp_utc": row.timestamp_utc,
        "timestamp_ny": row.timestamp_ny,
        "session_date": row.session_date,
        "session_type": row.session_type,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "last": row.last,
        "volume": row.volume,
        "number_of_trades": row.number_of_trades,
        "bid_volume": row.bid_volume,
        "ask_volume": row.ask_volume,
        "delta": row.delta,
        "price_ticks": row.price_ticks,
    }


def _bar_to_parquet_record(row: TimeBar) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "contract": row.contract,
        "timestamp_utc": row.timestamp_utc,
        "timestamp_ny": row.timestamp_ny,
        "session_date": row.session_date,
        "session_type": row.session_type,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "close": row.close,
        "volume": row.volume,
        "buying_volume": row.buying_volume,
        "selling_volume": row.selling_volume,
        "delta": row.delta,
        "cumulative_delta": row.cumulative_delta,
        "number_of_trades": row.number_of_trades,
        "vwap": row.vwap,
    }


def _summary_to_parquet_record(row: SessionSummary) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "contract": row.contract,
        "session_date": row.session_date,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "close": row.close,
        "total_volume": row.total_volume,
        "buying_volume": row.buying_volume,
        "selling_volume": row.selling_volume,
        "delta": row.delta,
        "cumulative_delta": row.cumulative_delta,
        "number_of_trades": row.number_of_trades,
        "globex_high": row.globex_high,
        "globex_low": row.globex_low,
        "globex_volume": row.globex_volume,
        "globex_delta": row.globex_delta,
        "rth_open": row.rth_open,
        "rth_high": row.rth_high,
        "rth_low": row.rth_low,
        "rth_close": row.rth_close,
        "rth_volume": row.rth_volume,
        "rth_delta": row.rth_delta,
        "range_points": row.range_points,
        "rth_range_points": row.rth_range_points,
    }


def _footprint_to_parquet_record(row: FootprintClusterRow) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "contract": row.contract,
        "timestamp_utc": row.timestamp_utc,
        "timestamp_ny": row.timestamp_ny,
        "session_date": row.session_date,
        "session_type": row.session_type,
        "price_ticks": row.price_ticks,
        "price": row.price,
        "volume": row.volume,
        "buying_volume": row.buying_volume,
        "selling_volume": row.selling_volume,
        "delta": row.delta,
        "number_of_trades": row.number_of_trades,
    }


def _volume_profile_to_parquet_record(row: VolumeProfileRow) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "contract": row.contract,
        "session_date": row.session_date,
        "session_type": row.session_type,
        "price_ticks": row.price_ticks,
        "price": row.price,
        "volume": row.volume,
        "buying_volume": row.buying_volume,
        "selling_volume": row.selling_volume,
        "delta": row.delta,
        "number_of_trades": row.number_of_trades,
    }


def _write_records(
    records: list[dict[str, object]],
    schema: pa.Schema,
    output_file: Path,
    compression: str,
) -> ParquetWriteResult:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, output_file, compression=compression)

    return ParquetWriteResult(
        path=output_file,
        rows=table.num_rows,
        file_size_bytes=output_file.stat().st_size,
    )
