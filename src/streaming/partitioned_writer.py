"""Stateful streaming writer for partitioned derived Parquet datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

from src.bars.time_bars import (
    TimeBar,
    floor_timestamp,
    merged_bar_session_type,
    parse_timeframe_minutes,
)
from src.ingest.convert_ticks import CleanTickRow, iter_clean_tick_rows
from src.ingest.order_rows import sort_clean_rows
from src.ingest.write_parquet import (
    BAR_SCHEMA,
    FOOTPRINT_CLUSTER_SCHEMA,
    SESSION_SUMMARY_SCHEMA,
    VOLUME_PROFILE_SCHEMA,
    ParquetWriteResult,
    _bar_to_parquet_record,
    _footprint_to_parquet_record,
    _summary_to_parquet_record,
    _volume_profile_to_parquet_record,
    _write_records,
)
from src.profiles.footprint import FootprintClusterRow
from src.profiles.volume_profile import VolumeProfileRow
from src.sessions.session_summary import SessionSummary


@dataclass(frozen=True)
class PartitionedWriteResult:
    """Summary of one written full-dataset partition."""

    dataset_type: str
    path: Path
    rows: int
    file_size_bytes: int
    session_date: date
    timeframe: str | None = None


PartitionBatchCallback = Callable[[list[PartitionedWriteResult]], None]


@dataclass
class _OrderAwareState:
    first_order: tuple[object, int]
    last_order: tuple[object, int]

    def update_order(self, row: CleanTickRow) -> tuple[bool, bool]:
        order = (row.timestamp_ny, row.sequence_id)
        is_earlier = order < self.first_order
        is_later = order > self.last_order

        if is_earlier:
            self.first_order = order

        if is_later:
            self.last_order = order

        return is_earlier, is_later


@dataclass
class _BarState(_OrderAwareState):
    symbol: str
    contract: str
    timeframe: str
    timestamp_ny: object
    session_date: date
    session_type: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = 0
    buying_volume: int = 0
    selling_volume: int = 0
    delta: int = 0
    number_of_trades: int = 0
    notional_volume: Decimal = Decimal("0")

    @classmethod
    def from_row(
        cls,
        row: CleanTickRow,
        timeframe: str,
        timestamp_ny: object,
    ) -> "_BarState":
        order = (row.timestamp_ny, row.sequence_id)
        state = cls(
            first_order=order,
            last_order=order,
            symbol=row.symbol,
            contract=row.contract,
            timeframe=timeframe,
            timestamp_ny=timestamp_ny,
            session_date=row.session_date,
            session_type=row.session_type,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.last,
        )
        state.update(row)
        return state

    def update(self, row: CleanTickRow) -> None:
        is_earlier, is_later = self.update_order(row)

        self.session_type = merged_bar_session_type(self.session_type, row.session_type)

        if is_earlier:
            self.open = row.open

        if is_later:
            self.close = row.last

        self.high = max(self.high, row.high)
        self.low = min(self.low, row.low)
        self.volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades
        self.notional_volume += row.last * row.volume

    def to_bar(self, cumulative_delta: int) -> TimeBar:
        vwap = None
        if self.volume:
            vwap = (self.notional_volume / self.volume).quantize(Decimal("0.000001"))

        return TimeBar(
            symbol=self.symbol,
            contract=self.contract,
            timeframe=self.timeframe,
            timestamp_utc=self.timestamp_ny.astimezone(timezone.utc),
            timestamp_ny=self.timestamp_ny,
            session_date=self.session_date,
            session_type=self.session_type,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            buying_volume=self.buying_volume,
            selling_volume=self.selling_volume,
            delta=self.delta,
            cumulative_delta=cumulative_delta,
            number_of_trades=self.number_of_trades,
            vwap=vwap,
        )


@dataclass
class _FootprintState:
    symbol: str
    contract: str
    timeframe: str
    timestamp_ny: object
    session_date: date
    session_type: str
    price_ticks: int
    price: Decimal
    volume: int = 0
    buying_volume: int = 0
    selling_volume: int = 0
    delta: int = 0
    number_of_trades: int = 0

    @classmethod
    def from_row(
        cls,
        row: CleanTickRow,
        timeframe: str,
        timestamp_ny: object,
        tick_size: Decimal,
    ) -> "_FootprintState":
        state = cls(
            symbol=row.symbol,
            contract=row.contract,
            timeframe=timeframe,
            timestamp_ny=timestamp_ny,
            session_date=row.session_date,
            session_type=row.session_type,
            price_ticks=row.price_ticks,
            price=Decimal(row.price_ticks) * tick_size,
        )
        state.update(row)
        return state

    def update(self, row: CleanTickRow) -> None:
        self.session_type = merged_bar_session_type(self.session_type, row.session_type)
        self.volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def to_row(self) -> FootprintClusterRow:
        return FootprintClusterRow(
            symbol=self.symbol,
            contract=self.contract,
            timeframe=self.timeframe,
            timestamp_utc=self.timestamp_ny.astimezone(timezone.utc),
            timestamp_ny=self.timestamp_ny,
            session_date=self.session_date,
            session_type=self.session_type,
            price_ticks=self.price_ticks,
            price=self.price,
            volume=self.volume,
            buying_volume=self.buying_volume,
            selling_volume=self.selling_volume,
            delta=self.delta,
            number_of_trades=self.number_of_trades,
        )


@dataclass
class _ProfileState:
    symbol: str
    contract: str
    session_date: date
    session_type: str
    price_ticks: int
    price: Decimal
    volume: int = 0
    buying_volume: int = 0
    selling_volume: int = 0
    delta: int = 0
    number_of_trades: int = 0

    @classmethod
    def from_row(
        cls,
        row: CleanTickRow,
        session_type: str,
        tick_size: Decimal,
    ) -> "_ProfileState":
        state = cls(
            symbol=row.symbol,
            contract=row.contract,
            session_date=row.session_date,
            session_type=session_type,
            price_ticks=row.price_ticks,
            price=Decimal(row.price_ticks) * tick_size,
        )
        state.update(row)
        return state

    def update(self, row: CleanTickRow) -> None:
        self.volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def to_row(self) -> VolumeProfileRow:
        return VolumeProfileRow(
            symbol=self.symbol,
            contract=self.contract,
            session_date=self.session_date,
            session_type=self.session_type,
            price_ticks=self.price_ticks,
            price=self.price,
            volume=self.volume,
            buying_volume=self.buying_volume,
            selling_volume=self.selling_volume,
            delta=self.delta,
            number_of_trades=self.number_of_trades,
        )


@dataclass
class _SessionState(_OrderAwareState):
    symbol: str
    contract: str
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    total_volume: int = 0
    buying_volume: int = 0
    selling_volume: int = 0
    delta: int = 0
    number_of_trades: int = 0
    globex_high: Decimal | None = None
    globex_low: Decimal | None = None
    globex_volume: int = 0
    globex_delta: int = 0
    rth_first_order: tuple[object, int] | None = None
    rth_last_order: tuple[object, int] | None = None
    rth_open: Decimal | None = None
    rth_high: Decimal | None = None
    rth_low: Decimal | None = None
    rth_close: Decimal | None = None
    rth_volume: int = 0
    rth_delta: int = 0

    @classmethod
    def from_row(cls, row: CleanTickRow) -> "_SessionState":
        order = (row.timestamp_ny, row.sequence_id)
        state = cls(
            first_order=order,
            last_order=order,
            symbol=row.symbol,
            contract=row.contract,
            session_date=row.session_date,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.last,
        )
        state.update(row)
        return state

    def update(self, row: CleanTickRow) -> None:
        is_earlier, is_later = self.update_order(row)

        if is_earlier:
            self.open = row.open

        if is_later:
            self.close = row.last

        self.high = max(self.high, row.high)
        self.low = min(self.low, row.low)
        self.total_volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

        if row.session_type == "globex":
            self.globex_high = _max_optional(self.globex_high, row.high)
            self.globex_low = _min_optional(self.globex_low, row.low)
            self.globex_volume += row.volume
            self.globex_delta += row.delta

        if row.session_type == "rth":
            order = (row.timestamp_ny, row.sequence_id)

            if self.rth_first_order is None or order < self.rth_first_order:
                self.rth_first_order = order
                self.rth_open = row.open

            if self.rth_last_order is None or order > self.rth_last_order:
                self.rth_last_order = order
                self.rth_close = row.last

            self.rth_high = _max_optional(self.rth_high, row.high)
            self.rth_low = _min_optional(self.rth_low, row.low)
            self.rth_volume += row.volume
            self.rth_delta += row.delta

    def to_summary(self) -> SessionSummary:
        rth_range_points = None
        if self.rth_high is not None and self.rth_low is not None:
            rth_range_points = self.rth_high - self.rth_low

        return SessionSummary(
            symbol=self.symbol,
            contract=self.contract,
            session_date=self.session_date,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            total_volume=self.total_volume,
            buying_volume=self.buying_volume,
            selling_volume=self.selling_volume,
            delta=self.delta,
            cumulative_delta=self.delta,
            number_of_trades=self.number_of_trades,
            globex_high=self.globex_high,
            globex_low=self.globex_low,
            globex_volume=self.globex_volume,
            globex_delta=self.globex_delta,
            rth_open=self.rth_open,
            rth_high=self.rth_high,
            rth_low=self.rth_low,
            rth_close=self.rth_close,
            rth_volume=self.rth_volume,
            rth_delta=self.rth_delta,
            range_points=self.high - self.low,
            rth_range_points=rth_range_points,
        )


class _StreamingDerivedState:
    def __init__(self, timeframes: list[str], tick_size: Decimal) -> None:
        self.timeframes = timeframes
        self.timeframe_minutes = {
            timeframe: parse_timeframe_minutes(timeframe)
            for timeframe in timeframes
        }
        self.tick_size = tick_size
        self.bars: dict[tuple[date, str, object], _BarState] = {}
        self.footprints: dict[tuple[date, str, object, int], _FootprintState] = {}
        self.profiles: dict[tuple[date, str, int], _ProfileState] = {}
        self.sessions: dict[date, _SessionState] = {}

    def add_rows(self, rows: Iterable[CleanTickRow]) -> None:
        for row in rows:
            self.add_row(row)

    def add_row(self, row: CleanTickRow) -> None:
        if row.session_date is None:
            return

        self._add_session(row)
        self._add_profiles(row)

        for timeframe in self.timeframes:
            timestamp_ny = floor_timestamp(row.timestamp_ny, self.timeframe_minutes[timeframe])
            self._add_bar(row, timeframe, timestamp_ny)
            self._add_footprint(row, timeframe, timestamp_ny)

    def flush_sessions_before(
        self,
        output_root: Path,
        compression: str,
        before_session_date: date,
    ) -> list[PartitionedWriteResult]:
        session_dates = [
            session_date
            for session_date in self.sessions
            if session_date < before_session_date
        ]
        return self.flush_sessions(output_root, compression, sorted(session_dates))

    def flush_all(
        self,
        output_root: Path,
        compression: str,
    ) -> list[PartitionedWriteResult]:
        return self.flush_sessions(output_root, compression, sorted(self.sessions))

    def flush_sessions(
        self,
        output_root: Path,
        compression: str,
        session_dates: Iterable[date],
    ) -> list[PartitionedWriteResult]:
        results: list[PartitionedWriteResult] = []

        for session_date in session_dates:
            if session_date not in self.sessions:
                continue

            results.extend(self._write_session(output_root, compression, session_date))
            self._drop_session(session_date)

        return results

    def _add_session(self, row: CleanTickRow) -> None:
        if row.session_date not in self.sessions:
            self.sessions[row.session_date] = _SessionState.from_row(row)
        else:
            self.sessions[row.session_date].update(row)

    def _add_profiles(self, row: CleanTickRow) -> None:
        for session_type in [row.session_type, "full"]:
            key = (row.session_date, session_type, row.price_ticks)

            if key not in self.profiles:
                self.profiles[key] = _ProfileState.from_row(
                    row=row,
                    session_type=session_type,
                    tick_size=self.tick_size,
                )
            else:
                self.profiles[key].update(row)

    def _add_bar(self, row: CleanTickRow, timeframe: str, timestamp_ny: object) -> None:
        key = (row.session_date, timeframe, timestamp_ny)

        if key not in self.bars:
            self.bars[key] = _BarState.from_row(
                row=row,
                timeframe=timeframe,
                timestamp_ny=timestamp_ny,
            )
        else:
            self.bars[key].update(row)

    def _add_footprint(self, row: CleanTickRow, timeframe: str, timestamp_ny: object) -> None:
        key = (
            row.session_date,
            timeframe,
            timestamp_ny,
            row.price_ticks,
        )

        if key not in self.footprints:
            self.footprints[key] = _FootprintState.from_row(
                row=row,
                timeframe=timeframe,
                timestamp_ny=timestamp_ny,
                tick_size=self.tick_size,
            )
        else:
            self.footprints[key].update(row)

    def _write_session(
        self,
        output_root: Path,
        compression: str,
        session_date: date,
    ) -> list[PartitionedWriteResult]:
        results: list[PartitionedWriteResult] = []

        summary = self.sessions[session_date].to_summary()
        results.append(
            _write_partition(
                output_root=output_root,
                dataset_type="session_summaries",
                session_date=session_date,
                records=[_summary_to_parquet_record(summary)],
                schema=SESSION_SUMMARY_SCHEMA,
                compression=compression,
            )
        )

        profile_rows = [
            self.profiles[key].to_row()
            for key in sorted(self.profiles, key=lambda item: (item[1], item[2]))
            if key[0] == session_date
        ]
        results.append(
            _write_partition(
                output_root=output_root,
                dataset_type="volume_profiles",
                session_date=session_date,
                records=[_volume_profile_to_parquet_record(row) for row in profile_rows],
                schema=VOLUME_PROFILE_SCHEMA,
                compression=compression,
            )
        )

        for timeframe in self.timeframes:
            bars = self._bars_for_session_timeframe(session_date, timeframe)
            results.append(
                _write_partition(
                    output_root=output_root,
                    dataset_type="bars",
                    session_date=session_date,
                    records=[_bar_to_parquet_record(row) for row in bars],
                    schema=BAR_SCHEMA,
                    compression=compression,
                    timeframe=timeframe,
                )
            )

            footprint_rows = [
                self.footprints[key].to_row()
                for key in sorted(
                    self.footprints,
                    key=lambda item: (item[2], item[3]),
                )
                if key[0] == session_date and key[1] == timeframe
            ]
            results.append(
                _write_partition(
                    output_root=output_root,
                    dataset_type="footprint_clusters",
                    session_date=session_date,
                    records=[
                        _footprint_to_parquet_record(row)
                        for row in footprint_rows
                    ],
                    schema=FOOTPRINT_CLUSTER_SCHEMA,
                    compression=compression,
                    timeframe=timeframe,
                )
            )

        return results

    def _bars_for_session_timeframe(
        self,
        session_date: date,
        timeframe: str,
    ) -> list[TimeBar]:
        states = [
            self.bars[key]
            for key in sorted(self.bars, key=lambda item: item[2])
            if key[0] == session_date and key[1] == timeframe
        ]
        cumulative_delta = 0
        rows: list[TimeBar] = []

        for state in states:
            cumulative_delta += state.delta
            rows.append(state.to_bar(cumulative_delta=cumulative_delta))

        return rows

    def _drop_session(self, session_date: date) -> None:
        self.sessions.pop(session_date, None)
        self.profiles = {
            key: value
            for key, value in self.profiles.items()
            if key[0] != session_date
        }
        self.bars = {
            key: value
            for key, value in self.bars.items()
            if key[0] != session_date
        }
        self.footprints = {
            key: value
            for key, value in self.footprints.items()
            if key[0] != session_date
        }


def write_partitioned_derived_parquets(
    input_path: str | Path,
    output_root: str | Path,
    config: Mapping[str, Any],
    chunk_size_rows: int | None = None,
    max_rows: int | None = None,
    timeframes: list[str] | None = None,
    flush_lag_sessions: int = 1,
    partition_batch_callback: PartitionBatchCallback | None = None,
) -> list[PartitionedWriteResult]:
    """Stream a raw file and write partitioned derived Parquet datasets."""
    rows = iter_clean_tick_rows(input_path, config, max_rows=max_rows)
    return write_partitioned_derived_parquets_from_rows(
        rows=rows,
        output_root=output_root,
        config=config,
        chunk_size_rows=chunk_size_rows,
        timeframes=timeframes,
        flush_lag_sessions=flush_lag_sessions,
        partition_batch_callback=partition_batch_callback,
    )


def write_partitioned_derived_parquets_from_rows(
    rows: Iterable[CleanTickRow],
    output_root: str | Path,
    config: Mapping[str, Any],
    chunk_size_rows: int | None = None,
    timeframes: list[str] | None = None,
    flush_lag_sessions: int = 1,
    partition_batch_callback: PartitionBatchCallback | None = None,
) -> list[PartitionedWriteResult]:
    """Write partitioned derived Parquet datasets from cleaned rows."""
    selected_timeframes = timeframes or list(config["derived_datasets"]["timeframes"])
    selected_chunk_size = chunk_size_rows or int(config["processing"]["chunk_size_rows"])

    if selected_chunk_size <= 0:
        raise ValueError("chunk_size_rows must be greater than zero")

    output_dir = Path(output_root)
    compression = config["parquet"]["compression"]
    state = _StreamingDerivedState(
        timeframes=selected_timeframes,
        tick_size=Decimal(str(config["market"]["tick_size"])),
    )
    results: list[PartitionedWriteResult] = []

    def collect_batch(batch: list[PartitionedWriteResult]) -> None:
        if not batch:
            return

        results.extend(batch)

        if partition_batch_callback is not None:
            partition_batch_callback(batch)

    for chunk in _iter_chunks(rows, selected_chunk_size):
        sorted_chunk = sort_clean_rows(chunk)
        state.add_rows(sorted_chunk)

        min_session_date = _min_session_date(sorted_chunk)
        if min_session_date is not None:
            flush_before = min_session_date - timedelta(days=flush_lag_sessions)
            collect_batch(
                state.flush_sessions_before(
                    output_root=output_dir,
                    compression=compression,
                    before_session_date=flush_before,
                )
            )

    collect_batch(state.flush_all(output_dir, compression))
    return results


def _write_partition(
    output_root: Path,
    dataset_type: str,
    session_date: date,
    records: list[dict[str, object]],
    schema: object,
    compression: str,
    timeframe: str | None = None,
) -> PartitionedWriteResult:
    output_file = _partition_file(output_root, dataset_type, session_date, timeframe)
    result = _write_records(
        records=records,
        schema=schema,
        output_file=output_file,
        compression=compression,
    )
    return _to_partitioned_result(
        dataset_type=dataset_type,
        session_date=session_date,
        timeframe=timeframe,
        result=result,
    )


def _partition_file(
    output_root: Path,
    dataset_type: str,
    session_date: date,
    timeframe: str | None = None,
) -> Path:
    parts = [output_root, Path(dataset_type)]

    if timeframe is not None:
        parts.append(Path(f"timeframe={timeframe}"))

    parts.extend(
        [
            Path(f"year={session_date.year:04d}"),
            Path(f"month={session_date.month:02d}"),
            Path(f"session={session_date.isoformat()}"),
            Path("part.parquet"),
        ]
    )

    output_file = parts[0]
    for part in parts[1:]:
        output_file = output_file / part

    return output_file


def _to_partitioned_result(
    dataset_type: str,
    session_date: date,
    timeframe: str | None,
    result: ParquetWriteResult,
) -> PartitionedWriteResult:
    return PartitionedWriteResult(
        dataset_type=dataset_type,
        path=result.path,
        rows=result.rows,
        file_size_bytes=result.file_size_bytes,
        session_date=session_date,
        timeframe=timeframe,
    )


def _iter_chunks(
    rows: Iterable[CleanTickRow],
    chunk_size_rows: int,
) -> Iterator[list[CleanTickRow]]:
    chunk = []

    for row in rows:
        chunk.append(row)

        if len(chunk) >= chunk_size_rows:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def _min_session_date(rows: Iterable[CleanTickRow]) -> date | None:
    session_dates = [
        row.session_date
        for row in rows
        if row.session_date is not None
    ]

    if not session_dates:
        return None

    return min(session_dates)


def _max_optional(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return max(current, value)


def _min_optional(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return min(current, value)
