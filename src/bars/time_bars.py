"""Build time-based volume/delta bars from cleaned tick rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable

from src.ingest.convert_ticks import CleanTickRow
from src.ingest.order_rows import sort_clean_rows


VWAP_QUANT = Decimal("0.000001")


@dataclass(frozen=True)
class TimeBar:
    """OHLCV bar with order-flow volume fields."""

    symbol: str
    contract: str
    timeframe: str
    timestamp_utc: datetime
    timestamp_ny: datetime
    session_date: object
    session_type: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    buying_volume: int
    selling_volume: int
    delta: int
    cumulative_delta: int
    number_of_trades: int
    vwap: Decimal | None

    def to_display_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "contract": self.contract,
            "timeframe": self.timeframe,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "timestamp_ny": self.timestamp_ny.isoformat(),
            "session_date": self.session_date.isoformat(),
            "session_type": self.session_type,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": self.volume,
            "buying_volume": self.buying_volume,
            "selling_volume": self.selling_volume,
            "delta": self.delta,
            "cumulative_delta": self.cumulative_delta,
            "number_of_trades": self.number_of_trades,
            "vwap": str(self.vwap) if self.vwap is not None else None,
        }


@dataclass
class _BarAccumulator:
    symbol: str
    contract: str
    timeframe: str
    timestamp_ny: datetime
    session_date: object
    session_type: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    buying_volume: int
    selling_volume: int
    delta: int
    number_of_trades: int
    notional_volume: Decimal

    @classmethod
    def from_row(cls, row: CleanTickRow, timeframe: str, timestamp_ny: datetime) -> "_BarAccumulator":
        return cls(
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
            volume=row.volume,
            buying_volume=row.ask_volume,
            selling_volume=row.bid_volume,
            delta=row.delta,
            number_of_trades=row.number_of_trades,
            notional_volume=row.last * row.volume,
        )

    def update(self, row: CleanTickRow) -> None:
        self.high = max(self.high, row.high)
        self.low = min(self.low, row.low)
        self.close = row.last
        self.volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades
        self.notional_volume += row.last * row.volume

    def to_bar(self, cumulative_delta: int) -> TimeBar:
        vwap = None
        if self.volume:
            vwap = (self.notional_volume / self.volume).quantize(VWAP_QUANT)

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


def build_time_bars(rows: Iterable[CleanTickRow], timeframe: str) -> list[TimeBar]:
    """Aggregate cleaned tick rows into time bars."""
    timeframe_minutes = parse_timeframe_minutes(timeframe)
    accumulators: dict[tuple[object, str, datetime], _BarAccumulator] = {}

    for row in sort_clean_rows(rows):
        if row.session_date is None:
            continue

        bar_timestamp = floor_timestamp(row.timestamp_ny, timeframe_minutes)
        key = (row.session_date, row.session_type, bar_timestamp)

        if key not in accumulators:
            accumulators[key] = _BarAccumulator.from_row(row, timeframe, bar_timestamp)
        else:
            accumulators[key].update(row)

    bars_without_cvd = [
        accumulators[key]
        for key in sorted(
            accumulators,
            key=lambda item: (item[0], item[2], item[1]),
        )
    ]

    cumulative_delta_by_session: dict[object, int] = {}
    bars: list[TimeBar] = []

    for accumulator in bars_without_cvd:
        cumulative_delta = cumulative_delta_by_session.get(accumulator.session_date, 0)
        cumulative_delta += accumulator.delta
        cumulative_delta_by_session[accumulator.session_date] = cumulative_delta
        bars.append(accumulator.to_bar(cumulative_delta=cumulative_delta))

    return bars


def parse_timeframe_minutes(timeframe: str) -> int:
    """Parse minute/hour timeframe strings such as 1m, 15m, 1h, or 4h."""
    normalized = timeframe.strip().lower()

    if normalized.endswith("m"):
        minutes = int(normalized[:-1])
    elif normalized.endswith("h"):
        minutes = int(normalized[:-1]) * 60
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    if minutes <= 0:
        raise ValueError(f"Timeframe must be positive: {timeframe}")

    return minutes


def floor_timestamp(timestamp: datetime, timeframe_minutes: int) -> datetime:
    """Floor a timestamp to a wall-clock aligned timeframe."""
    midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_minutes = int((timestamp - midnight).total_seconds() // 60)
    floored_minutes = elapsed_minutes - (elapsed_minutes % timeframe_minutes)
    return midnight + timedelta(minutes=floored_minutes)
