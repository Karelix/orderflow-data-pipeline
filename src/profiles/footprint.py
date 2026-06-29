"""Build Quantower-style footprint cluster rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable

from src.bars.time_bars import floor_timestamp, parse_timeframe_minutes
from src.ingest.convert_ticks import CleanTickRow
from src.ingest.order_rows import sort_clean_rows


@dataclass(frozen=True)
class FootprintClusterRow:
    """One price level inside one time bar."""

    symbol: str
    contract: str
    timeframe: str
    timestamp_utc: datetime
    timestamp_ny: datetime
    session_date: object
    session_type: str
    price_ticks: int
    price: Decimal
    volume: int
    buying_volume: int
    selling_volume: int
    delta: int
    number_of_trades: int

    def to_display_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "contract": self.contract,
            "timeframe": self.timeframe,
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "timestamp_ny": self.timestamp_ny.isoformat(),
            "session_date": self.session_date.isoformat(),
            "session_type": self.session_type,
            "price_ticks": self.price_ticks,
            "price": str(self.price),
            "volume": self.volume,
            "buying_volume": self.buying_volume,
            "selling_volume": self.selling_volume,
            "delta": self.delta,
            "number_of_trades": self.number_of_trades,
        }


@dataclass
class _FootprintAccumulator:
    symbol: str
    contract: str
    timeframe: str
    timestamp_ny: datetime
    session_date: object
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
        timestamp_ny: datetime,
        tick_size: Decimal,
    ) -> "_FootprintAccumulator":
        accumulator = cls(
            symbol=row.symbol,
            contract=row.contract,
            timeframe=timeframe,
            timestamp_ny=timestamp_ny,
            session_date=row.session_date,
            session_type=row.session_type,
            price_ticks=row.price_ticks,
            price=Decimal(row.price_ticks) * tick_size,
        )
        accumulator.update(row)
        return accumulator

    def update(self, row: CleanTickRow) -> None:
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


def build_footprint_clusters(
    rows: Iterable[CleanTickRow],
    timeframe: str,
    tick_size: Decimal,
) -> list[FootprintClusterRow]:
    """Aggregate rows by time bar and price level."""
    timeframe_minutes = parse_timeframe_minutes(timeframe)
    accumulators: dict[tuple[object, str, datetime, int], _FootprintAccumulator] = {}

    for row in sort_clean_rows(rows):
        if row.session_date is None:
            continue

        bar_timestamp = floor_timestamp(row.timestamp_ny, timeframe_minutes)
        key = (row.session_date, row.session_type, bar_timestamp, row.price_ticks)

        if key not in accumulators:
            accumulators[key] = _FootprintAccumulator.from_row(
                row=row,
                timeframe=timeframe,
                timestamp_ny=bar_timestamp,
                tick_size=tick_size,
            )
        else:
            accumulators[key].update(row)

    return [
        accumulators[key].to_row()
        for key in sorted(
            accumulators,
            key=lambda item: (item[0], item[2], item[1], item[3]),
        )
    ]
