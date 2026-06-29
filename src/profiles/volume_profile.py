"""Build session volume profiles by price level."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from src.ingest.convert_ticks import CleanTickRow
from src.ingest.order_rows import sort_clean_rows


@dataclass(frozen=True)
class VolumeProfileRow:
    """One price level in a session volume profile."""

    symbol: str
    contract: str
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
class _ProfileAccumulator:
    symbol: str
    contract: str
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
        session_type: str,
        tick_size: Decimal,
    ) -> "_ProfileAccumulator":
        accumulator = cls(
            symbol=row.symbol,
            contract=row.contract,
            session_date=row.session_date,
            session_type=session_type,
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


def build_volume_profiles(
    rows: Iterable[CleanTickRow],
    tick_size: Decimal,
    include_full: bool = True,
) -> list[VolumeProfileRow]:
    """Aggregate rows into session and full-session volume profiles."""
    accumulators: dict[tuple[object, str, int], _ProfileAccumulator] = {}

    for row in sort_clean_rows(rows):
        if row.session_date is None:
            continue

        session_types = [row.session_type]
        if include_full:
            session_types.append("full")

        for session_type in session_types:
            key = (row.session_date, session_type, row.price_ticks)

            if key not in accumulators:
                accumulators[key] = _ProfileAccumulator.from_row(
                    row=row,
                    session_type=session_type,
                    tick_size=tick_size,
                )
            else:
                accumulators[key].update(row)

    return [
        accumulators[key].to_row()
        for key in sorted(accumulators, key=lambda item: (item[0], item[1], item[2]))
    ]
