"""Build per-session order-flow summary rows."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from src.ingest.convert_ticks import CleanTickRow
from src.ingest.order_rows import sort_clean_rows


@dataclass(frozen=True)
class SessionSummary:
    """One-row summary of a trading session."""

    symbol: str
    contract: str
    session_date: object
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    total_volume: int
    buying_volume: int
    selling_volume: int
    delta: int
    cumulative_delta: int
    number_of_trades: int
    globex_high: Decimal | None
    globex_low: Decimal | None
    globex_volume: int
    globex_delta: int
    rth_open: Decimal | None
    rth_high: Decimal | None
    rth_low: Decimal | None
    rth_close: Decimal | None
    rth_volume: int
    rth_delta: int
    range_points: Decimal
    rth_range_points: Decimal | None

    def to_display_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "contract": self.contract,
            "session_date": self.session_date.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "total_volume": self.total_volume,
            "buying_volume": self.buying_volume,
            "selling_volume": self.selling_volume,
            "delta": self.delta,
            "cumulative_delta": self.cumulative_delta,
            "number_of_trades": self.number_of_trades,
            "globex_high": _decimal_to_string(self.globex_high),
            "globex_low": _decimal_to_string(self.globex_low),
            "globex_volume": self.globex_volume,
            "globex_delta": self.globex_delta,
            "rth_open": _decimal_to_string(self.rth_open),
            "rth_high": _decimal_to_string(self.rth_high),
            "rth_low": _decimal_to_string(self.rth_low),
            "rth_close": _decimal_to_string(self.rth_close),
            "rth_volume": self.rth_volume,
            "rth_delta": self.rth_delta,
            "range_points": str(self.range_points),
            "rth_range_points": _decimal_to_string(self.rth_range_points),
        }


@dataclass
class _SessionAccumulator:
    symbol: str
    contract: str
    session_date: object
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    total_volume: int
    buying_volume: int
    selling_volume: int
    delta: int
    number_of_trades: int
    globex_high: Decimal | None = None
    globex_low: Decimal | None = None
    globex_volume: int = 0
    globex_delta: int = 0
    rth_open: Decimal | None = None
    rth_high: Decimal | None = None
    rth_low: Decimal | None = None
    rth_close: Decimal | None = None
    rth_volume: int = 0
    rth_delta: int = 0

    @classmethod
    def from_row(cls, row: CleanTickRow) -> "_SessionAccumulator":
        accumulator = cls(
            symbol=row.symbol,
            contract=row.contract,
            session_date=row.session_date,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.last,
            total_volume=0,
            buying_volume=0,
            selling_volume=0,
            delta=0,
            number_of_trades=0,
        )
        accumulator.update(row)
        return accumulator

    def update(self, row: CleanTickRow) -> None:
        self.high = max(self.high, row.high)
        self.low = min(self.low, row.low)
        self.close = row.last
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
            self.rth_open = self.rth_open or row.open
            self.rth_high = _max_optional(self.rth_high, row.high)
            self.rth_low = _min_optional(self.rth_low, row.low)
            self.rth_close = row.last
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


def build_session_summaries(rows: Iterable[CleanTickRow]) -> list[SessionSummary]:
    """Build one order-flow summary row per session date."""
    accumulators: dict[object, _SessionAccumulator] = {}

    for row in sort_clean_rows(rows):
        if row.session_date is None:
            continue

        if row.session_date not in accumulators:
            accumulators[row.session_date] = _SessionAccumulator.from_row(row)
        else:
            accumulators[row.session_date].update(row)

    return [
        accumulators[session_date].to_summary()
        for session_date in sorted(accumulators)
    ]


def _max_optional(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return max(current, value)


def _min_optional(current: Decimal | None, value: Decimal) -> Decimal:
    if current is None:
        return value

    return min(current, value)


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None

    return str(value)
