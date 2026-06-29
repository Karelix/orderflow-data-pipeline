"""Validate derived order-flow datasets against cleaned tick rows."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from src.bars.time_bars import TimeBar
from src.ingest.convert_ticks import CleanTickRow
from src.profiles.footprint import FootprintClusterRow
from src.profiles.volume_profile import VolumeProfileRow
from src.sessions.session_summary import SessionSummary


@dataclass(frozen=True)
class ValidationCheck:
    """One validation assertion."""

    name: str
    expected: object
    actual: object
    passed: bool


@dataclass(frozen=True)
class ValidationReport:
    """Collection of validation checks."""

    checks: list[ValidationCheck]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failures(self) -> list[ValidationCheck]:
        return [check for check in self.checks if not check.passed]

    def format(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            f"Derived Dataset Validation: {status}",
            f"Checks: {len(self.checks)}",
            f"Failures: {len(self.failures)}",
        ]

        for failure in self.failures:
            lines.append(
                f"- {failure.name}: expected={failure.expected}, actual={failure.actual}"
            )

        return "\n".join(lines)


@dataclass
class _FlowTotals:
    volume: int = 0
    buying_volume: int = 0
    selling_volume: int = 0
    delta: int = 0
    number_of_trades: int = 0

    def add_tick(self, row: CleanTickRow) -> None:
        self.volume += row.volume
        self.buying_volume += row.ask_volume
        self.selling_volume += row.bid_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def add_bar(self, row: TimeBar) -> None:
        self.volume += row.volume
        self.buying_volume += row.buying_volume
        self.selling_volume += row.selling_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def add_summary(self, row: SessionSummary) -> None:
        self.volume += row.total_volume
        self.buying_volume += row.buying_volume
        self.selling_volume += row.selling_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def add_profile(self, row: VolumeProfileRow) -> None:
        self.volume += row.volume
        self.buying_volume += row.buying_volume
        self.selling_volume += row.selling_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def add_footprint(self, row: FootprintClusterRow) -> None:
        self.volume += row.volume
        self.buying_volume += row.buying_volume
        self.selling_volume += row.selling_volume
        self.delta += row.delta
        self.number_of_trades += row.number_of_trades

    def as_tuple(self) -> tuple[int, int, int, int, int]:
        return (
            self.volume,
            self.buying_volume,
            self.selling_volume,
            self.delta,
            self.number_of_trades,
        )


def validate_derived_datasets(
    clean_rows: Iterable[CleanTickRow],
    bars: Iterable[TimeBar],
    session_summaries: Iterable[SessionSummary],
    footprint_clusters: Iterable[FootprintClusterRow],
    volume_profiles: Iterable[VolumeProfileRow],
    tick_size: Decimal | None = None,
) -> ValidationReport:
    """Validate volume/delta conservation across derived datasets."""
    clean_row_list = [row for row in clean_rows if row.session_date is not None]
    bar_list = list(bars)
    summary_list = list(session_summaries)
    footprint_list = list(footprint_clusters)
    profile_list = list(volume_profiles)

    checks: list[ValidationCheck] = []
    tick_totals_by_session = _tick_totals_by_session(clean_row_list)
    tick_totals_by_session_type = _tick_totals_by_session_type(clean_row_list)

    _validate_session_summaries(checks, summary_list, tick_totals_by_session)
    _validate_bars(checks, bar_list, tick_totals_by_session)
    _validate_footprints(checks, footprint_list, tick_totals_by_session, tick_size)
    _validate_profiles(
        checks,
        profile_list,
        tick_totals_by_session,
        tick_totals_by_session_type,
        tick_size,
    )

    return ValidationReport(checks=checks)


def _validate_session_summaries(
    checks: list[ValidationCheck],
    summaries: list[SessionSummary],
    tick_totals_by_session: dict[object, _FlowTotals],
) -> None:
    summary_totals: dict[object, _FlowTotals] = {}

    for summary in summaries:
        totals = summary_totals.setdefault(summary.session_date, _FlowTotals())
        totals.add_summary(summary)
        _add_check(
            checks,
            f"session_summary_delta_identity:{summary.session_date}",
            summary.buying_volume - summary.selling_volume,
            summary.delta,
        )
        _add_check(
            checks,
            f"session_summary_cumulative_delta:{summary.session_date}",
            summary.delta,
            summary.cumulative_delta,
        )

    for session_date, expected in tick_totals_by_session.items():
        actual = summary_totals.get(session_date, _FlowTotals())
        _add_check(
            checks,
            f"session_summary_totals:{session_date}",
            expected.as_tuple(),
            actual.as_tuple(),
        )


def _validate_bars(
    checks: list[ValidationCheck],
    bars: list[TimeBar],
    tick_totals_by_session: dict[object, _FlowTotals],
) -> None:
    bars_by_timeframe_session: dict[tuple[str, object], list[TimeBar]] = {}

    for bar in bars:
        key = (bar.timeframe, bar.session_date)
        bars_by_timeframe_session.setdefault(key, []).append(bar)
        _add_check(
            checks,
            f"bar_delta_identity:{bar.timeframe}:{bar.session_date}:{bar.timestamp_ny}",
            bar.buying_volume - bar.selling_volume,
            bar.delta,
        )

    for (timeframe, session_date), grouped_bars in bars_by_timeframe_session.items():
        totals = _FlowTotals()
        running_delta = 0

        for bar in sorted(grouped_bars, key=lambda item: item.timestamp_ny):
            totals.add_bar(bar)
            running_delta += bar.delta
            _add_check(
                checks,
                f"bar_running_cumulative_delta:{timeframe}:{session_date}:{bar.timestamp_ny}",
                running_delta,
                bar.cumulative_delta,
            )

        expected = tick_totals_by_session[session_date]
        _add_check(
            checks,
            f"bar_totals:{timeframe}:{session_date}",
            expected.as_tuple(),
            totals.as_tuple(),
        )
        _add_check(
            checks,
            f"bar_final_cumulative_delta:{timeframe}:{session_date}",
            expected.delta,
            running_delta,
        )


def _validate_footprints(
    checks: list[ValidationCheck],
    footprints: list[FootprintClusterRow],
    tick_totals_by_session: dict[object, _FlowTotals],
    tick_size: Decimal | None,
) -> None:
    totals_by_timeframe_session: dict[tuple[str, object], _FlowTotals] = {}

    for row in footprints:
        key = (row.timeframe, row.session_date)
        totals = totals_by_timeframe_session.setdefault(key, _FlowTotals())
        totals.add_footprint(row)
        _add_check(
            checks,
            f"footprint_delta_identity:{row.timeframe}:{row.session_date}:{row.timestamp_ny}:{row.price_ticks}",
            row.buying_volume - row.selling_volume,
            row.delta,
        )

        if tick_size is not None:
            _add_check(
                checks,
                f"footprint_price_ticks:{row.timeframe}:{row.session_date}:{row.timestamp_ny}:{row.price_ticks}",
                Decimal(row.price_ticks) * tick_size,
                row.price,
            )

    for (timeframe, session_date), actual in totals_by_timeframe_session.items():
        expected = tick_totals_by_session[session_date]
        _add_check(
            checks,
            f"footprint_totals:{timeframe}:{session_date}",
            expected.as_tuple(),
            actual.as_tuple(),
        )


def _validate_profiles(
    checks: list[ValidationCheck],
    profiles: list[VolumeProfileRow],
    tick_totals_by_session: dict[object, _FlowTotals],
    tick_totals_by_session_type: dict[tuple[object, str], _FlowTotals],
    tick_size: Decimal | None,
) -> None:
    profile_totals: dict[tuple[object, str], _FlowTotals] = {}

    for row in profiles:
        key = (row.session_date, row.session_type)
        totals = profile_totals.setdefault(key, _FlowTotals())
        totals.add_profile(row)
        _add_check(
            checks,
            f"profile_delta_identity:{row.session_date}:{row.session_type}:{row.price_ticks}",
            row.buying_volume - row.selling_volume,
            row.delta,
        )

        if tick_size is not None:
            _add_check(
                checks,
                f"profile_price_ticks:{row.session_date}:{row.session_type}:{row.price_ticks}",
                Decimal(row.price_ticks) * tick_size,
                row.price,
            )

    for session_date, expected in tick_totals_by_session.items():
        actual = profile_totals.get((session_date, "full"), _FlowTotals())
        _add_check(
            checks,
            f"profile_full_totals:{session_date}",
            expected.as_tuple(),
            actual.as_tuple(),
        )

    for key, expected in tick_totals_by_session_type.items():
        actual = profile_totals.get(key, _FlowTotals())
        _add_check(
            checks,
            f"profile_session_type_totals:{key[0]}:{key[1]}",
            expected.as_tuple(),
            actual.as_tuple(),
        )


def _tick_totals_by_session(rows: list[CleanTickRow]) -> dict[object, _FlowTotals]:
    totals_by_session: dict[object, _FlowTotals] = {}

    for row in rows:
        totals = totals_by_session.setdefault(row.session_date, _FlowTotals())
        totals.add_tick(row)

    return totals_by_session


def _tick_totals_by_session_type(
    rows: list[CleanTickRow],
) -> dict[tuple[object, str], _FlowTotals]:
    totals_by_session_type: dict[tuple[object, str], _FlowTotals] = {}

    for row in rows:
        key = (row.session_date, row.session_type)
        totals = totals_by_session_type.setdefault(key, _FlowTotals())
        totals.add_tick(row)

    return totals_by_session_type


def _add_check(
    checks: list[ValidationCheck],
    name: str,
    expected: object,
    actual: object,
) -> None:
    checks.append(
        ValidationCheck(
            name=name,
            expected=expected,
            actual=actual,
            passed=expected == actual,
        )
    )
