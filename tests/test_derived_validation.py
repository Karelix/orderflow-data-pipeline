from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from src.bars import build_time_bars
from src.ingest import CleanTickRow
from src.profiles import build_footprint_clusters, build_volume_profiles
from src.sessions.session_summary import build_session_summaries
from src.validation import validate_derived_datasets


NY = ZoneInfo("America/New_York")
TICK_SIZE = Decimal("0.25")


def make_tick(
    timestamp_ny: datetime,
    price: str,
    volume: int,
    bid_volume: int,
    ask_volume: int,
    session_type: str,
    sequence_id: int,
) -> CleanTickRow:
    price_decimal = Decimal(price)
    return CleanTickRow(
        symbol="ES",
        contract="ESU26-CME",
        sequence_id=sequence_id,
        timestamp_utc=timestamp_ny.astimezone(timezone.utc),
        timestamp_ny=timestamp_ny,
        session_date=date(2026, 5, 25),
        session_type=session_type,
        open=price_decimal,
        high=price_decimal,
        low=price_decimal,
        last=price_decimal,
        volume=volume,
        number_of_trades=1,
        bid_volume=bid_volume,
        ask_volume=ask_volume,
        delta=ask_volume - bid_volume,
        price_ticks=int(price_decimal / TICK_SIZE),
    )


def sample_ticks() -> list[CleanTickRow]:
    return [
        make_tick(datetime(2026, 5, 24, 18, 0, 0, tzinfo=NY), "100.00", 10, 7, 3, "globex", 0),
        make_tick(datetime(2026, 5, 25, 9, 30, 10, tzinfo=NY), "101.00", 5, 1, 4, "rth", 1),
        make_tick(datetime(2026, 5, 25, 9, 30, 40, tzinfo=NY), "100.75", 7, 5, 2, "rth", 2),
        make_tick(datetime(2026, 5, 25, 9, 31, 5, tzinfo=NY), "101.00", 2, 0, 2, "rth", 3),
    ]


def build_all(rows: list[CleanTickRow]):
    bars = build_time_bars(rows, "1m")
    summaries = build_session_summaries(rows)
    footprints = build_footprint_clusters(rows, "1m", TICK_SIZE)
    profiles = build_volume_profiles(rows, TICK_SIZE)
    return bars, summaries, footprints, profiles


def test_validate_derived_datasets_passes_when_totals_match() -> None:
    rows = sample_ticks()
    bars, summaries, footprints, profiles = build_all(rows)

    report = validate_derived_datasets(
        clean_rows=rows,
        bars=bars,
        session_summaries=summaries,
        footprint_clusters=footprints,
        volume_profiles=profiles,
        tick_size=TICK_SIZE,
    )

    assert report.passed
    assert report.failures == []


def test_validate_derived_datasets_fails_when_bar_volume_is_wrong() -> None:
    rows = sample_ticks()
    bars, summaries, footprints, profiles = build_all(rows)
    bad_bars = [replace(bars[0], volume=bars[0].volume + 1), *bars[1:]]

    report = validate_derived_datasets(
        clean_rows=rows,
        bars=bad_bars,
        session_summaries=summaries,
        footprint_clusters=footprints,
        volume_profiles=profiles,
        tick_size=TICK_SIZE,
    )

    assert not report.passed
    assert any(check.name.startswith("bar_totals") for check in report.failures)
