from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from src.bars import build_time_bars
from src.ingest import CleanTickRow
from src.profiles import build_footprint_clusters, build_volume_profiles
from src.sessions.session_summary import build_session_summaries


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


def test_build_time_bars_includes_volume_delta_and_cumulative_delta() -> None:
    bars = build_time_bars(sample_ticks(), "1m")

    assert len(bars) == 3
    assert bars[0].timestamp_ny.isoformat() == "2026-05-24T18:00:00-04:00"
    assert bars[0].session_type == "globex"
    assert bars[0].volume == 10
    assert bars[0].buying_volume == 3
    assert bars[0].selling_volume == 7
    assert bars[0].delta == -4
    assert bars[0].cumulative_delta == -4

    assert bars[1].timestamp_ny.isoformat() == "2026-05-25T09:30:00-04:00"
    assert bars[1].session_type == "rth"
    assert bars[1].open == Decimal("101.00")
    assert bars[1].high == Decimal("101.00")
    assert bars[1].low == Decimal("100.75")
    assert bars[1].close == Decimal("100.75")
    assert bars[1].volume == 12
    assert bars[1].buying_volume == 6
    assert bars[1].selling_volume == 6
    assert bars[1].delta == 0
    assert bars[1].cumulative_delta == -4
    assert bars[1].vwap == Decimal("100.854167")

    assert bars[2].cumulative_delta == -2


def test_build_session_summaries_aggregates_session_and_rth_globex_fields() -> None:
    summaries = build_session_summaries(sample_ticks())

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.session_date == date(2026, 5, 25)
    assert summary.open == Decimal("100.00")
    assert summary.high == Decimal("101.00")
    assert summary.low == Decimal("100.00")
    assert summary.close == Decimal("101.00")
    assert summary.total_volume == 24
    assert summary.buying_volume == 11
    assert summary.selling_volume == 13
    assert summary.delta == -2
    assert summary.cumulative_delta == -2
    assert summary.globex_volume == 10
    assert summary.globex_delta == -4
    assert summary.rth_open == Decimal("101.00")
    assert summary.rth_volume == 14
    assert summary.rth_delta == 2
    assert summary.range_points == Decimal("1.00")
    assert summary.rth_range_points == Decimal("0.25")


def test_build_footprint_clusters_groups_by_timeframe_and_price() -> None:
    clusters = build_footprint_clusters(sample_ticks(), timeframe="1m", tick_size=TICK_SIZE)

    rth_930 = [
        row for row in clusters if row.timestamp_ny.isoformat() == "2026-05-25T09:30:00-04:00"
    ]

    assert len(rth_930) == 2
    assert rth_930[0].price == Decimal("100.75")
    assert rth_930[0].volume == 7
    assert rth_930[0].buying_volume == 2
    assert rth_930[0].selling_volume == 5
    assert rth_930[0].delta == -3

    assert rth_930[1].price == Decimal("101.00")
    assert rth_930[1].volume == 5
    assert rth_930[1].delta == 3


def test_build_volume_profiles_creates_session_and_full_profiles() -> None:
    profiles = build_volume_profiles(sample_ticks(), tick_size=TICK_SIZE)

    full_101 = [
        row for row in profiles if row.session_type == "full" and row.price == Decimal("101.00")
    ][0]
    rth_101 = [
        row for row in profiles if row.session_type == "rth" and row.price == Decimal("101.00")
    ][0]
    globex_100 = [
        row for row in profiles if row.session_type == "globex" and row.price == Decimal("100.00")
    ][0]

    assert full_101.volume == 7
    assert full_101.buying_volume == 6
    assert full_101.selling_volume == 1
    assert full_101.delta == 5

    assert rth_101.volume == 7
    assert rth_101.delta == 5

    assert globex_100.volume == 10
    assert globex_100.delta == -4


def test_derived_builders_sort_rows_by_timestamp_then_sequence_id() -> None:
    out_of_order_rows = [
        make_tick(datetime(2026, 5, 25, 9, 30, 50, tzinfo=NY), "102.00", 1, 1, 0, "rth", 0),
        make_tick(datetime(2026, 5, 25, 9, 30, 10, tzinfo=NY), "101.00", 2, 0, 2, "rth", 1),
        make_tick(datetime(2026, 5, 25, 9, 30, 10, tzinfo=NY), "101.25", 3, 1, 2, "rth", 2),
    ]

    bars = build_time_bars(out_of_order_rows, "1m")
    summaries = build_session_summaries(out_of_order_rows)
    clusters = build_footprint_clusters(out_of_order_rows, "1m", TICK_SIZE)
    profiles = build_volume_profiles(out_of_order_rows, TICK_SIZE)

    assert len(bars) == 1
    assert bars[0].open == Decimal("101.00")
    assert bars[0].close == Decimal("102.00")
    assert bars[0].delta == 2
    assert bars[0].cumulative_delta == 2

    assert summaries[0].open == Decimal("101.00")
    assert summaries[0].close == Decimal("102.00")
    assert summaries[0].rth_open == Decimal("101.00")
    assert summaries[0].rth_close == Decimal("102.00")

    assert [row.price for row in clusters] == [
        Decimal("101.00"),
        Decimal("101.25"),
        Decimal("102.00"),
    ]
    assert [row.price for row in profiles if row.session_type == "rth"] == [
        Decimal("101.00"),
        Decimal("101.25"),
        Decimal("102.00"),
    ]
