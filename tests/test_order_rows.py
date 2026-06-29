from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from src.ingest import CleanTickRow, sort_clean_rows


NY = ZoneInfo("America/New_York")


def make_row(timestamp_ny: datetime, sequence_id: int) -> CleanTickRow:
    return CleanTickRow(
        symbol="ES",
        contract="ESU26-CME",
        sequence_id=sequence_id,
        timestamp_utc=timestamp_ny.astimezone(timezone.utc),
        timestamp_ny=timestamp_ny,
        session_date=date(2026, 5, 25),
        session_type="rth",
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        last=Decimal("100.00"),
        volume=1,
        number_of_trades=1,
        bid_volume=1,
        ask_volume=0,
        delta=-1,
        price_ticks=400,
    )


def test_sort_clean_rows_uses_timestamp_then_sequence_id() -> None:
    rows = [
        make_row(datetime(2026, 5, 25, 9, 30, 1, tzinfo=NY), sequence_id=0),
        make_row(datetime(2026, 5, 25, 9, 30, 0, tzinfo=NY), sequence_id=2),
        make_row(datetime(2026, 5, 25, 9, 30, 0, tzinfo=NY), sequence_id=1),
    ]

    sorted_rows = sort_clean_rows(rows)

    assert [row.sequence_id for row in sorted_rows] == [1, 2, 0]
