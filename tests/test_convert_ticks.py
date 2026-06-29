from copy import deepcopy
from datetime import date
from decimal import Decimal

from src.config import load_config
from src.ingest import calculate_price_ticks, iter_clean_tick_rows


def test_iter_clean_tick_rows_adds_session_and_derived_fields(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 7519.00, 7519.25, 7519.00, 7519.00, 10, 2, 7, 3",
                "2026/5/25, 13:30:00.018, 7520.00, 7520.25, 7519.75, 7520.25, 4, 1, 1, 3",
                "2026/5/25, 21:00:00, 7521.00, 7521.00, 7521.00, 7521.00, 99, 1, 99, 0",
            ]
        ),
        encoding="utf-8",
    )

    rows = list(iter_clean_tick_rows(raw_file, load_config()))

    assert len(rows) == 2
    assert rows[0].symbol == "ES"
    assert rows[0].contract == "ESU26-CME"
    assert rows[0].sequence_id == 0
    assert rows[0].timestamp_utc.isoformat() == "2026-05-24T22:00:00+00:00"
    assert rows[0].timestamp_ny.isoformat() == "2026-05-24T18:00:00-04:00"
    assert rows[0].session_date == date(2026, 5, 25)
    assert rows[0].session_type == "globex"
    assert rows[0].delta == -4
    assert rows[0].price_ticks == 30076

    assert rows[1].sequence_id == 1
    assert rows[1].timestamp_ny.isoformat() == "2026-05-25T09:30:00.018000-04:00"
    assert rows[1].session_type == "rth"
    assert rows[1].delta == 2
    assert rows[1].price_ticks == 30081


def test_iter_clean_tick_rows_can_include_closed_rows_when_configured(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/25, 21:00:00, 7521.00, 7521.00, 7521.00, 7521.00, 99, 1, 99, 0",
            ]
        ),
        encoding="utf-8",
    )
    config = deepcopy(load_config())
    config["processing"]["exclude_maintenance_break"] = False

    rows = list(iter_clean_tick_rows(raw_file, config))

    assert len(rows) == 1
    assert rows[0].session_date is None
    assert rows[0].session_type == "closed"


def test_iter_clean_tick_rows_respects_raw_max_rows_before_filtering(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/25, 21:00:00, 7521.00, 7521.00, 7521.00, 7521.00, 99, 1, 99, 0",
                "2026/5/25, 22:00:00, 7522.00, 7522.00, 7522.00, 7522.00, 1, 1, 0, 1",
            ]
        ),
        encoding="utf-8",
    )

    rows = list(iter_clean_tick_rows(raw_file, load_config(), max_rows=1))

    assert rows == []


def test_calculate_price_ticks_uses_decimal_tick_size() -> None:
    assert calculate_price_ticks(Decimal("7519.00"), Decimal("0.25")) == 30076
    assert calculate_price_ticks(Decimal("7519.25"), Decimal("0.25")) == 30077
