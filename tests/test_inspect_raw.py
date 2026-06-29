from decimal import Decimal

import pytest

from src.config import load_config
from src.ingest import inspect_raw_file


def test_inspect_raw_file_summarizes_sierra_export(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 7519.00, 7519.00, 7519.00, 7519.00, 1, 1, 1, 0",
                "2026/5/24, 22:00:00, 7519.00, 7519.25, 7519.00, 7519.25, 3, 1, 1, 2",
                "2026/5/24, 22:00:01.018, 7519.25, 7519.25, 7519.00, 7519.00, 2, 1, 0, 2",
            ]
        ),
        encoding="utf-8",
    )

    inspection = inspect_raw_file(raw_file, load_config())

    assert inspection.path == raw_file
    assert inspection.row_count == 3
    assert not inspection.is_partial
    assert inspection.max_rows is None
    assert inspection.columns == [
        "Date",
        "Time",
        "Open",
        "High",
        "Low",
        "Last",
        "Volume",
        "NumberOfTrades",
        "BidVolume",
        "AskVolume",
    ]
    assert str(inspection.first_timestamp) == "2026-05-24 22:00:00"
    assert str(inspection.last_timestamp) == "2026-05-24 22:00:01.018000"
    assert inspection.duplicate_timestamp_count == 1
    assert inspection.min_last_price == Decimal("7519.00")
    assert inspection.max_last_price == Decimal("7519.25")
    assert inspection.total_volume == 6
    assert inspection.total_bid_volume == 2
    assert inspection.total_ask_volume == 4


def test_inspect_raw_file_rejects_missing_required_columns(tmp_path) -> None:
    raw_file = tmp_path / "bad_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Last, Volume",
                "2026/5/24, 22:00:00, 7519.00, 1",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required columns"):
        inspect_raw_file(raw_file, load_config())


def test_inspect_raw_file_can_limit_rows(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 7519.00, 7519.00, 7519.00, 7519.00, 1, 1, 1, 0",
                "2026/5/24, 22:00:01, 7519.00, 7519.00, 7519.00, 7519.00, 2, 1, 1, 1",
            ]
        ),
        encoding="utf-8",
    )

    inspection = inspect_raw_file(raw_file, load_config(), max_rows=1)

    assert inspection.row_count == 1
    assert inspection.is_partial
    assert inspection.max_rows == 1
    assert inspection.total_volume == 1
    assert str(inspection.last_timestamp) == "2026-05-24 22:00:00"
