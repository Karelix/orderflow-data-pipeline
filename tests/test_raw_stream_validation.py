from src.config import load_config
from src.validation import validate_raw_stream


def test_validate_raw_stream_passes_for_well_formed_rows(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 7519.00, 7519.00, 7519.00, 7519.00, 1, 1, 1, 0",
                "2026/5/24, 22:00:00, 7519.00, 7519.00, 7519.00, 7519.00, 3, 1, 1, 2",
                "2026/5/25, 13:30:00.018, 7520.25, 7520.25, 7520.25, 7520.25, 4, 1, 1, 3",
            ]
        ),
        encoding="utf-8",
    )

    report = validate_raw_stream(raw_file, load_config(), chunk_size=2)

    assert report.passed
    assert report.rows_scanned == 3
    assert report.chunks == 2
    assert report.adjacent_duplicate_timestamp_count == 1
    assert report.total_volume == 8
    assert report.total_bid_volume == 3
    assert report.total_ask_volume == 5
    assert report.total_delta == 2
    assert report.session_type_counts == {"globex": 2, "rth": 1}
    assert report.session_date_counts == {"2026-05-25": 3}
    assert report.tick_like_ratio == 1.0


def test_validate_raw_stream_flags_bad_rows(tmp_path) -> None:
    raw_file = tmp_path / "bad_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:01, 7519.00, 7520.00, 7519.00, 7519.10, 5, 1, 1, 1",
                "2026/5/24, 22:00:00, 7519.00, 7519.00, 7519.00, 7519.00, 1, 1, 1, 0",
            ]
        ),
        encoding="utf-8",
    )

    report = validate_raw_stream(raw_file, load_config(), chunk_size=1)

    assert not report.passed
    assert report.out_of_order_timestamp_count == 1
    assert report.volume_bid_ask_mismatch_count == 1
    assert report.price_tick_mismatch_count == 1
    assert report.non_tick_like_row_count == 1
    assert report.error_samples
