import pyarrow.parquet as pq

from src.config import load_config
from src.ingest.write_parquet import write_clean_tick_sample_parquet


def test_write_clean_tick_sample_parquet(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 7519.00, 7519.25, 7519.00, 7519.00, 10, 2, 7, 3",
                "2026/5/25, 13:30:00.018, 7520.00, 7520.25, 7519.75, 7520.25, 4, 1, 1, 3",
            ]
        ),
        encoding="utf-8",
    )
    output_file = tmp_path / "clean_ticks.parquet"

    result = write_clean_tick_sample_parquet(
        input_path=raw_file,
        output_path=output_file,
        config=load_config(),
        max_rows=10,
    )

    table = pq.read_table(output_file)
    data = table.to_pylist()

    assert result.path == output_file
    assert result.rows == 2
    assert result.file_size_bytes > 0
    assert table.schema.field("timestamp_utc").type.tz == "UTC"
    assert table.schema.field("timestamp_ny").type.tz == "America/New_York"
    assert data[0]["symbol"] == "ES"
    assert data[0]["session_date"].isoformat() == "2026-05-25"
    assert data[0]["session_type"] == "globex"
    assert data[0]["delta"] == -4
    assert data[0]["price_ticks"] == 30076
    assert data[1]["session_type"] == "rth"
    assert data[1]["price_ticks"] == 30081
