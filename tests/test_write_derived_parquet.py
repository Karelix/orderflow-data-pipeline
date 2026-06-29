import pyarrow.parquet as pq

from src.config import load_config
from src.ingest.write_parquet import write_derived_sample_parquets


def test_write_derived_sample_parquets(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 100.00, 100.00, 100.00, 100.00, 10, 1, 7, 3",
                "2026/5/25, 13:30:10, 101.00, 101.00, 101.00, 101.00, 5, 1, 1, 4",
                "2026/5/25, 13:30:40, 100.75, 100.75, 100.75, 100.75, 7, 1, 5, 2",
                "2026/5/25, 13:31:05, 101.00, 101.00, 101.00, 101.00, 2, 1, 0, 2",
            ]
        ),
        encoding="utf-8",
    )
    output_root = tmp_path / "derived_sample"

    results = write_derived_sample_parquets(
        input_path=raw_file,
        output_root=output_root,
        config=load_config(),
        max_rows=10,
        timeframes=["1m"],
    )

    assert len(results) == 4
    assert all(result.file_size_bytes > 0 for result in results)

    bars_path = output_root / "bars" / "timeframe=1m" / "part.parquet"
    summaries_path = output_root / "session_summaries" / "part.parquet"
    footprints_path = output_root / "footprint_clusters" / "timeframe=1m" / "part.parquet"
    profiles_path = output_root / "volume_profiles" / "part.parquet"

    bars = pq.read_table(bars_path)
    summaries = pq.read_table(summaries_path)
    footprints = pq.read_table(footprints_path)
    profiles = pq.read_table(profiles_path)

    assert bars.num_rows == 3
    assert summaries.num_rows == 1
    assert footprints.num_rows == 4
    assert profiles.num_rows == 6

    bar_rows = bars.to_pylist()
    assert bar_rows[1]["timeframe"] == "1m"
    assert bar_rows[1]["volume"] == 12
    assert bar_rows[1]["buying_volume"] == 6
    assert bar_rows[1]["selling_volume"] == 6
    assert bar_rows[1]["delta"] == 0
    assert bar_rows[1]["cumulative_delta"] == -4

    summary = summaries.to_pylist()[0]
    assert summary["total_volume"] == 24
    assert summary["delta"] == -2

    footprint_rows = footprints.to_pylist()
    assert {row["price_ticks"] for row in footprint_rows} == {400, 403, 404}

    profile_rows = profiles.to_pylist()
    assert {"globex", "rth", "full"} == {
        row["session_type"] for row in profile_rows
    }
