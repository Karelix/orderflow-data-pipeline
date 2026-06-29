from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from src.bars import build_time_bars
from src.config import load_config
from src.ingest import CleanTickRow
from src.manifest import build_manifest_for_parquet_tree, find_manifest_entries
from src.profiles import build_footprint_clusters, build_volume_profiles
from src.sessions.session_summary import build_session_summaries
from src.streaming import write_partitioned_derived_parquets_from_rows


NY = ZoneInfo("America/New_York")
TICK_SIZE = Decimal("0.25")


def make_tick(
    timestamp_ny: datetime,
    price: str,
    volume: int,
    bid_volume: int,
    ask_volume: int,
    session_date: date,
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
        session_date=session_date,
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


def sample_rows_with_chunk_boundaries() -> list[CleanTickRow]:
    return [
        make_tick(
            datetime(2026, 5, 25, 9, 30, 50, tzinfo=NY),
            "102.00",
            1,
            1,
            0,
            date(2026, 5, 25),
            "rth",
            0,
        ),
        make_tick(
            datetime(2026, 5, 24, 18, 0, 0, tzinfo=NY),
            "100.00",
            10,
            7,
            3,
            date(2026, 5, 25),
            "globex",
            1,
        ),
        make_tick(
            datetime(2026, 5, 25, 9, 30, 10, tzinfo=NY),
            "101.00",
            5,
            1,
            4,
            date(2026, 5, 25),
            "rth",
            2,
        ),
        make_tick(
            datetime(2026, 5, 25, 9, 30, 40, tzinfo=NY),
            "100.75",
            7,
            5,
            2,
            date(2026, 5, 25),
            "rth",
            3,
        ),
        make_tick(
            datetime(2026, 5, 25, 9, 31, 5, tzinfo=NY),
            "101.25",
            2,
            0,
            2,
            date(2026, 5, 25),
            "rth",
            4,
        ),
        make_tick(
            datetime(2026, 5, 25, 18, 0, 0, tzinfo=NY),
            "103.00",
            3,
            2,
            1,
            date(2026, 5, 26),
            "globex",
            5,
        ),
    ]


def test_partitioned_writer_matches_all_at_once_builders_across_chunks(tmp_path) -> None:
    rows = sample_rows_with_chunk_boundaries()
    config = load_config()
    output_root = tmp_path / "partitioned"

    results = write_partitioned_derived_parquets_from_rows(
        rows=rows,
        output_root=output_root,
        config=config,
        chunk_size_rows=2,
        timeframes=["1m"],
        flush_lag_sessions=0,
    )

    expected_bars = build_time_bars(rows, "1m")
    expected_summaries = build_session_summaries(rows)
    expected_profiles = build_volume_profiles(rows, TICK_SIZE)
    expected_footprints = build_footprint_clusters(rows, "1m", TICK_SIZE)

    actual_bars = _read_rows(output_root / "bars" / "timeframe=1m")
    actual_summaries = _read_rows(output_root / "session_summaries")
    actual_profiles = _read_rows(output_root / "volume_profiles")
    actual_footprints = _read_rows(output_root / "footprint_clusters" / "timeframe=1m")

    assert len(results) == 8
    assert _bar_projection(actual_bars) == [
        _display_projection(row.to_display_dict())
        for row in expected_bars
    ]
    assert _summary_projection(actual_summaries) == [
        _display_projection(row.to_display_dict())
        for row in expected_summaries
    ]
    assert _profile_projection(actual_profiles) == [
        _display_projection(row.to_display_dict())
        for row in expected_profiles
    ]
    assert _footprint_projection(actual_footprints) == [
        _display_projection(row.to_display_dict())
        for row in expected_footprints
    ]


def test_partitioned_writer_writes_one_session_date_per_file(tmp_path) -> None:
    rows = sample_rows_with_chunk_boundaries()
    config = load_config()
    output_root = tmp_path / "partitioned"

    write_partitioned_derived_parquets_from_rows(
        rows=rows,
        output_root=output_root,
        config=config,
        chunk_size_rows=1,
        timeframes=["1m"],
        flush_lag_sessions=1,
    )

    bar_files = sorted((output_root / "bars" / "timeframe=1m").rglob("*.parquet"))

    assert len(bar_files) == 2
    assert {path.parent.name for path in bar_files} == {
        "session=2026-05-25",
        "session=2026-05-26",
    }

    for path in bar_files:
        rows_in_file = pq.read_table(path).to_pylist()
        session_dates = {row["session_date"].isoformat() for row in rows_in_file}
        assert len(session_dates) == 1
        assert f"session={next(iter(session_dates))}" == path.parent.name

    manifest_entries = build_manifest_for_parquet_tree(
        root=output_root,
        config=config,
        repo_id="user/orderflow-es-001",
        repo_sequence=1,
        remote_prefix="main",
    )
    matches = find_manifest_entries(
        manifest_entries,
        dataset_type="bars",
        timeframe="1m",
        session_date=date(2026, 5, 25),
    )

    assert len(matches) == 1
    assert matches[0].session_date_min == date(2026, 5, 25)
    assert matches[0].session_date_max == date(2026, 5, 25)
    assert matches[0].remote_path == (
        "main/bars/timeframe=1m/year=2026/month=05/session=2026-05-25/part.parquet"
    )


def _read_rows(root) -> list[dict[str, object]]:
    rows = []

    for path in sorted(root.rglob("*.parquet")):
        rows.extend(pq.read_table(path).to_pylist())

    return rows


def _bar_projection(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        _display_projection(
            {
                "timestamp_utc": row["timestamp_utc"].isoformat(),
                "timestamp_ny": row["timestamp_ny"].isoformat(),
                "session_date": row["session_date"].isoformat(),
                "session_type": row["session_type"],
                "open": str(row["open"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "close": str(row["close"]),
                "volume": row["volume"],
                "buying_volume": row["buying_volume"],
                "selling_volume": row["selling_volume"],
                "delta": row["delta"],
                "cumulative_delta": row["cumulative_delta"],
            }
        )
        for row in sorted(rows, key=lambda item: (item["session_date"], item["timestamp_ny"], item["session_type"]))
    ]


def _summary_projection(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        _display_projection(
            {
                "session_date": row["session_date"].isoformat(),
                "open": str(row["open"]),
                "high": str(row["high"]),
                "low": str(row["low"]),
                "close": str(row["close"]),
                "total_volume": row["total_volume"],
                "buying_volume": row["buying_volume"],
                "selling_volume": row["selling_volume"],
                "delta": row["delta"],
                "cumulative_delta": row["cumulative_delta"],
                "globex_volume": row["globex_volume"],
                "rth_volume": row["rth_volume"],
            }
        )
        for row in sorted(rows, key=lambda item: item["session_date"])
    ]


def _profile_projection(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        _display_projection(
            {
                "session_date": row["session_date"].isoformat(),
                "session_type": row["session_type"],
                "price_ticks": row["price_ticks"],
                "volume": row["volume"],
                "buying_volume": row["buying_volume"],
                "selling_volume": row["selling_volume"],
                "delta": row["delta"],
            }
        )
        for row in sorted(rows, key=lambda item: (item["session_date"], item["session_type"], item["price_ticks"]))
    ]


def _footprint_projection(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        _display_projection(
            {
                "timestamp_utc": row["timestamp_utc"].isoformat(),
                "timestamp_ny": row["timestamp_ny"].isoformat(),
                "session_date": row["session_date"].isoformat(),
                "session_type": row["session_type"],
                "price_ticks": row["price_ticks"],
                "volume": row["volume"],
                "buying_volume": row["buying_volume"],
                "selling_volume": row["selling_volume"],
                "delta": row["delta"],
            }
        )
        for row in sorted(
            rows,
            key=lambda item: (
                item["session_date"],
                item["timestamp_ny"],
                item["session_type"],
                item["price_ticks"],
            ),
        )
    ]


def _display_projection(row: dict[str, object]) -> dict[str, object]:
    keys = [
        "timestamp_utc",
        "timestamp_ny",
        "session_date",
        "session_type",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        "volume",
        "buying_volume",
        "selling_volume",
        "delta",
        "cumulative_delta",
        "globex_volume",
        "rth_volume",
        "price_ticks",
    ]
    return {key: row[key] for key in keys if key in row}
