from datetime import date

from src.config import load_config
from src.ingest.write_parquet import write_derived_sample_parquets
from src.manifest import (
    build_manifest_for_parquet_tree,
    build_repository_registry,
    find_manifest_entries,
    read_manifest_parquet,
    read_repository_registry_parquet,
    write_manifest_parquet,
    write_repository_registry_parquet,
)


def test_manifest_tracks_repo_and_finds_requested_slice(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 100.00, 100.00, 100.00, 100.00, 10, 1, 7, 3",
                "2026/5/25, 13:30:10, 101.00, 101.00, 101.00, 101.00, 5, 1, 1, 4",
                "2026/5/25, 13:30:40, 100.75, 100.75, 100.75, 100.75, 7, 1, 5, 2",
            ]
        ),
        encoding="utf-8",
    )
    config = load_config()
    output_root = tmp_path / "derived_sample"
    write_derived_sample_parquets(
        input_path=raw_file,
        output_root=output_root,
        config=config,
        max_rows=10,
        timeframes=["1m"],
    )

    entries = build_manifest_for_parquet_tree(
        root=output_root,
        config=config,
        repo_id="user/orderflow-es-002",
        repo_sequence=2,
        remote_prefix="repo-002",
    )
    registry = build_repository_registry(entries, config)

    assert len(entries) == 4
    assert {entry.repo_id for entry in entries} == {"user/orderflow-es-002"}
    assert {entry.repo_sequence for entry in entries} == {2}
    assert registry[0].repo_id == "user/orderflow-es-002"
    assert registry[0].repo_sequence == 2
    assert registry[0].current_manifest_size_bytes > 0
    assert registry[0].first_session_date == date(2026, 5, 25)
    assert registry[0].last_session_date == date(2026, 5, 25)

    bar_matches = find_manifest_entries(
        entries,
        dataset_type="bars",
        symbol="ES",
        timeframe="1m",
        session_date=date(2026, 5, 25),
    )
    profile_matches = find_manifest_entries(
        entries,
        dataset_type="volume_profiles",
        session_type="rth",
        session_date=date(2026, 5, 25),
    )

    assert len(bar_matches) == 1
    assert bar_matches[0].repo_id == "user/orderflow-es-002"
    assert bar_matches[0].remote_path == "repo-002/bars/timeframe=1m/part.parquet"
    assert len(profile_matches) == 1
    assert profile_matches[0].session_type_values == "full,globex,rth"

    manifest_path = tmp_path / "manifest.parquet"
    registry_path = tmp_path / "repository_registry.parquet"
    write_manifest_parquet(entries, manifest_path)
    write_repository_registry_parquet(registry, registry_path)

    assert manifest_path.stat().st_size > 0
    assert registry_path.stat().st_size > 0
    assert read_manifest_parquet(manifest_path)[0].repo_id == "user/orderflow-es-002"
    assert read_repository_registry_parquet(registry_path)[0].repo_sequence == 2
