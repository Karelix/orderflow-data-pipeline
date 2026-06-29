from datetime import date

from src.config import load_config
from src.ingest.write_parquet import write_derived_sample_parquets
from src.manifest import (
    read_manifest_parquet,
    read_repository_registry_parquet,
    update_local_manifest_after_upload,
)


def test_update_local_manifest_after_upload_is_persistent_and_idempotent(tmp_path) -> None:
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
    config["paths"]["manifest_path"] = str(tmp_path / "metadata" / "manifest.parquet")
    config["paths"]["repository_registry_path"] = str(
        tmp_path / "metadata" / "repository_registry.parquet"
    )
    output_root = tmp_path / "derived_sample"
    write_derived_sample_parquets(
        input_path=raw_file,
        output_root=output_root,
        config=config,
        max_rows=10,
        timeframes=["1m"],
    )

    result = update_local_manifest_after_upload(
        uploaded_root=output_root,
        config=config,
        repo_id="user/orderflow-es-002",
        repo_sequence=2,
        remote_prefix="repo-002",
    )
    repeated_result = update_local_manifest_after_upload(
        uploaded_root=output_root,
        config=config,
        repo_id="user/orderflow-es-002",
        repo_sequence=2,
        remote_prefix="repo-002",
    )

    manifest_entries = read_manifest_parquet(config["paths"]["manifest_path"])
    repository_entries = read_repository_registry_parquet(
        config["paths"]["repository_registry_path"]
    )

    assert result.manifest_path.exists()
    assert result.repository_registry_path.exists()
    assert len(result.new_entries) == 4
    assert len(repeated_result.manifest_entries) == 4
    assert len(manifest_entries) == 4
    assert repository_entries[0].repo_id == "user/orderflow-es-002"
    assert repository_entries[0].repo_sequence == 2
    assert repository_entries[0].first_session_date == date(2026, 5, 25)
    assert repository_entries[0].last_session_date == date(2026, 5, 25)
