from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from src.config import load_config
from src.pipeline import RawValidationFailedError, process_raw_file_to_hf


class FakeHfApi:
    def __init__(self) -> None:
        self.uploaded = []

    def repo_info(self, **kwargs):
        return SimpleNamespace(siblings=[])

    def upload_file(self, **kwargs) -> None:
        self.uploaded.append(kwargs)


def test_process_raw_file_to_hf_dry_run_writes_partitioned_outputs(tmp_path) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/25, 13:30:50, 102.00, 102.00, 102.00, 102.00, 1, 1, 1, 0",
                "2026/5/24, 22:00:00, 100.00, 100.00, 100.00, 100.00, 10, 1, 7, 3",
                "2026/5/25, 13:30:10, 101.00, 101.00, 101.00, 101.00, 5, 1, 1, 4",
                "2026/5/25, 13:30:40, 100.75, 100.75, 100.75, 100.75, 7, 1, 5, 2",
            ]
        ),
        encoding="utf-8",
    )
    config = _test_config(tmp_path)
    config["validation"]["max_allowed_out_of_order_timestamps"] = 1
    output_root = tmp_path / "partitioned"

    result = process_raw_file_to_hf(
        input_path=raw_file,
        output_root=output_root,
        remote_prefix="main/test",
        config=config,
        validation_chunk_size=2,
        build_chunk_size=2,
        timeframes=["1m"],
        dry_run_upload=True,
        skip_remote_size_check=True,
    )

    bars_path = (
        output_root
        / "bars"
        / "timeframe=1m"
        / "year=2026"
        / "month=05"
        / "session=2026-05-25"
        / "part.parquet"
    )
    rows = pq.read_table(bars_path).to_pylist()

    assert result.raw_validation_report.out_of_order_timestamp_count == 1
    assert result.upload_result.dry_run is True
    assert result.output_cleaned is False
    assert bars_path.exists()
    assert {row["session_date"].isoformat() for row in rows} == {"2026-05-25"}
    assert result.upload_result.plan.files[0].path_in_repo.startswith("main/test/")


def test_process_raw_file_to_hf_stops_before_write_when_validation_fails(tmp_path) -> None:
    raw_file = tmp_path / "bad_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/25, 13:30:10, 101.00, 101.00, 101.00, 101.00, 5, 1, 1, 1",
            ]
        ),
        encoding="utf-8",
    )
    config = _test_config(tmp_path)
    output_root = tmp_path / "partitioned"

    with pytest.raises(RawValidationFailedError, match="volume mismatches"):
        process_raw_file_to_hf(
            input_path=raw_file,
            output_root=output_root,
            remote_prefix="main/test",
            config=config,
            validation_chunk_size=10,
            build_chunk_size=2,
            timeframes=["1m"],
            dry_run_upload=True,
            skip_remote_size_check=True,
        )

    assert not output_root.exists()


def test_process_raw_file_to_hf_uploads_with_fake_api_and_cleans_output(
    tmp_path,
    monkeypatch,
) -> None:
    raw_file = tmp_path / "sample_sierra.txt"
    raw_file.write_text(
        "\n".join(
            [
                "Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume",
                "2026/5/24, 22:00:00, 100.00, 100.00, 100.00, 100.00, 10, 1, 7, 3",
                "2026/5/25, 13:30:10, 101.00, 101.00, 101.00, 101.00, 5, 1, 1, 4",
            ]
        ),
        encoding="utf-8",
    )
    config = _test_config(tmp_path)
    output_root = tmp_path / "partitioned"
    api = FakeHfApi()
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "token")

    result = process_raw_file_to_hf(
        input_path=raw_file,
        output_root=output_root,
        remote_prefix="main/test",
        config=config,
        validation_chunk_size=10,
        build_chunk_size=1,
        timeframes=["1m"],
        dry_run_upload=False,
        skip_remote_size_check=True,
        cleanup_output_after_upload=True,
        api=api,
    )

    uploaded_paths = [item["path_in_repo"] for item in api.uploaded]

    assert result.upload_result.dry_run is False
    assert result.output_cleaned is True
    assert not output_root.exists()
    assert config["paths"]["manifest_path"]
    assert result.upload_result.manifest_update is not None
    assert {entry.data_tier for entry in result.upload_result.manifest_update.manifest_entries} == {
        "test"
    }
    assert any(path.startswith("main/test/bars/timeframe=1m/") for path in uploaded_paths)
    assert "metadata/manifest.parquet" in uploaded_paths
    assert "metadata/repository_registry.parquet" in uploaded_paths


def _test_config(tmp_path) -> dict:
    config = load_config()
    config["paths"]["manifest_path"] = str(tmp_path / "metadata" / "manifest.parquet")
    config["paths"]["repository_registry_path"] = str(
        tmp_path / "metadata" / "repository_registry.parquet"
    )
    config["storage"]["active_repo_id"] = "karelix/orderflow-es-001"
    config["storage"]["repositories"] = [
        {
            "repo_id": "karelix/orderflow-es-001",
            "repo_sequence": 1,
            "role": "active",
            "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
        }
    ]
    config["storage"]["repo_size_limit_bytes"] = 1000000
    config["validation"] = {"max_allowed_out_of_order_timestamps": 0}
    return config
