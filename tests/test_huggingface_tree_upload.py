from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from src.storage import (
    upload_parquet_files_to_hf,
    build_parquet_tree_upload_plan,
    collect_parquet_tree_files,
    upload_parquet_tree_to_hf,
)


class FakeHfApi:
    def __init__(self, repo_sizes: dict[str, int] | None = None) -> None:
        self.repo_sizes = repo_sizes or {}
        self.created = []
        self.uploaded = []
        self.commits = []
        self.repo_info_calls = []

    def repo_info(self, **kwargs):
        self.repo_info_calls.append(kwargs)
        repo_id = kwargs["repo_id"]
        size = self.repo_sizes.get(repo_id, 0)
        return SimpleNamespace(siblings=[SimpleNamespace(size=size)])

    def create_repo(self, **kwargs) -> None:
        self.created.append(kwargs)

    def upload_file(self, **kwargs) -> None:
        self.uploaded.append(kwargs)

    def create_commit(self, **kwargs) -> None:
        self.commits.append(kwargs)

        for operation in kwargs["operations"]:
            self.uploaded.append(
                {
                    "path_in_repo": operation.path_in_repo,
                    "path_or_fileobj": operation.path_or_fileobj,
                    "repo_id": kwargs["repo_id"],
                    "repo_type": kwargs["repo_type"],
                    "token": kwargs["token"],
                }
            )


class FlakyUploadHfApi(FakeHfApi):
    def __init__(self, repo_sizes: dict[str, int] | None = None) -> None:
        super().__init__(repo_sizes=repo_sizes)
        self.remaining_upload_failures = 1

    def create_commit(self, **kwargs) -> None:
        if self.remaining_upload_failures:
            self.remaining_upload_failures -= 1
            raise RuntimeError("temporary gateway timeout")

        super().create_commit(**kwargs)


def test_collect_parquet_tree_files_maps_remote_paths(tmp_path) -> None:
    root = tmp_path / "derived"
    parquet_path = root / "bars" / "timeframe=1m" / "part.parquet"
    parquet_path.parent.mkdir(parents=True)
    parquet_path.write_bytes(b"parquet")

    files = collect_parquet_tree_files(root, remote_prefix="samples/test")

    assert len(files) == 1
    assert files[0].relative_path.as_posix() == "bars/timeframe=1m/part.parquet"
    assert files[0].path_in_repo == "samples/test/bars/timeframe=1m/part.parquet"
    assert files[0].size_bytes == len(b"parquet")


def test_build_upload_plan_selects_first_repo_with_capacity(tmp_path, monkeypatch) -> None:
    root = tmp_path / "derived"
    first_file = root / "bars" / "part.parquet"
    second_file = root / "profiles" / "part.parquet"
    first_file.parent.mkdir(parents=True)
    second_file.parent.mkdir(parents=True)
    first_file.write_bytes(b"a" * 200)
    second_file.write_bytes(b"b" * 100)
    monkeypatch.setenv("HF_TOKEN_REPO_1", "token-1")
    monkeypatch.setenv("HF_TOKEN_REPO_2", "token-2")
    config = {
        "storage": {
            "provider": "huggingface",
            "repo_size_limit_bytes": 1000,
            "repositories": [
                {
                    "repo_id": "user/repo-001",
                    "repo_sequence": 1,
                    "role": "active",
                    "token_env_var": "HF_TOKEN_REPO_1",
                },
                {
                    "repo_id": "user/repo-002",
                    "repo_sequence": 2,
                    "role": "standby",
                    "token_env_var": "HF_TOKEN_REPO_2",
                },
            ],
        }
    }
    api = FakeHfApi(repo_sizes={"user/repo-001": 850, "user/repo-002": 100})

    plan = build_parquet_tree_upload_plan(
        input_root=root,
        config=config,
        remote_prefix="remote",
        api=api,
    )

    assert plan.repo_id == "user/repo-002"
    assert plan.repo_sequence == 2
    assert plan.total_size_bytes == 300
    assert [call["repo_id"] for call in api.repo_info_calls] == [
        "user/repo-001",
        "user/repo-002",
    ]


def test_upload_parquet_tree_uploads_files_updates_manifest_and_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "derived"
    parquet_path = root / "bars" / "timeframe=1m" / "part.parquet"
    parquet_path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "symbol": ["ES"],
            "contract": ["ESU26-CME"],
            "timestamp_utc": [None],
            "timestamp_ny": [None],
            "session_date": [None],
        }
    )
    pq.write_table(table, parquet_path)
    manifest_path = tmp_path / "metadata" / "manifest.parquet"
    registry_path = tmp_path / "metadata" / "repository_registry.parquet"
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "token")
    config = {
        "dataset": {"symbol": "ES", "default_contract": "ESU26-CME"},
        "storage": {
            "provider": "huggingface",
            "active_repo_id": "karelix/orderflow-es-001",
            "repo_size_limit_bytes": 1000000,
            "metadata_remote_prefix": "metadata",
            "repositories": [
                {
                    "repo_id": "karelix/orderflow-es-001",
                    "repo_sequence": 1,
                    "role": "active",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
                }
            ],
        },
        "paths": {
            "manifest_path": str(manifest_path),
            "repository_registry_path": str(registry_path),
        },
    }
    api = FakeHfApi(repo_sizes={"karelix/orderflow-es-001": 0})

    result = upload_parquet_tree_to_hf(
        input_root=root,
        config=config,
        remote_prefix="samples/test",
        api=api,
    )

    assert result.dry_run is False
    assert result.plan.repo_id == "karelix/orderflow-es-001"
    assert len(result.uploaded_files) == 1
    assert result.uploaded_files[0].path_in_repo == (
        "samples/test/bars/timeframe=1m/part.parquet"
    )
    assert result.manifest_update is not None
    assert result.manifest_update.manifest_path.exists()
    assert result.manifest_update.repository_registry_path.exists()
    assert len(result.manifest_update.manifest_entries) == 1
    assert result.manifest_update.manifest_entries[0].data_tier == "sample"
    assert [item["path_in_repo"] for item in api.uploaded] == [
        "samples/test/bars/timeframe=1m/part.parquet",
        "metadata/manifest.parquet",
        "metadata/repository_registry.parquet",
    ]
    assert len(api.commits) == 2
    assert [len(commit["operations"]) for commit in api.commits] == [1, 2]
    assert len(result.uploaded_metadata_files) == 2


def test_upload_parquet_files_uploads_selected_files_and_updates_manifest(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "derived"
    selected_path = root / "bars" / "timeframe=1h" / "part.parquet"
    skipped_path = root / "volume_profiles" / "part.parquet"
    selected_path.parent.mkdir(parents=True)
    skipped_path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "symbol": ["ES"],
            "contract": ["ESU26-CME"],
            "timestamp_utc": [None],
            "timestamp_ny": [None],
            "session_date": [None],
        }
    )
    pq.write_table(table, selected_path)
    pq.write_table(table, skipped_path)
    manifest_path = tmp_path / "metadata" / "manifest.parquet"
    registry_path = tmp_path / "metadata" / "repository_registry.parquet"
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "token")
    config = {
        "dataset": {"symbol": "ES", "default_contract": "ESU26-CME"},
        "storage": {
            "provider": "huggingface",
            "active_repo_id": "karelix/orderflow-es-001",
            "repo_size_limit_bytes": 1000000,
            "metadata_remote_prefix": "metadata",
            "repositories": [
                {
                    "repo_id": "karelix/orderflow-es-001",
                    "repo_sequence": 1,
                    "role": "active",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
                }
            ],
        },
        "paths": {
            "manifest_path": str(manifest_path),
            "repository_registry_path": str(registry_path),
        },
    }
    api = FakeHfApi(repo_sizes={"karelix/orderflow-es-001": 0})

    result = upload_parquet_files_to_hf(
        input_root=root,
        parquet_files=[selected_path],
        config=config,
        remote_prefix="main/ESU26-CME/test",
        api=api,
    )

    assert result.dry_run is False
    assert [file.path_in_repo for file in result.uploaded_files] == [
        "main/ESU26-CME/test/bars/timeframe=1h/part.parquet"
    ]
    assert result.manifest_update is not None
    assert len(result.manifest_update.new_entries) == 1
    assert result.manifest_update.new_entries[0].data_tier == "test"
    assert result.manifest_update.new_entries[0].dataset_type == "bars"
    assert [item["path_in_repo"] for item in api.uploaded] == [
        "main/ESU26-CME/test/bars/timeframe=1h/part.parquet",
        "metadata/manifest.parquet",
        "metadata/repository_registry.parquet",
    ]
    assert len(api.commits) == 2
    assert [len(commit["operations"]) for commit in api.commits] == [1, 2]


def test_upload_parquet_files_retries_transient_upload_failure(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "derived"
    parquet_path = root / "bars" / "timeframe=1h" / "part.parquet"
    parquet_path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "symbol": ["ES"],
            "contract": ["ESU26-CME"],
            "timestamp_utc": [None],
            "timestamp_ny": [None],
            "session_date": [None],
        }
    )
    pq.write_table(table, parquet_path)
    manifest_path = tmp_path / "metadata" / "manifest.parquet"
    registry_path = tmp_path / "metadata" / "repository_registry.parquet"
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "token")
    monkeypatch.setattr("src.storage.huggingface_tree.time.sleep", lambda _: None)
    config = {
        "dataset": {"symbol": "ES", "default_contract": "ESU26-CME"},
        "storage": {
            "provider": "huggingface",
            "active_repo_id": "karelix/orderflow-es-001",
            "repo_size_limit_bytes": 1000000,
            "metadata_remote_prefix": "metadata",
            "repositories": [
                {
                    "repo_id": "karelix/orderflow-es-001",
                    "repo_sequence": 1,
                    "role": "active",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
                }
            ],
        },
        "paths": {
            "manifest_path": str(manifest_path),
            "repository_registry_path": str(registry_path),
        },
    }
    api = FlakyUploadHfApi(repo_sizes={"karelix/orderflow-es-001": 0})

    result = upload_parquet_files_to_hf(
        input_root=root,
        parquet_files=[parquet_path],
        config=config,
        remote_prefix="main/ESU26-CME",
        api=api,
    )

    assert api.remaining_upload_failures == 0
    assert result.uploaded_files[0].path_in_repo == (
        "main/ESU26-CME/bars/timeframe=1h/part.parquet"
    )
    assert len(api.commits) == 2


def test_upload_parquet_files_batches_multiple_files_into_one_parquet_commit(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "derived"
    first_path = root / "bars" / "timeframe=1h" / "part.parquet"
    second_path = root / "volume_profiles" / "part.parquet"
    first_path.parent.mkdir(parents=True)
    second_path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "symbol": ["ES"],
            "contract": ["ESU26-CME"],
            "timestamp_utc": [None],
            "timestamp_ny": [None],
            "session_date": [None],
        }
    )
    pq.write_table(table, first_path)
    pq.write_table(table, second_path)
    manifest_path = tmp_path / "metadata" / "manifest.parquet"
    registry_path = tmp_path / "metadata" / "repository_registry.parquet"
    monkeypatch.setenv("HF_TOKEN_ORDERFLOW_ES_001", "token")
    config = {
        "dataset": {"symbol": "ES", "default_contract": "ESU26-CME"},
        "storage": {
            "provider": "huggingface",
            "active_repo_id": "karelix/orderflow-es-001",
            "repo_size_limit_bytes": 1000000,
            "metadata_remote_prefix": "metadata",
            "repositories": [
                {
                    "repo_id": "karelix/orderflow-es-001",
                    "repo_sequence": 1,
                    "role": "active",
                    "token_env_var": "HF_TOKEN_ORDERFLOW_ES_001",
                }
            ],
        },
        "paths": {
            "manifest_path": str(manifest_path),
            "repository_registry_path": str(registry_path),
        },
    }
    api = FakeHfApi(repo_sizes={"karelix/orderflow-es-001": 0})

    result = upload_parquet_files_to_hf(
        input_root=root,
        parquet_files=[first_path, second_path],
        config=config,
        remote_prefix="main/ESU26-CME",
        api=api,
    )

    assert len(result.uploaded_files) == 2
    assert len(api.commits) == 2
    assert [len(commit["operations"]) for commit in api.commits] == [2, 2]
