from src.storage import upload_metadata_files


class FakeHfApi:
    def __init__(self) -> None:
        self.created = []
        self.uploaded = []
        self.commits = []

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


class FlakyHfApi(FakeHfApi):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_failures = 1

    def create_commit(self, **kwargs) -> None:
        if self.remaining_failures:
            self.remaining_failures -= 1
            raise RuntimeError("temporary gateway timeout")

        super().create_commit(**kwargs)


def test_upload_metadata_files_uses_metadata_paths(tmp_path) -> None:
    manifest_path = tmp_path / "manifest.parquet"
    registry_path = tmp_path / "repository_registry.parquet"
    manifest_path.write_bytes(b"manifest")
    registry_path.write_bytes(b"registry")
    api = FakeHfApi()

    uploaded = upload_metadata_files(
        repo_id="user/orderflow-es-001",
        manifest_path=manifest_path,
        repository_registry_path=registry_path,
        remote_prefix="metadata",
        token="token",
        create_repo=True,
        api=api,
    )

    assert api.created[0]["repo_id"] == "user/orderflow-es-001"
    assert api.created[0]["repo_type"] == "dataset"
    assert [item["path_in_repo"] for item in api.uploaded] == [
        "metadata/manifest.parquet",
        "metadata/repository_registry.parquet",
    ]
    assert len(api.commits) == 1
    assert len(api.commits[0]["operations"]) == 2
    assert [item.path_in_repo for item in uploaded] == [
        "metadata/manifest.parquet",
        "metadata/repository_registry.parquet",
    ]


def test_upload_metadata_files_retries_transient_upload_failure(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "manifest.parquet"
    registry_path = tmp_path / "repository_registry.parquet"
    manifest_path.write_bytes(b"manifest")
    registry_path.write_bytes(b"registry")
    api = FlakyHfApi()
    monkeypatch.setattr("src.storage.huggingface_metadata.time.sleep", lambda _: None)

    uploaded = upload_metadata_files(
        repo_id="user/orderflow-es-001",
        manifest_path=manifest_path,
        repository_registry_path=registry_path,
        remote_prefix="metadata",
        token="token",
        api=api,
    )

    assert api.remaining_failures == 0
    assert len(api.uploaded) == 2
    assert len(api.commits) == 1
    assert len(uploaded) == 2
