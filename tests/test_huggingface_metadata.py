from src.storage import upload_metadata_files


class FakeHfApi:
    def __init__(self) -> None:
        self.created = []
        self.uploaded = []

    def create_repo(self, **kwargs) -> None:
        self.created.append(kwargs)

    def upload_file(self, **kwargs) -> None:
        self.uploaded.append(kwargs)


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
    assert [item.path_in_repo for item in uploaded] == [
        "metadata/manifest.parquet",
        "metadata/repository_registry.parquet",
    ]
