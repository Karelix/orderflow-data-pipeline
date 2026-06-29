"""Upload tiny dataset metadata files to Hugging Face."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class UploadedMetadataFile:
    """One metadata file uploaded to Hugging Face."""

    local_path: Path
    path_in_repo: str
    repo_id: str


def upload_metadata_files(
    repo_id: str,
    manifest_path: str | Path,
    repository_registry_path: str | Path,
    remote_prefix: str = "metadata",
    token: str | None = None,
    create_repo: bool = False,
    private: bool = True,
    api: object | None = None,
) -> list[UploadedMetadataFile]:
    """Upload the manifest and repository registry to a Hugging Face dataset repo."""
    selected_api = api or _create_hf_api(token=token)
    prefix = remote_prefix.strip("/")
    files = [
        (Path(manifest_path), _join_remote(prefix, "manifest.parquet")),
        (
            Path(repository_registry_path),
            _join_remote(prefix, "repository_registry.parquet"),
        ),
    ]

    if create_repo:
        selected_api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True,
            token=token,
        )

    uploaded: list[UploadedMetadataFile] = []

    for local_path, path_in_repo in files:
        selected_api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
        )
        uploaded.append(
            UploadedMetadataFile(
                local_path=local_path,
                path_in_repo=path_in_repo,
                repo_id=repo_id,
            )
        )

    return uploaded


def _create_hf_api(token: str | None) -> object:
    try:
        from huggingface_hub import HfApi
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required to upload metadata. "
            "Install or update the conda environment from environment.yml."
        ) from exc

    return HfApi(token=token)


def _join_remote(prefix: str, filename: str) -> str:
    if not prefix:
        return filename

    return f"{prefix}/{filename}"
