"""Persistent local manifest updates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.manifest.manifest import (
    ManifestEntry,
    RepositoryEntry,
    build_manifest_for_parquet_files,
    build_manifest_for_parquet_tree,
    build_repository_registry,
    merge_manifest_entries,
    read_manifest_parquet,
    write_manifest_parquet,
    write_repository_registry_parquet,
)


@dataclass(frozen=True)
class ManifestUpdateResult:
    """Result of recording an uploaded parquet tree in local metadata."""

    new_entries: list[ManifestEntry]
    manifest_entries: list[ManifestEntry]
    repository_entries: list[RepositoryEntry]
    manifest_path: Path
    repository_registry_path: Path


def update_local_manifest_after_upload(
    uploaded_root: str | Path,
    config: Mapping[str, Any],
    repo_id: str | None = None,
    repo_sequence: int | None = None,
    remote_prefix: str = "",
    validation_status: str = "validated",
    data_tier: str | None = None,
    manifest_path: str | Path | None = None,
    repository_registry_path: str | Path | None = None,
) -> ManifestUpdateResult:
    """Record a successfully uploaded parquet tree in the persistent manifest."""
    selected_manifest_path = Path(manifest_path or config["paths"]["manifest_path"])
    selected_registry_path = Path(
        repository_registry_path or config["paths"]["repository_registry_path"]
    )
    existing_entries = _read_manifest_if_exists(selected_manifest_path)
    new_entries = build_manifest_for_parquet_tree(
        root=uploaded_root,
        config=config,
        repo_id=repo_id,
        repo_sequence=repo_sequence,
        remote_prefix=remote_prefix,
        validation_status=validation_status,
        data_tier=data_tier,
    )
    manifest_entries = merge_manifest_entries(existing_entries, new_entries)
    repository_entries = build_repository_registry(manifest_entries, config)

    write_manifest_parquet(manifest_entries, selected_manifest_path)
    write_repository_registry_parquet(repository_entries, selected_registry_path)

    return ManifestUpdateResult(
        new_entries=new_entries,
        manifest_entries=manifest_entries,
        repository_entries=repository_entries,
        manifest_path=selected_manifest_path,
        repository_registry_path=selected_registry_path,
    )


def update_local_manifest_after_file_upload(
    uploaded_root: str | Path,
    parquet_files: list[str | Path],
    config: Mapping[str, Any],
    repo_id: str | None = None,
    repo_sequence: int | None = None,
    remote_prefix: str = "",
    validation_status: str = "validated",
    data_tier: str | None = None,
    manifest_path: str | Path | None = None,
    repository_registry_path: str | Path | None = None,
) -> ManifestUpdateResult:
    """Record successfully uploaded explicit Parquet files in local metadata."""
    selected_manifest_path = Path(manifest_path or config["paths"]["manifest_path"])
    selected_registry_path = Path(
        repository_registry_path or config["paths"]["repository_registry_path"]
    )
    existing_entries = _read_manifest_if_exists(selected_manifest_path)
    new_entries = build_manifest_for_parquet_files(
        root=uploaded_root,
        parquet_files=parquet_files,
        config=config,
        repo_id=repo_id,
        repo_sequence=repo_sequence,
        remote_prefix=remote_prefix,
        validation_status=validation_status,
        data_tier=data_tier,
    )
    manifest_entries = merge_manifest_entries(existing_entries, new_entries)
    repository_entries = build_repository_registry(manifest_entries, config)

    write_manifest_parquet(manifest_entries, selected_manifest_path)
    write_repository_registry_parquet(repository_entries, selected_registry_path)

    return ManifestUpdateResult(
        new_entries=new_entries,
        manifest_entries=manifest_entries,
        repository_entries=repository_entries,
        manifest_path=selected_manifest_path,
        repository_registry_path=selected_registry_path,
    )


def _read_manifest_if_exists(path: Path) -> list[ManifestEntry]:
    if not path.exists():
        return []

    return read_manifest_parquet(path)
