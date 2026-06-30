"""Upload local Parquet trees to Hugging Face dataset repositories."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.manifest.local_store import (
    ManifestUpdateResult,
    update_local_manifest_after_file_upload,
    update_local_manifest_after_upload,
)
from src.storage.huggingface_metadata import UploadedMetadataFile, upload_metadata_files
from src.storage.secrets import resolve_repo_token


@dataclass(frozen=True)
class ParquetTreeFile:
    """One local Parquet file and its target path inside a repo."""

    local_path: Path
    relative_path: Path
    path_in_repo: str
    size_bytes: int


@dataclass(frozen=True)
class RepositoryCapacity:
    """Current capacity state for one Hugging Face repo."""

    repo_id: str
    repo_sequence: int
    role: str
    size_limit_bytes: int
    current_size_bytes: int
    remaining_size_bytes: int
    token_env_var: str | None


@dataclass(frozen=True)
class ParquetTreeUploadPlan:
    """A selected repo and the files that will be uploaded to it."""

    input_root: Path
    repo_id: str
    repo_sequence: int
    remote_prefix: str
    total_size_bytes: int
    files: list[ParquetTreeFile]
    repository_capacity: RepositoryCapacity


@dataclass(frozen=True)
class UploadedParquetFile:
    """One Parquet file uploaded to Hugging Face."""

    local_path: Path
    path_in_repo: str
    repo_id: str
    size_bytes: int


@dataclass(frozen=True)
class ParquetTreeUploadResult:
    """Result of uploading a Parquet tree and updating metadata."""

    plan: ParquetTreeUploadPlan
    uploaded_files: list[UploadedParquetFile]
    manifest_update: ManifestUpdateResult | None
    uploaded_metadata_files: list[UploadedMetadataFile]
    dry_run: bool


def collect_parquet_tree_files(
    input_root: str | Path,
    remote_prefix: str = "",
) -> list[ParquetTreeFile]:
    """Collect Parquet files under a root and map them to repo paths."""
    root = Path(input_root)

    if not root.exists():
        raise FileNotFoundError(f"Input root not found: {root}")

    if not root.is_dir():
        raise NotADirectoryError(f"Input root is not a directory: {root}")

    files = []

    for local_path in sorted(root.rglob("*.parquet")):
        relative_path = local_path.relative_to(root)
        files.append(
            ParquetTreeFile(
                local_path=local_path,
                relative_path=relative_path,
                path_in_repo=_join_remote_path(remote_prefix, relative_path),
                size_bytes=local_path.stat().st_size,
            )
        )

    if not files:
        raise ValueError(f"No Parquet files found under: {root}")

    return files


def collect_parquet_files(
    input_root: str | Path,
    parquet_files: list[str | Path],
    remote_prefix: str = "",
) -> list[ParquetTreeFile]:
    """Collect explicit Parquet files under a root and map them to repo paths."""
    root = Path(input_root)

    if not root.exists():
        raise FileNotFoundError(f"Input root not found: {root}")

    if not root.is_dir():
        raise NotADirectoryError(f"Input root is not a directory: {root}")

    files = []
    resolved_root = root.resolve()

    for parquet_file in sorted(Path(path) for path in parquet_files):
        local_path = parquet_file

        if not local_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {local_path}")

        if not local_path.is_file():
            raise ValueError(f"Expected a Parquet file, got: {local_path}")

        if local_path.suffix != ".parquet":
            raise ValueError(f"Expected a .parquet file, got: {local_path}")

        relative_path = local_path.resolve().relative_to(resolved_root)
        files.append(
            ParquetTreeFile(
                local_path=local_path,
                relative_path=relative_path,
                path_in_repo=_join_remote_path(remote_prefix, relative_path),
                size_bytes=local_path.stat().st_size,
            )
        )

    if not files:
        raise ValueError("At least one Parquet file is required.")

    return files


def build_parquet_tree_upload_plan(
    input_root: str | Path,
    config: Mapping[str, Any],
    remote_prefix: str = "",
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_missing_repo: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> ParquetTreeUploadPlan:
    """Build an upload plan and choose a repo with enough remaining capacity."""
    files = collect_parquet_tree_files(input_root=input_root, remote_prefix=remote_prefix)
    total_size_bytes = sum(file.size_bytes for file in files)
    capacity = choose_repository_for_upload(
        config=config,
        required_size_bytes=total_size_bytes,
        repo_id=repo_id,
        env_file=env_file,
        create_missing_repo=create_missing_repo,
        skip_remote_size_check=skip_remote_size_check,
        api=api,
    )

    return ParquetTreeUploadPlan(
        input_root=Path(input_root),
        repo_id=capacity.repo_id,
        repo_sequence=capacity.repo_sequence,
        remote_prefix=remote_prefix,
        total_size_bytes=total_size_bytes,
        files=files,
        repository_capacity=capacity,
    )


def build_parquet_file_upload_plan(
    input_root: str | Path,
    parquet_files: list[str | Path],
    config: Mapping[str, Any],
    remote_prefix: str = "",
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_missing_repo: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> ParquetTreeUploadPlan:
    """Build an upload plan for explicit Parquet files under a root."""
    files = collect_parquet_files(
        input_root=input_root,
        parquet_files=parquet_files,
        remote_prefix=remote_prefix,
    )
    total_size_bytes = sum(file.size_bytes for file in files)
    capacity = choose_repository_for_upload(
        config=config,
        required_size_bytes=total_size_bytes,
        repo_id=repo_id,
        env_file=env_file,
        create_missing_repo=create_missing_repo,
        skip_remote_size_check=skip_remote_size_check,
        api=api,
    )

    return ParquetTreeUploadPlan(
        input_root=Path(input_root),
        repo_id=capacity.repo_id,
        repo_sequence=capacity.repo_sequence,
        remote_prefix=remote_prefix,
        total_size_bytes=total_size_bytes,
        files=files,
        repository_capacity=capacity,
    )


def choose_repository_for_upload(
    config: Mapping[str, Any],
    required_size_bytes: int,
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_missing_repo: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> RepositoryCapacity:
    """Choose the first configured repo that can fit the required bytes."""
    candidates = _configured_repositories(config)

    if repo_id is not None:
        candidates = [repo for repo in candidates if repo["repo_id"] == repo_id]

        if not candidates:
            raise ValueError(f"Repo is not configured in dataset.yaml: {repo_id}")
    else:
        candidates = [
            repo
            for repo in candidates
            if str(repo.get("role", "active")) in {"active", "standby"}
        ]

    if not candidates:
        raise ValueError("No Hugging Face repositories are configured for upload.")

    capacities: list[RepositoryCapacity] = []

    for repo in candidates:
        capacity = get_repository_capacity(
            config=config,
            repo=repo,
            env_file=env_file,
            create_missing_repo=create_missing_repo,
            skip_remote_size_check=skip_remote_size_check,
            api=api,
        )
        capacities.append(capacity)

        if capacity.remaining_size_bytes >= required_size_bytes:
            return capacity

    details = "; ".join(
        f"{capacity.repo_id} remaining={capacity.remaining_size_bytes}"
        for capacity in capacities
    )
    raise RuntimeError(
        f"No configured repo has enough remaining capacity for {required_size_bytes} bytes. "
        f"Checked: {details}"
    )


def get_repository_capacity(
    config: Mapping[str, Any],
    repo: Mapping[str, Any],
    env_file: str | Path | None = None,
    create_missing_repo: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> RepositoryCapacity:
    """Read current repo size and calculate remaining capacity."""
    storage = config["storage"]
    repo_id = str(repo["repo_id"])
    token_env_var = repo.get("token_env_var") or storage.get("default_token_env_var")
    size_limit_bytes = int(repo.get("size_limit_bytes", storage["repo_size_limit_bytes"]))

    if skip_remote_size_check:
        current_size_bytes = 0
    else:
        selected_api = api or _create_hf_api()
        token = resolve_repo_token(
            config=config,
            repo_id=repo_id,
            token_env_var=str(token_env_var) if token_env_var else None,
            env_file=env_file,
            required=True,
        )
        current_size_bytes = get_huggingface_repo_size(
            api=selected_api,
            repo_id=repo_id,
            token=token,
            create_missing_repo=create_missing_repo,
        )

    return RepositoryCapacity(
        repo_id=repo_id,
        repo_sequence=int(repo.get("repo_sequence", 0)),
        role=str(repo.get("role", "active")),
        size_limit_bytes=size_limit_bytes,
        current_size_bytes=current_size_bytes,
        remaining_size_bytes=size_limit_bytes - current_size_bytes,
        token_env_var=str(token_env_var) if token_env_var else None,
    )


def get_huggingface_repo_size(
    api: object,
    repo_id: str,
    token: str,
    create_missing_repo: bool = False,
) -> int:
    """Calculate current repo size from Hugging Face file metadata."""
    try:
        try:
            repo_info = api.repo_info(
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
                files_metadata=True,
            )
        except TypeError:
            repo_info = api.repo_info(
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
    except Exception:
        if create_missing_repo:
            return 0

        raise

    siblings = getattr(repo_info, "siblings", None) or []
    return sum(_sibling_size(sibling) for sibling in siblings)


def upload_parquet_tree_to_hf(
    input_root: str | Path,
    config: Mapping[str, Any],
    remote_prefix: str = "",
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_repo: bool = False,
    private: bool = True,
    upload_metadata: bool = True,
    metadata_repo_id: str | None = None,
    metadata_remote_prefix: str | None = None,
    manifest_path: str | Path | None = None,
    repository_registry_path: str | Path | None = None,
    validation_status: str = "validated",
    data_tier: str | None = None,
    dry_run: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> ParquetTreeUploadResult:
    """Upload a Parquet tree, update local manifest, and upload metadata files."""
    plan = build_parquet_tree_upload_plan(
        input_root=input_root,
        config=config,
        remote_prefix=remote_prefix,
        repo_id=repo_id,
        env_file=env_file,
        create_missing_repo=create_repo,
        skip_remote_size_check=skip_remote_size_check,
        api=api,
    )

    if dry_run:
        return ParquetTreeUploadResult(
            plan=plan,
            uploaded_files=[],
            manifest_update=None,
            uploaded_metadata_files=[],
            dry_run=True,
        )

    selected_api = api or _create_hf_api()
    token = resolve_repo_token(
        config=config,
        repo_id=plan.repo_id,
        token_env_var=plan.repository_capacity.token_env_var,
        env_file=env_file,
        required=True,
    )

    if create_repo:
        selected_api.create_repo(
            repo_id=plan.repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True,
            token=token,
        )

    uploaded_files = _upload_plan_files(
        api=selected_api,
        plan=plan,
        token=token,
    )

    manifest_update = update_local_manifest_after_upload(
        uploaded_root=input_root,
        config=config,
        repo_id=plan.repo_id,
        repo_sequence=plan.repo_sequence,
        remote_prefix=remote_prefix,
        validation_status=validation_status,
        data_tier=data_tier,
        manifest_path=manifest_path,
        repository_registry_path=repository_registry_path,
    )

    uploaded_metadata_files: list[UploadedMetadataFile] = []

    if upload_metadata:
        selected_metadata_repo_id = metadata_repo_id or plan.repo_id
        metadata_token = resolve_repo_token(
            config=config,
            repo_id=selected_metadata_repo_id,
            env_file=env_file,
            required=True,
        )
        uploaded_metadata_files = upload_metadata_files(
            repo_id=selected_metadata_repo_id,
            manifest_path=manifest_update.manifest_path,
            repository_registry_path=manifest_update.repository_registry_path,
            remote_prefix=(
                metadata_remote_prefix
                or config["storage"].get("metadata_remote_prefix")
                or "metadata"
            ),
            token=metadata_token,
            create_repo=False,
            api=selected_api,
        )

    return ParquetTreeUploadResult(
        plan=plan,
        uploaded_files=uploaded_files,
        manifest_update=manifest_update,
        uploaded_metadata_files=uploaded_metadata_files,
        dry_run=False,
    )


def upload_parquet_files_to_hf(
    input_root: str | Path,
    parquet_files: list[str | Path],
    config: Mapping[str, Any],
    remote_prefix: str = "",
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_repo: bool = False,
    private: bool = True,
    upload_metadata: bool = True,
    metadata_repo_id: str | None = None,
    metadata_remote_prefix: str | None = None,
    manifest_path: str | Path | None = None,
    repository_registry_path: str | Path | None = None,
    validation_status: str = "validated",
    data_tier: str | None = None,
    dry_run: bool = False,
    skip_remote_size_check: bool = False,
    api: object | None = None,
) -> ParquetTreeUploadResult:
    """Upload explicit Parquet files, update local manifest, and upload metadata."""
    plan = build_parquet_file_upload_plan(
        input_root=input_root,
        parquet_files=parquet_files,
        config=config,
        remote_prefix=remote_prefix,
        repo_id=repo_id,
        env_file=env_file,
        create_missing_repo=create_repo,
        skip_remote_size_check=skip_remote_size_check,
        api=api,
    )

    if dry_run:
        return ParquetTreeUploadResult(
            plan=plan,
            uploaded_files=[],
            manifest_update=None,
            uploaded_metadata_files=[],
            dry_run=True,
        )

    selected_api = api or _create_hf_api()
    token = resolve_repo_token(
        config=config,
        repo_id=plan.repo_id,
        token_env_var=plan.repository_capacity.token_env_var,
        env_file=env_file,
        required=True,
    )

    if create_repo:
        selected_api.create_repo(
            repo_id=plan.repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True,
            token=token,
        )

    uploaded_files = _upload_plan_files(
        api=selected_api,
        plan=plan,
        token=token,
    )
    manifest_update = update_local_manifest_after_file_upload(
        uploaded_root=input_root,
        parquet_files=[file.local_path for file in plan.files],
        config=config,
        repo_id=plan.repo_id,
        repo_sequence=plan.repo_sequence,
        remote_prefix=remote_prefix,
        validation_status=validation_status,
        data_tier=data_tier,
        manifest_path=manifest_path,
        repository_registry_path=repository_registry_path,
    )
    uploaded_metadata_files = _upload_metadata_if_requested(
        upload_metadata=upload_metadata,
        api=selected_api,
        config=config,
        env_file=env_file,
        plan=plan,
        manifest_update=manifest_update,
        metadata_repo_id=metadata_repo_id,
        metadata_remote_prefix=metadata_remote_prefix,
    )

    return ParquetTreeUploadResult(
        plan=plan,
        uploaded_files=uploaded_files,
        manifest_update=manifest_update,
        uploaded_metadata_files=uploaded_metadata_files,
        dry_run=False,
    )


def _upload_plan_files(
    api: object,
    plan: ParquetTreeUploadPlan,
    token: str,
) -> list[UploadedParquetFile]:
    _commit_plan_files_with_retries(
        api=api,
        plan=plan,
        token=token,
    )

    return [
        UploadedParquetFile(
            local_path=file.local_path,
            path_in_repo=file.path_in_repo,
            repo_id=plan.repo_id,
            size_bytes=file.size_bytes,
        )
        for file in plan.files
    ]


def _commit_plan_files_with_retries(
    api: object,
    plan: ParquetTreeUploadPlan,
    token: str,
    max_attempts: int = 5,
    initial_delay_seconds: float = 5.0,
) -> None:
    operations = _commit_operations(plan.files)

    for attempt in range(1, max_attempts + 1):
        try:
            api.create_commit(
                repo_id=plan.repo_id,
                operations=operations,
                commit_message="Upload Parquet dataset batch",
                repo_type="dataset",
                token=token,
            )
            return
        except Exception:
            if attempt == max_attempts:
                raise

            time.sleep(initial_delay_seconds * (2 ** (attempt - 1)))


def _commit_operations(files: list[ParquetTreeFile]) -> list[object]:
    try:
        from huggingface_hub import CommitOperationAdd
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required to upload Parquet files. "
            "Install or update the conda environment from environment.yml."
        ) from exc

    return [
        CommitOperationAdd(
            path_in_repo=file.path_in_repo,
            path_or_fileobj=str(file.local_path),
        )
        for file in files
    ]


def _upload_metadata_if_requested(
    upload_metadata: bool,
    api: object,
    config: Mapping[str, Any],
    env_file: str | Path | None,
    plan: ParquetTreeUploadPlan,
    manifest_update: ManifestUpdateResult,
    metadata_repo_id: str | None,
    metadata_remote_prefix: str | None,
) -> list[UploadedMetadataFile]:
    if not upload_metadata:
        return []

    selected_metadata_repo_id = metadata_repo_id or plan.repo_id
    metadata_token = resolve_repo_token(
        config=config,
        repo_id=selected_metadata_repo_id,
        env_file=env_file,
        required=True,
    )
    return upload_metadata_files(
        repo_id=selected_metadata_repo_id,
        manifest_path=manifest_update.manifest_path,
        repository_registry_path=manifest_update.repository_registry_path,
        remote_prefix=(
            metadata_remote_prefix
            or config["storage"].get("metadata_remote_prefix")
            or "metadata"
        ),
        token=metadata_token,
        create_repo=False,
        api=api,
    )


def _configured_repositories(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    storage = config["storage"]
    repositories = list(storage.get("repositories", []))

    if not repositories and storage.get("active_repo_id"):
        repositories = [
            {
                "repo_id": storage["active_repo_id"],
                "repo_sequence": 1,
                "role": "active",
                "token_env_var": storage.get("default_token_env_var"),
            }
        ]

    return sorted(repositories, key=lambda repo: int(repo.get("repo_sequence", 0)))


def _create_hf_api() -> object:
    try:
        from huggingface_hub import HfApi
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required to upload Parquet files. "
            "Install or update the conda environment from environment.yml."
        ) from exc

    return HfApi()


def _join_remote_path(prefix: str, relative_path: Path) -> str:
    remote_parts = [part for part in prefix.strip("/").split("/") if part]
    remote_parts.extend(relative_path.parts)
    return "/".join(remote_parts).replace("\\", "/")


def _sibling_size(sibling: object) -> int:
    if isinstance(sibling, Mapping):
        size = sibling.get("size")
    else:
        size = getattr(sibling, "size", None)

    return int(size or 0)
