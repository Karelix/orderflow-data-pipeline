"""Update local metadata after a parquet tree has been uploaded."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.manifest import update_local_manifest_after_upload
from src.storage import resolve_repo_token, upload_metadata_files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--uploaded-root", required=True)
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--repo-sequence", type=int, default=None)
    parser.add_argument("--remote-prefix", default="")
    parser.add_argument("--validation-status", default="validated")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--registry-path", default=None)
    parser.add_argument("--upload-metadata", action="store_true")
    parser.add_argument("--metadata-repo-id", default=None)
    parser.add_argument("--metadata-remote-prefix", default=None)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--hf-token", default=None)
    parser.add_argument("--hf-token-env-var", default=None)
    parser.add_argument("--create-repo", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    result = update_local_manifest_after_upload(
        uploaded_root=args.uploaded_root,
        config=config,
        repo_id=args.repo_id,
        repo_sequence=args.repo_sequence,
        remote_prefix=args.remote_prefix,
        validation_status=args.validation_status,
        manifest_path=args.manifest_path,
        repository_registry_path=args.registry_path,
    )

    print(f"New manifest rows: {len(result.new_entries)}")
    print(f"Total manifest rows: {len(result.manifest_entries)}")
    print(f"Repository rows: {len(result.repository_entries)}")
    print(f"Manifest: {result.manifest_path}")
    print(f"Repository registry: {result.repository_registry_path}")

    for repository in result.repository_entries:
        print(
            " | ".join(
                [
                    f"repo={repository.repo_id}",
                    f"seq={repository.repo_sequence}",
                    f"role={repository.role}",
                    f"manifest_size={repository.current_manifest_size_bytes}",
                    f"limit={repository.size_limit_bytes}",
                    f"dates={_format_range(repository.first_session_date, repository.last_session_date)}",
                    f"types={repository.dataset_types or ''}",
                ]
            )
        )

    if args.upload_metadata:
        storage = config.get("storage", {})
        metadata_repo_id = args.metadata_repo_id or args.repo_id or storage["active_repo_id"]
        remote_prefix = (
            args.metadata_remote_prefix
            or storage.get("metadata_remote_prefix")
            or "metadata"
        )
        token = args.hf_token or resolve_repo_token(
            config=config,
            repo_id=metadata_repo_id,
            token_env_var=args.hf_token_env_var,
            env_file=args.env_file,
            required=True,
        )
        uploaded = upload_metadata_files(
            repo_id=metadata_repo_id,
            manifest_path=result.manifest_path,
            repository_registry_path=result.repository_registry_path,
            remote_prefix=remote_prefix,
            token=token,
            create_repo=args.create_repo,
        )

        for uploaded_file in uploaded:
            print(
                f"Uploaded metadata: repo={uploaded_file.repo_id} "
                f"path={uploaded_file.path_in_repo}"
            )

    return 0


def _format_range(start: object | None, end: object | None) -> str:
    if start is None or end is None:
        return ""

    if start == end:
        return str(start)

    return f"{start}..{end}"


if __name__ == "__main__":
    raise SystemExit(main())
