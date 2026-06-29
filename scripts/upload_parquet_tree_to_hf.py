"""Upload a local Parquet tree to Hugging Face and update metadata."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.storage import upload_parquet_tree_to_hf


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--remote-prefix", required=True)
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--create-repo", action="store_true")
    parser.add_argument("--public", action="store_true")
    parser.add_argument("--skip-metadata-upload", action="store_true")
    parser.add_argument("--metadata-repo-id", default=None)
    parser.add_argument("--metadata-remote-prefix", default=None)
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--registry-path", default=None)
    parser.add_argument("--validation-status", default="validated")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-remote-size-check", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    result = upload_parquet_tree_to_hf(
        input_root=args.input_root,
        config=config,
        remote_prefix=args.remote_prefix,
        repo_id=args.repo_id,
        env_file=args.env_file,
        create_repo=args.create_repo,
        private=not args.public,
        upload_metadata=not args.skip_metadata_upload,
        metadata_repo_id=args.metadata_repo_id,
        metadata_remote_prefix=args.metadata_remote_prefix,
        manifest_path=args.manifest_path,
        repository_registry_path=args.registry_path,
        validation_status=args.validation_status,
        dry_run=args.dry_run,
        skip_remote_size_check=args.skip_remote_size_check,
    )

    plan = result.plan
    capacity = plan.repository_capacity
    print(f"Mode: {'dry-run' if result.dry_run else 'upload'}")
    print(f"Input root: {plan.input_root}")
    print(f"Remote prefix: {plan.remote_prefix}")
    print(f"Selected repo: {plan.repo_id}")
    print(f"Repo sequence: {plan.repo_sequence}")
    print(f"Parquet files: {len(plan.files)}")
    print(f"Upload bytes: {plan.total_size_bytes}")
    print(f"Repo current bytes: {capacity.current_size_bytes}")
    print(f"Repo remaining bytes before upload: {capacity.remaining_size_bytes}")

    for file in plan.files[:10]:
        print(f"plan | {file.local_path} -> {file.path_in_repo} | bytes={file.size_bytes}")

    if len(plan.files) > 10:
        print(f"plan | ... {len(plan.files) - 10} more files")

    if result.dry_run:
        return 0

    print(f"Uploaded Parquet files: {len(result.uploaded_files)}")

    if result.manifest_update is not None:
        print(f"Manifest: {result.manifest_update.manifest_path}")
        print(f"Repository registry: {result.manifest_update.repository_registry_path}")
        print(f"Total manifest rows: {len(result.manifest_update.manifest_entries)}")

    print(f"Uploaded metadata files: {len(result.uploaded_metadata_files)}")

    for metadata_file in result.uploaded_metadata_files:
        print(
            f"metadata | repo={metadata_file.repo_id} "
            f"path={metadata_file.path_in_repo}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
