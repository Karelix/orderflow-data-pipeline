"""Upload the local manifest and repository registry to Hugging Face."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.storage import resolve_repo_token, upload_metadata_files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--registry-path", default=None)
    parser.add_argument("--metadata-remote-prefix", default=None)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--hf-token-env-var", default=None)
    parser.add_argument("--create-repo", action="store_true")
    parser.add_argument("--public", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    storage = config.get("storage", {})
    paths = config.get("paths", {})

    repo_id = args.repo_id or storage["active_repo_id"]
    manifest_path = Path(args.manifest_path or paths["manifest_path"])
    registry_path = Path(args.registry_path or paths["repository_registry_path"])
    remote_prefix = (
        args.metadata_remote_prefix
        or storage.get("metadata_remote_prefix")
        or "metadata"
    )

    missing_paths = [
        path
        for path in (manifest_path, registry_path)
        if not path.exists()
    ]
    if missing_paths:
        for path in missing_paths:
            print(f"Metadata file not found: {path}")
        return 1

    token = resolve_repo_token(
        config=config,
        repo_id=repo_id,
        token_env_var=args.hf_token_env_var,
        env_file=args.env_file,
        required=True,
    )
    uploaded = upload_metadata_files(
        repo_id=repo_id,
        manifest_path=manifest_path,
        repository_registry_path=registry_path,
        remote_prefix=remote_prefix,
        token=token,
        create_repo=args.create_repo,
        private=not args.public,
    )

    print(f"Uploaded metadata files: {len(uploaded)}")
    for uploaded_file in uploaded:
        print(
            " | ".join(
                [
                    f"repo={uploaded_file.repo_id}",
                    f"local={uploaded_file.local_path}",
                    f"remote={uploaded_file.path_in_repo}",
                ]
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
