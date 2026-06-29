"""Build manifest and repository registry files for the local derived sample."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.manifest import (
    build_manifest_for_parquet_tree,
    build_repository_registry,
    write_manifest_parquet,
    write_repository_registry_parquet,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input-root", default="data_local/tmp/derived_sample")
    parser.add_argument("--manifest-output", default="data_local/tmp/derived_sample_manifest.parquet")
    parser.add_argument(
        "--registry-output",
        default="data_local/tmp/derived_sample_repository_registry.parquet",
    )
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--repo-sequence", type=int, default=None)
    parser.add_argument("--remote-prefix", default="samples/derived_sample")
    parser.add_argument(
        "--data-tier",
        default=None,
        help="Manifest tier. Defaults to inference from remote prefix.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    entries = build_manifest_for_parquet_tree(
        root=args.input_root,
        config=config,
        repo_id=args.repo_id,
        repo_sequence=args.repo_sequence,
        remote_prefix=args.remote_prefix,
        data_tier=args.data_tier,
    )
    repositories = build_repository_registry(entries, config)

    write_manifest_parquet(entries, args.manifest_output)
    write_repository_registry_parquet(repositories, args.registry_output)

    print(f"Manifest rows: {len(entries)}")
    print(f"Repository rows: {len(repositories)}")
    print(f"Manifest: {args.manifest_output}")
    print(f"Repository registry: {args.registry_output}")
    for entry in entries[:5]:
        print(f"{entry.dataset_type} | repo={entry.repo_id} | remote={entry.remote_path}")


if __name__ == "__main__":
    main()
