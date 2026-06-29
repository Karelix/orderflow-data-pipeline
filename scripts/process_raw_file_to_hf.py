"""Validate a raw file, write partitioned datasets, and upload to Hugging Face."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.pipeline import RawValidationFailedError, process_raw_file_to_hf
from src.validation.raw_stream import RawStreamChunkSummary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/dataset.yaml")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output-root", default="data_local/tmp/process_raw_to_hf")
    parser.add_argument("--remote-prefix", required=True)
    parser.add_argument("--validation-chunk-size", type=int, default=100000)
    parser.add_argument("--build-chunk-size", type=int, default=None)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--timeframe",
        action="append",
        default=None,
        help="Timeframe to build. Can be repeated. Defaults to config timeframes.",
    )
    parser.add_argument("--max-out-of-order-timestamps", type=int, default=None)
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--create-repo", action="store_true")
    parser.add_argument("--public", action="store_true")
    parser.add_argument("--skip-metadata-upload", action="store_true")
    parser.add_argument("--metadata-repo-id", default=None)
    parser.add_argument("--metadata-remote-prefix", default=None)
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--registry-path", default=None)
    parser.add_argument("--dry-run-upload", action="store_true")
    parser.add_argument("--skip-remote-size-check", action="store_true")
    parser.add_argument("--cleanup-output-after-upload", action="store_true")
    parser.add_argument(
        "--data-tier",
        default=None,
        help="Manifest tier for this upload. Defaults to inference from remote prefix.",
    )
    parser.add_argument("--quiet-validation", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    input_path = args.input or config["raw_data"]["sample_file"]

    def report_validation_progress(summary: RawStreamChunkSummary) -> None:
        if args.quiet_validation:
            return

        print(
            "validation chunk={chunk} rows={rows} first={first} last={last} volume={volume} delta={delta}".format(
                chunk=summary.chunk_index,
                rows=summary.rows,
                first=summary.first_timestamp,
                last=summary.last_timestamp,
                volume=summary.total_volume,
                delta=summary.total_delta,
            ),
            flush=True,
        )

    try:
        result = process_raw_file_to_hf(
            input_path=input_path,
            output_root=args.output_root,
            remote_prefix=args.remote_prefix,
            config=config,
            validation_chunk_size=args.validation_chunk_size,
            build_chunk_size=args.build_chunk_size,
            max_rows=args.max_rows,
            timeframes=args.timeframe,
            max_allowed_out_of_order_timestamps=args.max_out_of_order_timestamps,
            repo_id=args.repo_id,
            env_file=args.env_file,
            create_repo=args.create_repo,
            private=not args.public,
            upload_metadata=not args.skip_metadata_upload,
            metadata_repo_id=args.metadata_repo_id,
            metadata_remote_prefix=args.metadata_remote_prefix,
            manifest_path=args.manifest_path,
            repository_registry_path=args.registry_path,
            dry_run_upload=args.dry_run_upload,
            skip_remote_size_check=args.skip_remote_size_check,
            cleanup_output_after_upload=args.cleanup_output_after_upload,
            data_tier=args.data_tier,
            validation_progress_callback=report_validation_progress,
        )
    except RawValidationFailedError as exc:
        print(exc.report.format())
        print("Upload gate failures:")
        for failure in exc.failures:
            print(f"- {failure}")
        return 1

    upload_plan = result.upload_result.plan
    print(result.raw_validation_report.format())
    print(f"Partition files written: {len(result.partition_results)}")
    print(f"Output root: {result.output_root}")
    print(f"Remote prefix: {result.remote_prefix}")
    print(f"Upload mode: {'dry-run' if result.upload_result.dry_run else 'upload'}")
    print(f"Selected repo: {upload_plan.repo_id}")
    print(f"Parquet files planned: {len(upload_plan.files)}")
    print(f"Upload bytes planned: {upload_plan.total_size_bytes}")

    if not result.upload_result.dry_run:
        print(f"Uploaded Parquet files: {len(result.upload_result.uploaded_files)}")

        if result.upload_result.manifest_update is not None:
            print(f"Manifest: {result.upload_result.manifest_update.manifest_path}")
            print(
                "Repository registry: "
                f"{result.upload_result.manifest_update.repository_registry_path}"
            )

        print(f"Uploaded metadata files: {len(result.upload_result.uploaded_metadata_files)}")

    print(f"Output cleaned: {result.output_cleaned}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
