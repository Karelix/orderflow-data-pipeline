"""Validate a raw file, write partitioned datasets, and upload to Hugging Face."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.pipeline import (
    RawValidationFailedError,
    StagedUploadProgress,
    process_raw_file_to_hf,
)
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
        "--staged-upload",
        action="store_true",
        help="Upload each written partition batch and delete it after upload.",
    )
    parser.add_argument(
        "--keep-staged-output",
        action="store_true",
        help="Keep local files written during staged upload.",
    )
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

    def report_staged_progress(progress: StagedUploadProgress) -> None:
        session_dates = _format_session_dates(progress)
        datasets = _format_datasets(progress)
        rows = sum(item.rows for item in progress.partition_results)
        size_bytes = sum(item.file_size_bytes for item in progress.partition_results)

        if progress.status == "ready":
            print(
                "stage={stage} ready sessions={sessions} files={files} rows={rows} size={size} datasets={datasets}".format(
                    stage=progress.stage_index,
                    sessions=session_dates,
                    files=len(progress.partition_results),
                    rows=rows,
                    size=_format_bytes(size_bytes),
                    datasets=datasets,
                ),
                flush=True,
            )
            return

        if progress.status == "uploaded" and progress.upload_result is not None:
            action = "planned" if progress.upload_result.dry_run else "uploaded"
            files = (
                len(progress.upload_result.plan.files)
                if progress.upload_result.dry_run
                else len(progress.upload_result.uploaded_files)
            )
            print(
                "stage={stage} {action} sessions={sessions} files={files} repo={repo} cleaned={cleaned}".format(
                    stage=progress.stage_index,
                    action=action,
                    sessions=session_dates,
                    files=files,
                    repo=progress.upload_result.plan.repo_id,
                    cleaned=str(progress.local_files_deleted).lower(),
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
            staged_upload=args.staged_upload,
            keep_staged_output=args.keep_staged_output,
            validation_progress_callback=report_validation_progress,
            staged_progress_callback=report_staged_progress,
        )
    except RawValidationFailedError as exc:
        print(exc.report.format())
        print("Upload gate failures:")
        for failure in exc.failures:
            print(f"- {failure}")
        return 1

    upload_results = result.staged_upload_results or [result.upload_result]
    upload_plan = result.upload_result.plan
    planned_file_count = sum(len(item.plan.files) for item in upload_results)
    planned_bytes = sum(item.plan.total_size_bytes for item in upload_results)
    uploaded_file_count = sum(len(item.uploaded_files) for item in upload_results)
    uploaded_metadata_count = sum(
        len(item.uploaded_metadata_files)
        for item in upload_results
    )
    selected_repos = ",".join(
        sorted({item.plan.repo_id for item in upload_results})
    )

    print(result.raw_validation_report.format())
    print(f"Partition files written: {len(result.partition_results)}")
    print(f"Output root: {result.output_root}")
    print(f"Remote prefix: {result.remote_prefix}")
    print(
        "Upload mode: "
        f"{'staged ' if result.staged_upload_results else ''}"
        f"{'dry-run' if result.upload_result.dry_run else 'upload'}"
    )
    print(f"Upload stages: {len(upload_results)}")
    print(f"Selected repo: {selected_repos}")
    print(f"Last selected repo: {upload_plan.repo_id}")
    print(f"Parquet files planned: {planned_file_count}")
    print(f"Upload bytes planned: {planned_bytes}")

    if not result.upload_result.dry_run:
        print(f"Uploaded Parquet files: {uploaded_file_count}")

        if result.upload_result.manifest_update is not None:
            print(f"Manifest: {result.upload_result.manifest_update.manifest_path}")
            print(
                "Repository registry: "
                f"{result.upload_result.manifest_update.repository_registry_path}"
            )

        print(f"Uploaded metadata files: {uploaded_metadata_count}")

    print(f"Output cleaned: {result.output_cleaned}")
    return 0


def _format_session_dates(progress: StagedUploadProgress) -> str:
    values = sorted({item.session_date.isoformat() for item in progress.partition_results})

    if not values:
        return ""

    if len(values) == 1:
        return values[0]

    return f"{values[0]}..{values[-1]}"


def _format_datasets(progress: StagedUploadProgress) -> str:
    return ",".join(sorted({item.dataset_type for item in progress.partition_results}))


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}"

        size /= 1024

    return f"{value}B"


if __name__ == "__main__":
    raise SystemExit(main())
