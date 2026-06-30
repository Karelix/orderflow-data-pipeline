"""Orchestrate raw validation, partitioned writing, and Hugging Face upload."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from src.storage import (
    ParquetTreeUploadResult,
    UploadedParquetFile,
    upload_parquet_files_to_hf,
    upload_parquet_tree_to_hf,
)
from src.streaming import PartitionedWriteResult, write_partitioned_derived_parquets
from src.validation import RawStreamValidationReport, validate_raw_stream
from src.validation.raw_stream import RawStreamChunkSummary


ProgressCallback = Callable[[RawStreamChunkSummary], None]


@dataclass(frozen=True)
class StagedUploadProgress:
    """Progress for one staged partition batch."""

    stage_index: int
    status: str
    partition_results: list[PartitionedWriteResult]
    upload_result: ParquetTreeUploadResult | None = None
    local_files_deleted: bool = False


@dataclass(frozen=True)
class ProcessRawToHfResult:
    """Result of a raw-file processing run."""

    raw_validation_report: RawStreamValidationReport
    partition_results: list[PartitionedWriteResult]
    upload_result: ParquetTreeUploadResult
    output_root: Path
    remote_prefix: str
    output_cleaned: bool
    staged_upload_results: list[ParquetTreeUploadResult] = field(default_factory=list)


class RawValidationFailedError(RuntimeError):
    """Raised when raw validation fails the upload gate."""

    def __init__(
        self,
        report: RawStreamValidationReport,
        failures: list[str],
    ) -> None:
        self.report = report
        self.failures = failures
        super().__init__("Raw validation failed upload gate: " + "; ".join(failures))


def process_raw_file_to_hf(
    input_path: str | Path,
    output_root: str | Path,
    remote_prefix: str,
    config: Mapping[str, Any],
    validation_chunk_size: int = 100000,
    build_chunk_size: int | None = None,
    max_rows: int | None = None,
    timeframes: list[str] | None = None,
    max_allowed_out_of_order_timestamps: int | None = None,
    repo_id: str | None = None,
    env_file: str | Path | None = None,
    create_repo: bool = False,
    private: bool = True,
    upload_metadata: bool = True,
    metadata_repo_id: str | None = None,
    metadata_remote_prefix: str | None = None,
    manifest_path: str | Path | None = None,
    repository_registry_path: str | Path | None = None,
    dry_run_upload: bool = False,
    skip_remote_size_check: bool = False,
    cleanup_output_after_upload: bool = False,
    data_tier: str | None = None,
    staged_upload: bool = False,
    keep_staged_output: bool = False,
    api: object | None = None,
    validation_progress_callback: ProgressCallback | None = None,
    staged_progress_callback: Callable[[StagedUploadProgress], None] | None = None,
) -> ProcessRawToHfResult:
    """Validate a raw file, write partitioned datasets, and upload them."""
    selected_input_path = Path(input_path)
    selected_output_root = Path(output_root)
    allowed_out_of_order = _selected_out_of_order_allowance(
        config=config,
        override=max_allowed_out_of_order_timestamps,
    )

    validation_report = validate_raw_stream(
        path=selected_input_path,
        config=config,
        chunk_size=validation_chunk_size,
        max_rows=max_rows,
        progress_callback=validation_progress_callback,
    )
    validation_failures = raw_validation_failures_for_upload(
        report=validation_report,
        max_allowed_out_of_order_timestamps=allowed_out_of_order,
    )

    if validation_failures:
        raise RawValidationFailedError(validation_report, validation_failures)

    staged_upload_results: list[ParquetTreeUploadResult] = []

    def upload_partition_batch(batch: list[PartitionedWriteResult]) -> None:
        stage_index = len(staged_upload_results) + 1

        if staged_progress_callback is not None:
            staged_progress_callback(
                StagedUploadProgress(
                    stage_index=stage_index,
                    status="ready",
                    partition_results=batch,
                )
            )

        upload_result = upload_parquet_files_to_hf(
            input_root=selected_output_root,
            parquet_files=[result.path for result in batch],
            config=config,
            remote_prefix=remote_prefix,
            repo_id=repo_id,
            env_file=env_file,
            create_repo=create_repo,
            private=private,
            upload_metadata=upload_metadata,
            metadata_repo_id=metadata_repo_id,
            metadata_remote_prefix=metadata_remote_prefix,
            manifest_path=manifest_path,
            repository_registry_path=repository_registry_path,
            validation_status="validated",
            data_tier=data_tier,
            dry_run=dry_run_upload,
            skip_remote_size_check=skip_remote_size_check,
            api=api,
        )
        staged_upload_results.append(upload_result)

        local_files_deleted = False
        if not dry_run_upload and not keep_staged_output:
            _delete_uploaded_files(
                uploaded_files=upload_result.uploaded_files,
                output_root=selected_output_root,
            )
            local_files_deleted = True

        if staged_progress_callback is not None:
            staged_progress_callback(
                StagedUploadProgress(
                    stage_index=stage_index,
                    status="uploaded",
                    partition_results=batch,
                    upload_result=upload_result,
                    local_files_deleted=local_files_deleted,
                )
            )

    partition_results = write_partitioned_derived_parquets(
        input_path=selected_input_path,
        output_root=selected_output_root,
        config=config,
        chunk_size_rows=build_chunk_size,
        max_rows=max_rows,
        timeframes=timeframes,
        partition_batch_callback=upload_partition_batch if staged_upload else None,
    )

    if staged_upload:
        if not staged_upload_results:
            raise RuntimeError("No partition files were written for staged upload.")

        upload_result = staged_upload_results[-1]
    else:
        upload_result = upload_parquet_tree_to_hf(
            input_root=selected_output_root,
            config=config,
            remote_prefix=remote_prefix,
            repo_id=repo_id,
            env_file=env_file,
            create_repo=create_repo,
            private=private,
            upload_metadata=upload_metadata,
            metadata_repo_id=metadata_repo_id,
            metadata_remote_prefix=metadata_remote_prefix,
            manifest_path=manifest_path,
            repository_registry_path=repository_registry_path,
            validation_status="validated",
            data_tier=data_tier,
            dry_run=dry_run_upload,
            skip_remote_size_check=skip_remote_size_check,
            api=api,
        )

    output_cleaned = False
    if cleanup_output_after_upload and not upload_result.dry_run:
        _cleanup_output_root(selected_output_root)
        output_cleaned = True
    elif staged_upload and not dry_run_upload and not keep_staged_output:
        output_cleaned = not selected_output_root.exists()

    return ProcessRawToHfResult(
        raw_validation_report=validation_report,
        partition_results=partition_results,
        upload_result=upload_result,
        output_root=selected_output_root,
        remote_prefix=remote_prefix,
        output_cleaned=output_cleaned,
        staged_upload_results=staged_upload_results,
    )


def raw_validation_failures_for_upload(
    report: RawStreamValidationReport,
    max_allowed_out_of_order_timestamps: int = 0,
) -> list[str]:
    """Return validation failures that should prevent upload."""
    failures = []

    if report.parse_error_count:
        failures.append(f"parse errors={report.parse_error_count}")

    if report.out_of_order_timestamp_count > max_allowed_out_of_order_timestamps:
        failures.append(
            "out-of-order timestamps="
            f"{report.out_of_order_timestamp_count} "
            f"> allowed {max_allowed_out_of_order_timestamps}"
        )

    if report.volume_bid_ask_mismatch_count:
        failures.append(
            "volume mismatches="
            f"{report.volume_bid_ask_mismatch_count}"
        )

    if report.price_tick_mismatch_count:
        failures.append(f"price tick mismatches={report.price_tick_mismatch_count}")

    return failures


def _selected_out_of_order_allowance(
    config: Mapping[str, Any],
    override: int | None,
) -> int:
    if override is not None:
        return override

    return int(
        config.get("validation", {}).get(
            "max_allowed_out_of_order_timestamps",
            0,
        )
    )


def _cleanup_output_root(output_root: Path) -> None:
    resolved = output_root.resolve()

    if not resolved.exists():
        return

    if not resolved.is_dir():
        raise NotADirectoryError(f"Output root is not a directory: {resolved}")

    cwd = Path.cwd().resolve()
    if resolved == cwd or resolved == Path(resolved.anchor):
        raise ValueError(f"Refusing to clean unsafe output root: {resolved}")

    if len(resolved.parts) < 3:
        raise ValueError(f"Refusing to clean suspiciously short path: {resolved}")

    shutil.rmtree(resolved)


def _delete_uploaded_files(
    uploaded_files: list[UploadedParquetFile],
    output_root: Path,
) -> None:
    resolved_root = output_root.resolve()

    for uploaded_file in uploaded_files:
        local_path = uploaded_file.local_path.resolve()
        local_path.relative_to(resolved_root)

        if local_path.exists():
            if not local_path.is_file():
                raise ValueError(f"Uploaded path is not a file: {local_path}")

            local_path.unlink()

        _remove_empty_parents(local_path.parent, resolved_root)


def _remove_empty_parents(start: Path, stop: Path) -> None:
    current = start

    while True:
        if not current.exists() or not current.is_dir() or any(current.iterdir()):
            return

        current.rmdir()

        if current == stop:
            return

        current = current.parent
