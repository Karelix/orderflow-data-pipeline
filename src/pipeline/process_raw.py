"""Orchestrate raw validation, partitioned writing, and Hugging Face upload."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from src.storage import ParquetTreeUploadResult, upload_parquet_tree_to_hf
from src.streaming import PartitionedWriteResult, write_partitioned_derived_parquets
from src.validation import RawStreamValidationReport, validate_raw_stream
from src.validation.raw_stream import RawStreamChunkSummary


ProgressCallback = Callable[[RawStreamChunkSummary], None]


@dataclass(frozen=True)
class ProcessRawToHfResult:
    """Result of a raw-file processing run."""

    raw_validation_report: RawStreamValidationReport
    partition_results: list[PartitionedWriteResult]
    upload_result: ParquetTreeUploadResult
    output_root: Path
    remote_prefix: str
    output_cleaned: bool


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
    api: object | None = None,
    validation_progress_callback: ProgressCallback | None = None,
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

    partition_results = write_partitioned_derived_parquets(
        input_path=selected_input_path,
        output_root=selected_output_root,
        config=config,
        chunk_size_rows=build_chunk_size,
        max_rows=max_rows,
        timeframes=timeframes,
    )
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

    return ProcessRawToHfResult(
        raw_validation_report=validation_report,
        partition_results=partition_results,
        upload_result=upload_result,
        output_root=selected_output_root,
        remote_prefix=remote_prefix,
        output_cleaned=output_cleaned,
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
