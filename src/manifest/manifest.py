"""Build file manifests and Hugging Face repository registries."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq


MANIFEST_SCHEMA = pa.schema(
    [
        ("repo_id", pa.string()),
        ("repo_sequence", pa.int32()),
        ("remote_path", pa.string()),
        ("data_tier", pa.string()),
        ("local_path", pa.string()),
        ("dataset_type", pa.string()),
        ("symbol", pa.string()),
        ("contract", pa.string()),
        ("timeframe", pa.string()),
        ("session_type", pa.string()),
        ("session_type_values", pa.string()),
        ("session_date_min", pa.date32()),
        ("session_date_max", pa.date32()),
        ("year_min", pa.int16()),
        ("year_max", pa.int16()),
        ("month_min", pa.int8()),
        ("month_max", pa.int8()),
        ("rows", pa.int64()),
        ("file_size_bytes", pa.int64()),
        ("start_timestamp_utc", pa.timestamp("us", tz="UTC")),
        ("end_timestamp_utc", pa.timestamp("us", tz="UTC")),
        ("start_timestamp_ny", pa.timestamp("us", tz="America/New_York")),
        ("end_timestamp_ny", pa.timestamp("us", tz="America/New_York")),
        ("validation_status", pa.string()),
        ("created_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)

DEFAULT_MANIFEST_DATA_TIER = "main"
DATA_TIER_ALIASES = {
    "prod": "main",
    "production": "main",
    "samples": "sample",
    "tests": "test",
    "testing": "test",
}

REPOSITORY_REGISTRY_SCHEMA = pa.schema(
    [
        ("repo_id", pa.string()),
        ("repo_sequence", pa.int32()),
        ("role", pa.string()),
        ("provider", pa.string()),
        ("size_limit_bytes", pa.int64()),
        ("current_manifest_size_bytes", pa.int64()),
        ("first_session_date", pa.date32()),
        ("last_session_date", pa.date32()),
        ("dataset_types", pa.string()),
        ("notes", pa.string()),
        ("updated_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class ManifestEntry:
    """One generated Parquet file and where it lives."""

    repo_id: str
    repo_sequence: int
    remote_path: str
    data_tier: str
    local_path: str
    dataset_type: str
    symbol: str
    contract: str
    timeframe: str | None
    session_type: str | None
    session_type_values: str | None
    session_date_min: date | None
    session_date_max: date | None
    year_min: int | None
    year_max: int | None
    month_min: int | None
    month_max: int | None
    rows: int
    file_size_bytes: int
    start_timestamp_utc: datetime | None
    end_timestamp_utc: datetime | None
    start_timestamp_ny: datetime | None
    end_timestamp_ny: datetime | None
    validation_status: str
    created_at_utc: datetime


@dataclass(frozen=True)
class RepositoryEntry:
    """One Hugging Face repository used by the dataset."""

    repo_id: str
    repo_sequence: int
    role: str
    provider: str
    size_limit_bytes: int
    current_manifest_size_bytes: int
    first_session_date: date | None
    last_session_date: date | None
    dataset_types: str | None
    notes: str | None
    updated_at_utc: datetime


def build_manifest_for_parquet_tree(
    root: str | Path,
    config: Mapping[str, Any],
    repo_id: str | None = None,
    repo_sequence: int | None = None,
    remote_prefix: str = "",
    validation_status: str = "validated",
    data_tier: str | None = None,
) -> list[ManifestEntry]:
    """Build manifest entries for every Parquet file under a root directory."""
    root_path = Path(root)
    selected_repo_id = repo_id or config["storage"]["active_repo_id"]
    selected_repo_sequence = repo_sequence or _repo_sequence_for_id(
        config=config,
        repo_id=selected_repo_id,
    )

    return [
        build_manifest_entry(
            parquet_path=path,
            root=root_path,
            config=config,
            repo_id=selected_repo_id,
            repo_sequence=selected_repo_sequence,
            remote_prefix=remote_prefix,
            validation_status=validation_status,
            data_tier=data_tier,
        )
        for path in sorted(root_path.rglob("*.parquet"))
    ]


def build_manifest_for_parquet_files(
    root: str | Path,
    parquet_files: Iterable[str | Path],
    config: Mapping[str, Any],
    repo_id: str | None = None,
    repo_sequence: int | None = None,
    remote_prefix: str = "",
    validation_status: str = "validated",
    data_tier: str | None = None,
) -> list[ManifestEntry]:
    """Build manifest entries for explicit Parquet files under a root."""
    root_path = Path(root)
    selected_repo_id = repo_id or config["storage"]["active_repo_id"]
    selected_repo_sequence = repo_sequence or _repo_sequence_for_id(
        config=config,
        repo_id=selected_repo_id,
    )

    return [
        build_manifest_entry(
            parquet_path=path,
            root=root_path,
            config=config,
            repo_id=selected_repo_id,
            repo_sequence=selected_repo_sequence,
            remote_prefix=remote_prefix,
            validation_status=validation_status,
            data_tier=data_tier,
        )
        for path in sorted(_resolve_parquet_files(root_path, parquet_files))
    ]


def build_manifest_entry(
    parquet_path: str | Path,
    root: str | Path,
    config: Mapping[str, Any],
    repo_id: str,
    repo_sequence: int,
    remote_prefix: str = "",
    validation_status: str = "validated",
    data_tier: str | None = None,
) -> ManifestEntry:
    """Create a manifest entry from one local Parquet file."""
    file_path = Path(parquet_path)
    root_path = Path(root)
    relative_path = file_path.resolve().relative_to(root_path.resolve())
    remote_path = _join_remote_path(remote_prefix, relative_path)
    selected_data_tier = normalize_manifest_data_tier(
        data_tier or infer_manifest_data_tier(remote_path)
    )
    table = pq.read_table(file_path)
    dataset_type = _dataset_type_from_path(relative_path)

    symbol = _single_or_default(table, "symbol", config["dataset"]["symbol"])
    contract = _single_or_default(table, "contract", config["dataset"]["default_contract"])
    timeframe = _single_or_none(table, "timeframe") or _partition_value(relative_path, "timeframe")
    session_type_values = _values_as_csv(table, "session_type")
    session_type = _single_or_none(table, "session_type")

    session_dates = _date_values(table, "session_date")
    timestamps_utc = _datetime_values(table, "timestamp_utc")
    timestamps_ny = _datetime_values(table, "timestamp_ny")
    created_at = datetime.now(timezone.utc)

    return ManifestEntry(
        repo_id=repo_id,
        repo_sequence=repo_sequence,
        remote_path=remote_path,
        data_tier=selected_data_tier,
        local_path=str(file_path),
        dataset_type=dataset_type,
        symbol=symbol,
        contract=contract,
        timeframe=timeframe,
        session_type=session_type,
        session_type_values=session_type_values,
        session_date_min=min(session_dates) if session_dates else None,
        session_date_max=max(session_dates) if session_dates else None,
        year_min=min(value.year for value in session_dates) if session_dates else None,
        year_max=max(value.year for value in session_dates) if session_dates else None,
        month_min=min(value.month for value in session_dates) if session_dates else None,
        month_max=max(value.month for value in session_dates) if session_dates else None,
        rows=table.num_rows,
        file_size_bytes=file_path.stat().st_size,
        start_timestamp_utc=min(timestamps_utc) if timestamps_utc else None,
        end_timestamp_utc=max(timestamps_utc) if timestamps_utc else None,
        start_timestamp_ny=min(timestamps_ny) if timestamps_ny else None,
        end_timestamp_ny=max(timestamps_ny) if timestamps_ny else None,
        validation_status=validation_status,
        created_at_utc=created_at,
    )


def build_repository_registry(
    manifest_entries: Iterable[ManifestEntry],
    config: Mapping[str, Any],
) -> list[RepositoryEntry]:
    """Summarize manifest entries by Hugging Face repository."""
    provider = config["storage"]["provider"]
    size_limit_bytes = int(config["storage"]["repo_size_limit_bytes"])
    configured_repos = {
        repo["repo_id"]: repo
        for repo in config["storage"].get("repositories", [])
    }
    entries_by_repo: dict[str, list[ManifestEntry]] = {}

    for entry in manifest_entries:
        entries_by_repo.setdefault(entry.repo_id, []).append(entry)

    registry: list[RepositoryEntry] = []
    updated_at = datetime.now(timezone.utc)

    for repo_id, repo_entries in sorted(
        entries_by_repo.items(),
        key=lambda item: _repo_sequence_for_entries(config, item[0], item[1]),
    ):
        config_entry = configured_repos.get(repo_id, {})
        repo_sequence = _repo_sequence_for_entries(config, repo_id, repo_entries)
        session_dates = [
            value
            for entry in repo_entries
            for value in (entry.session_date_min, entry.session_date_max)
            if value is not None
        ]
        dataset_types = ",".join(sorted({entry.dataset_type for entry in repo_entries}))

        registry.append(
            RepositoryEntry(
                repo_id=repo_id,
                repo_sequence=repo_sequence,
                role=config_entry.get("role", "active"),
                provider=provider,
                size_limit_bytes=size_limit_bytes,
                current_manifest_size_bytes=sum(
                    entry.file_size_bytes for entry in repo_entries
                ),
                first_session_date=min(session_dates) if session_dates else None,
                last_session_date=max(session_dates) if session_dates else None,
                dataset_types=dataset_types or None,
                notes=config_entry.get("notes"),
                updated_at_utc=updated_at,
            )
        )

    return registry


def find_manifest_entries(
    entries: Iterable[ManifestEntry],
    data_tier: str | None = None,
    dataset_type: str | None = None,
    symbol: str | None = None,
    contract: str | None = None,
    timeframe: str | None = None,
    session_type: str | None = None,
    session_date: date | None = None,
) -> list[ManifestEntry]:
    """Find manifest entries that can contain a requested dataset slice."""
    matches = []
    selected_data_tier = (
        normalize_manifest_data_tier(data_tier) if data_tier is not None else None
    )

    for entry in entries:
        if selected_data_tier is not None and entry.data_tier != selected_data_tier:
            continue
        if dataset_type is not None and entry.dataset_type != dataset_type:
            continue
        if symbol is not None and entry.symbol != symbol:
            continue
        if contract is not None and entry.contract != contract:
            continue
        if timeframe is not None and entry.timeframe != timeframe:
            continue
        if session_type is not None and not _entry_has_session_type(entry, session_type):
            continue
        if session_date is not None and not _entry_contains_session_date(entry, session_date):
            continue

        matches.append(entry)

    return matches


def merge_manifest_entries(
    existing_entries: Iterable[ManifestEntry],
    new_entries: Iterable[ManifestEntry],
) -> list[ManifestEntry]:
    """Merge manifest entries, replacing rows that point to the same remote file."""
    by_remote_file: dict[tuple[str, str], ManifestEntry] = {}

    for entry in existing_entries:
        by_remote_file[_manifest_identity(entry)] = entry

    for entry in new_entries:
        by_remote_file[_manifest_identity(entry)] = entry

    return sorted(
        by_remote_file.values(),
        key=lambda entry: (
            entry.repo_sequence,
            entry.repo_id,
            entry.data_tier,
            entry.dataset_type,
            entry.timeframe or "",
            entry.session_type or "",
            entry.session_date_min or date.min,
            entry.remote_path,
        ),
    )


def write_manifest_parquet(entries: Iterable[ManifestEntry], output_path: str | Path) -> None:
    """Write manifest entries to Parquet."""
    _write_dataclass_parquet(entries, MANIFEST_SCHEMA, output_path)


def read_manifest_parquet(path: str | Path) -> list[ManifestEntry]:
    """Read manifest entries from Parquet."""
    table = pq.read_table(path)
    return [_manifest_entry_from_row(row) for row in table.to_pylist()]


def write_repository_registry_parquet(
    entries: Iterable[RepositoryEntry],
    output_path: str | Path,
) -> None:
    """Write repository registry entries to Parquet."""
    _write_dataclass_parquet(entries, REPOSITORY_REGISTRY_SCHEMA, output_path)


def read_repository_registry_parquet(path: str | Path) -> list[RepositoryEntry]:
    """Read repository registry entries from Parquet."""
    table = pq.read_table(path, schema=REPOSITORY_REGISTRY_SCHEMA)
    return [RepositoryEntry(**row) for row in table.to_pylist()]


def _write_dataclass_parquet(
    entries: Iterable[object],
    schema: pa.Schema,
    output_path: str | Path,
) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(entry) for entry in entries]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output_file, compression="snappy")


def _manifest_identity(entry: ManifestEntry) -> tuple[str, str]:
    return (entry.repo_id, entry.remote_path)


def _resolve_parquet_files(
    root: Path,
    parquet_files: Iterable[str | Path],
) -> list[Path]:
    root_path = root.resolve()
    resolved_files = []

    for parquet_file in parquet_files:
        path = Path(parquet_file)

        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        if not path.is_file():
            raise ValueError(f"Expected a Parquet file, got: {path}")

        if path.suffix != ".parquet":
            raise ValueError(f"Expected a .parquet file, got: {path}")

        path.resolve().relative_to(root_path)
        resolved_files.append(path)

    if not resolved_files:
        raise ValueError("At least one Parquet file is required.")

    return resolved_files


def infer_manifest_data_tier(remote_path: str) -> str:
    """Infer a manifest data tier from a remote path."""
    parts = [
        part.lower()
        for part in remote_path.replace("\\", "/").split("/")
        if part
    ]

    if any(_is_tier_path_part(part, {"sample", "samples"}) for part in parts):
        return "sample"

    if any(_is_tier_path_part(part, {"test", "tests", "testing"}) for part in parts):
        return "test"

    if parts and parts[0] == "main":
        return "main"

    return DEFAULT_MANIFEST_DATA_TIER


def normalize_manifest_data_tier(value: str) -> str:
    """Normalize data-tier names used by CLIs and manifest rows."""
    normalized = value.strip().lower()

    if not normalized:
        return DEFAULT_MANIFEST_DATA_TIER

    return DATA_TIER_ALIASES.get(normalized, normalized)


def _is_tier_path_part(part: str, tier_names: set[str]) -> bool:
    return (
        part in tier_names
        or any(part.startswith(f"{name}-") for name in tier_names)
        or any(part.startswith(f"{name}_") for name in tier_names)
    )


def _manifest_entry_from_row(row: Mapping[str, Any]) -> ManifestEntry:
    field_names = {field.name for field in fields(ManifestEntry)}
    normalized = {name: row.get(name) for name in field_names}

    if normalized["data_tier"] is None:
        normalized["data_tier"] = infer_manifest_data_tier(
            str(normalized.get("remote_path") or "")
        )
    else:
        normalized["data_tier"] = normalize_manifest_data_tier(
            str(normalized["data_tier"])
        )

    return ManifestEntry(**normalized)


def _dataset_type_from_path(path: Path) -> str:
    return path.parts[0]


def _join_remote_path(prefix: str, relative_path: Path) -> str:
    remote_parts = [part for part in prefix.strip("/").split("/") if part]
    remote_parts.extend(relative_path.parts)
    return "/".join(remote_parts).replace("\\", "/")


def _repo_sequence_for_id(config: Mapping[str, Any], repo_id: str) -> int:
    for repo in config["storage"].get("repositories", []):
        if repo["repo_id"] == repo_id:
            return int(repo["repo_sequence"])

    return 0


def _repo_sequence_for_entries(
    config: Mapping[str, Any],
    repo_id: str,
    entries: Iterable[ManifestEntry],
) -> int:
    configured_sequence = _repo_sequence_for_id(config, repo_id)

    if configured_sequence:
        return configured_sequence

    return min(entry.repo_sequence for entry in entries)


def _single_or_default(table: pa.Table, column: str, default: str) -> str:
    return _single_or_none(table, column) or default


def _single_or_none(table: pa.Table, column: str) -> str | None:
    values = _string_values(table, column)

    if len(values) == 1:
        return values[0]

    return None


def _values_as_csv(table: pa.Table, column: str) -> str | None:
    values = _string_values(table, column)

    if not values:
        return None

    return ",".join(values)


def _string_values(table: pa.Table, column: str) -> list[str]:
    if column not in table.column_names:
        return []

    return sorted(
        {
            str(value)
            for value in table[column].to_pylist()
            if value is not None
        }
    )


def _date_values(table: pa.Table, column: str) -> list[date]:
    if column not in table.column_names:
        return []

    return [
        value
        for value in table[column].to_pylist()
        if value is not None
    ]


def _datetime_values(table: pa.Table, column: str) -> list[datetime]:
    if column not in table.column_names:
        return []

    return [
        value
        for value in table[column].to_pylist()
        if value is not None
    ]


def _partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="

    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]

    return None


def _entry_has_session_type(entry: ManifestEntry, session_type: str) -> bool:
    if entry.session_type == session_type:
        return True

    if entry.session_type_values is None:
        return False

    return session_type in entry.session_type_values.split(",")


def _entry_contains_session_date(entry: ManifestEntry, session_date: date) -> bool:
    if entry.session_date_min is None or entry.session_date_max is None:
        return False

    return entry.session_date_min <= session_date <= entry.session_date_max
