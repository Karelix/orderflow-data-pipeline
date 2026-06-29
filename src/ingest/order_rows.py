"""Ordering helpers for cleaned tick rows."""

from __future__ import annotations

from collections.abc import Iterable

from src.ingest.convert_ticks import CleanTickRow


def sort_clean_rows(rows: Iterable[CleanTickRow]) -> list[CleanTickRow]:
    """Sort rows chronologically while preserving raw order ties."""
    return sorted(rows, key=lambda row: (row.timestamp_ny, row.sequence_id))
