# Project Status

This file should be updated whenever the pipeline changes in a meaningful way.

## Implemented

- ES-focused dataset configuration in `config/dataset.yaml`.
- Sierra Chart raw file inspection.
- Config loader for `dataset.yaml`.
- Git ignore rules that keep local data, generated Parquet files, and `.env`
  out of GitHub.
- Session classification for `globex`, `rth`, and `post_rth` using New York
  time and RTH-date session dating.
- Clean tick row conversion with:
  - UTC timestamp.
  - New York timestamp.
  - `session_date`.
  - `session_type`.
  - `sequence_id`.
  - `delta`.
  - integer `price_ticks`.
- Streaming full raw validation with configurable chunk size.
- Sorting helper for tiny timestamp reversals before derived aggregation.
- Time bars for configured timeframes with:
  - OHLC.
  - total volume.
  - buying volume.
  - selling volume.
  - delta.
  - cumulative delta.
  - number of trades.
  - VWAP.
- Session summaries.
- Footprint cluster rows by timeframe and price level.
- Volume profiles for `globex`, `rth`, `post_rth`, and `full`.
- Local sample Parquet writers for clean ticks and derived datasets.
- Partitioned main-style Parquet writer that writes one file per
  `session_date` partition.
- Stateful chunked aggregation for bars, footprints, volume profiles, and
  session summaries.
- Tests comparing chunked partitioned output against the existing all-at-once
  builders, including a bar split across chunks.
- Derived dataset validation checks.
- Repo-aware manifest entries with `repo_id`, `repo_sequence`, `remote_path`,
  local path, dataset type, timeframe, session type values, dates, rows, size,
  timestamps, and validation status.
- Repository registry with per-repo size summaries.
- Manifest query script for finding which repo contains a requested dataset
  slice.
- Local manifest and repository registry update after upload.
- Hugging Face metadata upload helper for `metadata/manifest.parquet` and
  `metadata/repository_registry.parquet`.
- Local Parquet tree uploader for Hugging Face dataset repos.
- Upload planning with local file size calculation.
- Hugging Face repo size inspection from repo file metadata.
- Automatic repo selection using configured active or standby repos and the
  100GB limit.
- Upload dry-run mode for local planning without changing remote or local
  metadata.
- Post-upload local manifest update and optional remote metadata upload.
- Per-repo Hugging Face token environment variables.
- Local `.env` file for secrets and tracked `.env.example` template.
- Automated tests for parsing, sessions, derived datasets, validation,
  manifests, local metadata updates, HF metadata upload wiring, Parquet tree
  upload planning, partitioned writing, and token resolution.

## Current Operating Rules

- The dataset is for ES.
- Full raw validation should run before every real upload.
- Raw data and generated Parquet files stay under `data_local/` and are not
  committed.
- Actual Hugging Face tokens stay in `.env` and are not committed.
- `dataset.yaml` stores token environment variable names, not token values.
- Manifests should be kept locally and copied to Hugging Face metadata paths so
  repo lookup remains recoverable.

## Validated Raw File Notes

The first full raw validation of `ESU26-CME_first_test.txt` scanned 22,420,750
rows. It found no parse errors, no volume mismatches, no tick-size mismatches,
and no closed maintenance rows. It found two tiny timestamp reversals, so
derived builders sort rows by `(timestamp_ny, sequence_id)` before aggregation.

## Still To Do

- Test the uploader against the real `karelix/orderflow-es-001` Hugging Face
  repo using the small derived sample.
- Connect full raw validation, partitioned derived writing, and Hugging Face
  upload into one orchestration script.
- Upload completed partition trees in stages so large outputs do not remain
  permanently on local disk.
- Add resumable upload behavior.
- Add raw file registry metadata.
- Add tests for resumable or retryable upload behavior once that exists.

## Suggested Next Commit Title

```text
Add Hugging Face Parquet tree upload workflow
```
