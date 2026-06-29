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
- Per-repo Hugging Face token environment variables.
- Local `.env` file for secrets and tracked `.env.example` template.
- Automated tests for parsing, sessions, derived datasets, validation,
  manifests, local metadata updates, HF metadata upload wiring, and token
  resolution.

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

- Implement `scripts/upload_parquet_tree_to_hf.py`.
- Add Hugging Face repo size inspection.
- Add automatic repo selection when a repo approaches the configured 100GB
  limit.
- Upload Parquet trees to the selected Hugging Face repo.
- After each upload, automatically update local manifest and upload remote
  metadata.
- Connect the full raw-to-derived build to the uploader.
- Build the full pipeline so large outputs can be uploaded in stages instead of
  remaining permanently on local disk.
- Add resumable upload behavior.
- Add raw file registry metadata.
- Add integration tests around repo selection and upload planning using fake HF
  APIs.

## Suggested Next Commit Title

```text
Build ES orderflow validation, derived samples, and manifest metadata
```
