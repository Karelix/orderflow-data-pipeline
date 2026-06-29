# Orderflow Data Pipeline

Research pipeline for converting Sierra Chart ES order-flow exports into clean
Parquet datasets, validating the raw data, and tracking where uploaded Hugging
Face dataset files live.

This repo is currently focused on **ES**, not NQ.

## What This Builds

The pipeline reads Sierra Chart CSV/TXT exports and produces:

- Clean tick rows with UTC and New York timestamps.
- Time bars for `1m`, `5m`, `15m`, `30m`, `1h`, and `4h`.
- Session summaries.
- Footprint cluster rows by timeframe and price level.
- Volume profiles for `globex`, `rth`, `post_rth`, and `full` sessions.
- A repo-aware manifest that tells you which Hugging Face repo contains each
  Parquet file.

The current code supports local samples and validation. The full Hugging Face
Parquet tree uploader is the next major piece.

## Project Layout

```text
config/dataset.yaml        Main dataset, session, storage, and path config.
src/                       Pipeline library code.
scripts/                   Command-line entry points.
tests/                     Pytest test suite.
data_local/                Local raw data, samples, metadata, and outputs.
PROJECT_STATUS.md          Living implementation checklist.
.env                       Local Hugging Face tokens, ignored by git.
.env.example               Safe token template, committed to git.
```

`data_local/` is intentionally ignored by git except for `.gitkeep` files.
Do not commit raw market data or generated Parquet files.

## Setup

Create or update the conda environment:

```powershell
conda env update -f environment.yml
conda activate orderflow-data-pipeline
```

In this workspace we have been using:

```powershell
C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe
```

You can run commands with that interpreter directly:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" -m pytest tests
```

## Configure Data

The default raw sample path is configured in `config/dataset.yaml`:

```yaml
raw_data:
  sample_file: data_local/bronze/raw_sierra_csv/ESU26-CME_first_test.txt
```

Place local Sierra Chart exports under:

```text
data_local/bronze/raw_sierra_csv/
```

The current session model is:

- `globex`: 18:00 to 09:30 New York time.
- `rth`: 09:30 to 16:15 New York time.
- `post_rth`: 16:15 to 17:00 New York time.
- `full`: combined 18:00 to 17:00 New York time.

`session_date` follows the RTH date convention.

## Configure Hugging Face Tokens

Copy the token template:

```powershell
Copy-Item .env.example .env
```

Then fill in `.env`:

```text
HF_TOKEN=
HF_TOKEN_ORDERFLOW_ES_001=
HF_TOKEN_ORDERFLOW_ES_002=
```

The actual tokens stay in `.env`, which is ignored by git. The repo-to-token
mapping lives in `config/dataset.yaml`:

```yaml
storage:
  repositories:
    - repo_id: your-username/orderflow-es-001
      token_env_var: HF_TOKEN_ORDERFLOW_ES_001
    - repo_id: other-username/orderflow-es-002
      token_env_var: HF_TOKEN_ORDERFLOW_ES_002
```

This allows a later overflow repo to use a different username and token.

## Inspect Raw Data

Quick inspection:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\inspect_raw_file.py --max-rows 100000
```

Preview cleaned rows:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\preview_clean_ticks.py --max-rows 20 --show-rows 5
```

## Full Raw Validation

Run this before every upload candidate:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\validate_full_raw_stream.py --chunk-size 100000
```

This streams the raw file without writing the full derived dataset locally. It
checks parsing, ordering, tick-size alignment, volume consistency, OHLC shape,
maintenance-break rows, and session counts.

If you want to validate a different raw file:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\validate_full_raw_stream.py --input data_local\bronze\raw_sierra_csv\YOUR_FILE.txt --chunk-size 100000
```

## Build Local Samples

Write a capped clean tick sample:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\write_clean_tick_sample.py --max-rows 100000
```

Preview derived datasets without writing Parquet:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\preview_derived_datasets.py --max-rows 10000 --timeframe 1m
```

Write capped derived Parquet samples:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\write_derived_sample.py --max-rows 10000
```

Default output:

```text
data_local/tmp/derived_sample/
```

Validate derived sample logic:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\validate_derived_sample.py --max-rows 10000
```

## Manifest And Repo Lookup

Build a manifest for the local derived sample:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\build_sample_manifest.py
```

Query the manifest for a dataset slice:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\query_manifest.py --manifest data_local\tmp\derived_sample_manifest.parquet --dataset-type bars --timeframe 1m --session-date 2026-05-25
```

The output includes:

```text
repo=...
seq=...
remote=...
```

That is how you know which Hugging Face repo contains the data you want.

## Update Metadata After Upload

After a Parquet tree has been uploaded, update the persistent local manifest and
repository registry:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\update_manifest_after_upload.py --uploaded-root data_local\tmp\derived_sample --repo-id your-username/orderflow-es-001 --repo-sequence 1 --remote-prefix samples/derived_sample
```

To also upload the tiny metadata files back to Hugging Face:

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" scripts\update_manifest_after_upload.py --uploaded-root data_local\tmp\derived_sample --repo-id your-username/orderflow-es-001 --repo-sequence 1 --remote-prefix samples/derived_sample --upload-metadata
```

Remote metadata path:

```text
metadata/manifest.parquet
metadata/repository_registry.parquet
```

## Run Tests

```powershell
& "C:\Users\Petridis\.conda\envs\portfolio-projects\python.exe" -m pytest tests
```

At the time this README was written, the suite has 31 tests.

## Current Limits

- Full raw validation exists.
- Local sample derived dataset writing exists.
- Manifest and metadata upload helpers exist.
- Full direct-to-Hugging-Face derived dataset upload is not implemented yet.
- Repo capacity selection is not implemented yet.

See `PROJECT_STATUS.md` for the living checklist.
