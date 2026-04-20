# Garmin Data Hub

A local Garmin analytics app built on **SQLite + Streamlit**.

The project syncs Garmin data using `garmin-givemydata`, applies app-specific schema extensions, ingests FIT trackpoints, and refreshes cached derived metrics used across the UI.

> Garmin Connect download support in this project is powered by the open-source [`garmin-givemydata`](https://github.com/pe-st/garmin-givemydata) project. Garmin is not affiliated with or endorsing this application.

## Current Architecture

- **Sync source:** `garmin-givemydata`
- **Primary local database:** `%LOCALAPPDATA%\GarminDataHub\garmin.db`
- **Optional DB override:** `GARMIN_DATA_HUB_DB`
- **Post-sync refresh:** schema updates, athlete profile refresh, and `activity_metrics` derived-metric rebuilds
- **Trackpoints:** incrementally ingested from new/changed FIT archives for mapping and analysis

## Main Capabilities

- Garmin Connect sync via visible browser login flow
- Incremental FIT trackpoint ingestion into `activity_trackpoint`
- Activity analysis with map and detail views
- Derived metrics including HR zones, TRIMP/TSS, FTP estimates, and power zones
- Charts for training load, pace/power trends, and power profile analysis
- Build Plan and compliance pages for planning workflows
- Local-first operation with SQLite storage

## Streamlit Pages

Located in `src/garmin_data_hub/ui_streamlit/pages`:

- `0_Backup_Import.py` — Garmin sync, progress, and derived-refresh diagnostics
- `2_Activities.py` — activity browsing and analysis
- `3_Build_Plan.py` — plan creation workflow
- `4_Charts.py` — trend and power/load charts
- `5_Compliance.py` — compliance and zone analysis
- `6_8020_Help.py`
- `_1_MCP_Query.py` — hidden helper page (Streamlit naming convention)

## Quick Start (Developers)

```powershell
cd Training_Planner
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -e .[dev]
streamlit run src/garmin_data_hub/ui_streamlit/app.py
```

## Launch Options

### Streamlit UI

```powershell
streamlit run src/garmin_data_hub/ui_streamlit/app.py
```

### Sync CLI

```powershell
garmin-sync --visible --chrome
```

or:

```powershell
python -m garmin_data_hub.cli_backup_ingest --visible --chrome
```

Helpful sync options:

- `--days <N>` — limit sync window
- `--skip-trackpoints` — skip FIT trackpoint extraction
- `--rebuild-trackpoints` — rebuild all trackpoints
- `--trackpoints-max <N>` — cap processed activities
- `--db <path>` — use a custom SQLite path

## Tests

Run the project test suite with:

```powershell
python -m pytest
```

Targeted regression checks used during recent sync/metrics work:

```powershell
python -m pytest tests/test_sync_progress.py tests/test_metrics_refresh.py
```

## Packaging / Release

The Windows packaging pipeline lives in `packaging/build.ps1`.

Example build command:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0
```

`garmin-givemydata` source selection:

- Default mode is `pypi` (no extra args needed)
- Use `-GivemydataSource local` to package against your local sibling repo copy
- Use `-GivemydataPypiSpec` to pin a specific PyPI version

Examples:

```powershell
# Default PyPI mode
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0

# Local repo mode
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0 -GivemydataSource local

# PyPI mode with explicit version
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0 -GivemydataSource pypi -GivemydataPypiSpec "garmin-givemydata==0.1.10"
```

It will:

- build the Streamlit app directory (`GarminDataHub`)
- build the CLI directory (`cli_backup_ingest`)
- print a source-mode banner (`LOCAL` or `PYPI`) in build logs
- bundle `garmin-givemydata.exe` from project `.venv` with the release artifacts
- copy outputs under `release/<version>/`
- optionally build the installer via Inno Setup when available

## Project Requirements

- Python `>=3.10`
- Windows is the primary supported environment

Core dependencies are defined in `pyproject.toml`, including:

- `streamlit`
- `pandas`
- `numpy`
- `fitparse`
- `gpxpy`
- `garmin-givemydata`

## Repository Notes

- Secret-like artifacts under `scripts/cookies/*.json` are gitignored
- Download artifacts under `scripts/downloads/` are gitignored
- Build/release artifacts under `build/` and `release/` are not intended for source control
- Metric/source lineage reference: `docs/activity-metrics-data-lineage.md`

## License

See `LICENSE`.
