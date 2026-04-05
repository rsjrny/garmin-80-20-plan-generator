# Garmin Data Hub

A local Garmin analytics app built on Streamlit + SQLite.

The app syncs Garmin data using `garmin-givemydata`, applies app-specific schema extensions, and provides pages for activity analysis, charts, and training plan generation.

> Garmin Connect download support in this project is powered by the open-source [`garmin-givemydata`](https://github.com/pe-st/garmin-givemydata) project, which is used to authenticate and download activity FIT files/data for local analysis. Garmin is not affiliated with or endorsing this application.

## Current Architecture

- Data sync source: `garmin-givemydata`
- Primary local database: `%LOCALAPPDATA%\GarminDataHub\garmin.db`
- Optional DB override: `GARMIN_DATA_HUB_DB`
- App-specific schema and metrics are applied after each sync
- Trackpoints are incrementally ingested from new/changed FIT archives

## Main Capabilities

- Garmin Connect sync via browser login flow
- Incremental trackpoint ingestion into `activity_trackpoint`
- Activity analysis with map and chart views
- Build Plan page with 80/20 planning workflow
- Charts and compliance views for trend analysis
- Local-first operation with SQLite storage

## Streamlit Pages (Current)

Located in `src/garmin_data_hub/ui_streamlit/pages`:

- `0_Backup_Import.py` (Garmin Sync)
- `2_Activities.py`
- `3_Build_Plan.py`
- `4_Charts.py`
- `5_Compliance.py`
- `6_8020_Help.py`
- `_1_MCP_Query.py` (currently hidden by Streamlit filename convention)

## Quick Start (Developers)

```powershell
cd Training_Planner
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -e .
streamlit run src/garmin_data_hub/ui_streamlit/app.py
```

## CLI Usage

CLI module:

```powershell
python -m garmin_data_hub.cli_backup_ingest --visible --chrome
```

Helpful options:

- `--days <N>`: limit sync window
- `--skip-trackpoints`: skip FIT trackpoint extraction
- `--rebuild-trackpoints`: rebuild all trackpoints
- `--trackpoints-max <N>`: cap processed activities
- `--db <path>`: custom DB path (filename is normalized to `garmin.db` behavior)

Project script entry point:

```powershell
garmin-sync --visible --chrome
```

## Packaging / Release

Build pipeline script:

- `packaging/build.ps1`

What it does:

- Builds Streamlit app executable directory (`GarminDataHub`)
- Builds CLI executable directory (`cli_backup_ingest`)
- Bundles `garmin-givemydata.exe` alongside app artifacts
- Organizes outputs under `release/<version>/`
- Updates `pyproject.toml` version from build `-Version`
- Optionally compiles installer via Inno Setup if `ISCC.exe` is available

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

## License

See `LICENSE`.
