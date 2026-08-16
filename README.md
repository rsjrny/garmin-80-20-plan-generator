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
- Manual ChatGPT coaching-packet export and validated plan import
- Local-first operation with SQLite storage

## Manual ChatGPT Plan Exchange

The **Build Plan** page supports a local, API-free round trip:

1. Configure the event and training settings.
2. Add optional schedule, injury, strength, and nutrition constraints.
3. Download the privacy-minimized coaching packet and upload it to ChatGPT.
4. Copy the included prompt and ask ChatGPT to return the required JSON plan.
5. Upload that response to Build Plan for validation and a change preview.
6. Explicitly approve the plan before it is written to SQLite.

The packet includes summarized activities and bounded plan context. It excludes
GPS routes, raw trackpoints, exact activity times, device identifiers, file
paths, and raw Garmin JSON. Garmin Data Hub makes no ChatGPT or OpenAI API call
and does not require an API key.

Accepted plans replace only their declared future date window. The save is one
atomic transaction: previous state is archived in `plan_import_history`, exact
duration/distance/TSS values are written to `planned_workout`, and Plan Review's
active snapshot is updated. A failed write is rolled back without a partial
calendar change.

## Streamlit Pages

Located in `src/garmin_data_hub/ui_streamlit/pages`:

- `0_Backup_Import.py` — Garmin sync, progress, and derived-refresh diagnostics
- `1_Plan_Review.py` — plan review and training planning
- `2_Compliance.py` — compliance and zone analysis
- `3_Past_Activities.py` — activity browsing and analysis
- `4_Charts.py` — trend and power/load charts
- `5_Build_Plan.py` — deterministic planning and manual ChatGPT plan exchange
- `6_8020_Help.py` — 80/20 training methodology guidance
- `7_MCP_Query.py` — **Advanced:** MCP tool console for database queries and Garmin data access (requires garmin_mcp sidecar)

### MCP Query Page (Advanced)

The **MCP Query** page (page 7) provides advanced database access through six Model Context Protocol (MCP) tools:

#### Available MCP Tools

1. **garmin_schema** — View database schema (tables, row counts, columns)
2. **garmin_query** — Execute read-only SELECT queries with preset templates
3. **garmin_health_summary** — Fetch daily health metrics (HR, stress, body battery) by date range
4. **garmin_activities** — List activities with filtering by type, date, and limit
5. **garmin_trends** — Retrieve metric trends (weekly or monthly aggregation)
6. **garmin_sync** — Trigger manual sync and view sync status

#### Requirements

- **garmin_mcp** package must be installed in the active Python environment
- Sidecar subprocess spawned automatically on page load
- If sidecar is unavailable, the page shows diagnostics and recovery steps

#### Usage Examples

- Query the database schema: "Show me all tables and row counts"
- Find recent activities: "List my last 10 running activities with distance and duration"
- Analyze health trends: "Plot my resting heart rate trend over the last month"
- Retrieve daily metrics: "Get my stress and body battery for the past 7 days"
- Execute custom SQL: Use preset queries or write custom SELECT queries

#### Features

- **Dual-mode interface:** MCP tools dropdown or raw SQL mode
- **Natural language routing:** Type a question and the UI auto-selects the best tool
- **Result export:** Download results as CSV, view structured JSON
- **AI summaries:** Automatic narrative interpretation of results
- **Error handling:** Timeouts, retries, and clear error messages
- **Read-only enforcement:** SQL validation blocks INSERT, DELETE, DROP, and DDL

#### Troubleshooting

If MCP Query shows "sidecar unavailable":
1. Verify garmin_mcp is installed: `pip list | grep garmin-mcp`
2. Test sidecar manually: `python -m garmin_mcp`
3. Restart the Streamlit app

---## Quick Start (Developers)

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

`garmin-givemydata` packaging behavior:

- Build uses PyPI package install/upgrade into `.venv`
- Use `-GivemydataPypiSpec` to pin a specific PyPI version when needed

Examples:

```powershell
# Default (upgrades PyPI package in .venv)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0

# PyPI with explicit version
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0 -GivemydataPypiSpec "garmin-givemydata==0.1.10"
```

It will:

- build the Streamlit app directory (`GarminDataHub`)
- build the CLI directory (`cli_backup_ingest`)
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
