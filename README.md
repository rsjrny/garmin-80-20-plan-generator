# Garmin Data Hub

A local Garmin analytics desktop app built on **SQLite + NiceGUI**.

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
- AI-first planning with a deterministic rule-based baseline/fallback
- Account-authenticated Codex CLI plan generation
- Shared local training-policy validation across baseline and AI plans
- Local-first operation with SQLite storage

## AI Coaching Workspace

The dedicated **Codex Plan Workspace** is the recommended personalized-plan path
and supports an API-key-free, account-authenticated Codex CLI round trip:

1. Configure the event and training settings on Build Plan.
2. Add optional schedule, injury, strength, and nutrition constraints.
3. Generate a structured proposal through the locally installed, signed-in Codex CLI.
4. Review the plain-language summary, policy results, macros, and exact database changes.
5. Explicitly approve the plan before it is written to SQLite.

The packet includes summarized activities and bounded plan context. It excludes
GPS routes, raw trackpoints, exact activity times, device identifiers, file
paths, and raw Garmin JSON. The automated path starts `codex exec` in a read-only,
ephemeral, isolated directory and reuses the user's saved Codex/ChatGPT CLI login.
Garmin Data Hub does not use the OpenAI SDK, make a direct API call, read an API
key, or save authentication in SQLite.

Workspace constraints and the selected training-history window are saved in the
local SQLite settings database and restored when the application is reopened.

Accepted plans replace only their declared future date window. The save is one
atomic transaction: previous state is archived in `plan_import_history`, exact
duration/distance/TSS values are written to `planned_workout`, and Plan Review's
active snapshot is updated. A failed write is rolled back without a partial
calendar change.

## Planning Architecture

The application uses an **AI-first, rules-governed** planning model:

1. Garmin history, the active plan, event settings, and athlete preferences are
   assembled into a privacy-minimized coaching packet.
2. Codex CLI proposes and explains a
   structured plan.
3. A local deterministic policy engine checks the proposal before it can be
   saved. Checks include race-day correctness, daily and weekly session limits,
   age-based hard-session limits, hard-day separation, run-volume progression,
   workload caps, strength frequency, long-session placement, and known-duration
   80/20 distribution.
4. The athlete reviews a plain-language summary and exact database differences.
5. An approved plan is saved atomically with provenance and a recovery snapshot.

The Build Plan page retains a deterministic rule-based generator as an offline
baseline and fallback. Baselines pass through the same policy engine and are
clearly identified in Plan Review. The policy code remains authoritative; AI
cannot bypass it.

AI plans include actual scheduled strength sessions in full Base, Build, and
Peak weeks. They also include one food-agnostic macro target for every plan date:
carbohydrate, protein, and fat ranges in g/kg plus optional during-training
carbohydrate in g/hour. These educational suggestions are validated, displayed
with the calendar, and stored with the accepted plan; they do not prescribe
specific foods or medical nutrition treatment. The offline baseline schedules
alternating strength sessions but does not invent individualized macro targets.

## NiceGUI Pages

The primary desktop interface is implemented under `src/garmin_data_hub/ui_nicegui`:

- **Dashboard** - Garmin history, threshold, metrics-health, and upcoming-plan overview
- **Garmin Sync** - non-blocking sync, cancellation, logs, and derived-metric repair
- **Activities** - filters, splits, local GPS-track inspection, and complete JSON export
- **Charts** - volume, heart-rate, speed, distribution, and training-load trends
- **Plan** - event/schedule settings, HR thresholds, and active calendar review
- **Codex Coach** - account-authenticated generation, deterministic validation, exact diff, and explicit approval
- **Compliance** - planned-versus-completed distance and duration
- **Data Query** - guarded read-only SQL and advanced garmin_mcp calls
- **Settings** - persistent distance units and activity, chart, dashboard, and sync defaults
- **Guide** - the end-to-end local workflow

## Data Query Page (Advanced)

The NiceGUI **Data Query** page provides guarded read-only SQLite access and an
advanced Model Context Protocol (MCP) tool runner. Six common tools are offered
initially, and any installed `garmin_mcp` tool name can be entered directly:

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

- **Dual-mode interface:** MCP tool runner or raw SQL mode
- **Extensible tools:** type any installed `garmin_mcp` tool name
- **Error handling:** Timeouts, retries, and clear error messages
- **Read-only enforcement:** SQL validation blocks INSERT, DELETE, DROP, and DDL

#### Troubleshooting

If MCP Query shows "sidecar unavailable":
1. Verify garmin_mcp is installed: `pip list | grep garmin-mcp`
2. Test sidecar manually: `python -m garmin_mcp`
3. Restart Garmin Data Hub

---

## Quick Start (Developers)

```powershell
cd Training_Planner
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -e .[dev]
garmin-data-hub
```

## Launch Options

### Primary NiceGUI desktop UI

```powershell
garmin-data-hub
```

NiceGUI opens in a native Windows window and uses the active database by default.
Use `--browser` for a normal browser window or `--sandbox` to copy the active
database to `%LOCALAPPDATA%\GarminDataHub\nicegui-preview\garmin-preview.db`.

```powershell
garmin-data-hub --sandbox
garmin-data-hub --browser
```

`--source-db` selects an explicit live SQLite path; `--preview-db` selects an
explicit sandbox copy. Codex generation runs in a cancellable background worker,
while stale-response checks, locked settings, the local training policy, exact
database changes, and explicit acknowledgement remain mandatory.

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

- build the NiceGUI desktop app directory (`GarminDataHub`)
- build the CLI directory (`cli_backup_ingest`)
- bundle `garmin-givemydata.exe` from project `.venv` with the release artifacts
- copy outputs under `release/<version>/`
- optionally build the installer via Inno Setup when available

## Project Requirements

- Python `>=3.10`
- Windows is the primary supported environment

Core dependencies are defined in `pyproject.toml`, including:

- `nicegui`
- `pywebview`
- `plotly`
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
