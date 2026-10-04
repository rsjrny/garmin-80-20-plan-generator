# Garmin Data Hub

A local Garmin analytics desktop app built on **SQLite + NiceGUI**.

The project syncs Garmin data using `garmin-givemydata`, applies app-specific schema extensions, ingests FIT trackpoints, and refreshes cached derived metrics used across the UI.

> Garmin Connect download support in this project is powered by the open-source [`garmin-givemydata`](https://github.com/nrvim/garmin-givemydata) project. Garmin is not affiliated with or endorsing this application.

Developer documentation: [Developer guide](docs/developer-guide.md) covers setup, architecture, database contracts, testing, upstream upgrades, and releases.

## Current Architecture

- **Sync source:** `garmin-givemydata`
- **Primary local database:** `%LOCALAPPDATA%\GarminDataHub\garmin.db`
- **Optional DB override:** `GARMIN_DATA_HUB_DB`
- **Post-sync refresh:** schema updates, app-owned archive ingestion, and targeted metric/provenance refresh
- **Trackpoints:** incrementally ingested by Garmin Data Hub from new/changed FIT/GPX/TCX archives

## Main Capabilities

- Garmin Connect login on the Sync page, optional Windows Credential Manager
  storage, and visible-browser MFA
- Incremental archive ingestion into `activity_trackpoints`
- Activity analysis with map and detail views
- Derived metrics including HR zones, TRIMP/TSS, FTP estimates, and power zones
- Charts for training load, pace/power trends, and power profile analysis
- AI-first planning with a deterministic rule-based baseline/fallback
- Account-authenticated Codex CLI plan generation
- Shared local training-policy validation across baseline and AI plans
- Local-first operation with SQLite storage
- Persistent season calendars with multiple running events and A/B/C priorities

## AI Coaching Workspace

The dedicated **Codex Plan Workspace** is the recommended personalized-plan path
and supports an API-key-free, account-authenticated Codex CLI round trip:

1. Configure the event and training settings on Plan.
2. Add optional schedule, injury, strength, and nutrition constraints.
3. Generate a structured proposal through the locally installed, signed-in Codex CLI.
4. Review the plain-language summary, policy results, macros, and exact database changes.
5. Explicitly approve the plan before it is written to SQLite.

The Coach page checks Node.js, npm, the Codex CLI, and the saved Codex login on
first use. Detection runs only version and login-status checks; it never invokes
an installer. If a command is missing or broken, **Install missing tools** shows
the exact changes and requires an explicit consent checkbox.
On Windows it installs the Node.js LTS package through WinGet (which includes
npm), then installs `@openai/codex` through npm only if Codex is still missing.
Commands that pass a real `--version` check are not upgraded or replaced.

Choose **Sign in to Codex** to open the official `codex login` terminal/browser
flow, then return to the Coach page and choose **Check again**. Garmin Data Hub
does not collect or store the Codex account password, browser MFA response, or
CLI authentication file. Codex sign-in is separate from the Garmin login stored
in Windows Credential Manager.

Manual recovery commands are:

```powershell
winget install --id OpenJS.NodeJS.LTS --exact --source winget
npm install --global @openai/codex
codex login
codex login status
```

See the official [Codex CLI installation guide](https://learn.chatgpt.com/docs/codex/cli),
[Codex authentication guide](https://learn.chatgpt.com/docs/auth), and
[Node.js downloads](https://nodejs.org/en/download) if WinGet or npm is blocked
by an administrator, proxy, or offline environment.

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

The Plan page retains a deterministic rule-based generator as an offline
baseline and fallback. Choose **Generate offline baseline** to update the active
calendar without creating a file, or **Generate baseline + workbook** to also
write the configured `.xlsx` workbook. Both paths show the affected date range
for confirmation, validate the result with the local policy engine, and replace
only workouts from the plan start through the event date. A failed validation,
workbook preparation, or database transaction leaves the active plan unchanged.
The workbook button exposes **Download last workbook** after a successful export.
Offline generation needs no Node.js, npm, Codex login, or network connection.
The policy code remains authoritative; AI cannot bypass it.

AI plans include actual scheduled strength sessions in full Base, Build, and
Peak weeks. They also include one food-agnostic macro target for every plan date:
carbohydrate, protein, and fat ranges in g/kg plus optional during-training
carbohydrate in g/hour. These educational suggestions are validated, displayed
with the calendar, and stored with the accepted plan; they do not prescribe
specific foods or medical nutrition treatment. The offline baseline schedules
alternating strength sessions but does not invent individualized macro targets.

## Season calendars

Open **Seasons** to create a running season of up to 366 days, save training
availability, and add dated events with A/B/C priorities. Event changes are saved
as planning intent. Choose **Preview yearly schedule** to review a continuous
canonical schedule, phases, weekly/event workload and conflicts. For 80/20, save
LTHR and confirm it is a measured running threshold; for Maffetone, select and confirm
an adjustment. Save an explicit starting weekly duration when covered history is
unavailable. Unknown load and event-readiness limits remain visible as warnings.

Review and acknowledge warnings to apply to an empty season starting today or later.
For linked schedules, choose calculated affected range, full schedule from today, or
custom future dates. Preview shows the replacement range, additions, removals,
replacements and preserved sessions with reasons. History, completion and unresolved
activity matches stay protected. Future locked/manual sessions require explicit
per-preview replacement IDs. Infeasible merged training blocks apply.

Use **Workout protection** to lock sessions, record completion, or preview a timed
prescription edit. Edits create a new immutable revision and protect the resulting
manual occurrence. The calendar shows generated, manual, locked, completed and
preserved status. Preserved prescriptions and matches retain their original parameter
snapshots; an explicit confirmed intensity refresh applies only to new prescriptions.
Apply is atomic, audited and idempotent; stale previews require regeneration.
Rest/auxiliary sessions are typed, and cached nutrition in the replacement range
is invalidated. Applied history records ranges, changes, protection and overrides.

Use **Event preparation and tuning** in the preview to inspect effective preparation,
taper and recovery windows, plus controlled/shared/constrained preparation days.
These are calendar opportunities, not a readiness prediction. Conflict guidance shows
calendar clearance dates and links to event tuning; save changes and generate a fresh
preview before apply. An A build takes precedence over a supporting taper, while event
day and required recovery remain reserved. Short easy completion events during an A
taper need an explicit **Planned completion minutes** estimate, at most 5 km, and a
duration within the displayed easy-session budget. Performance goals remain separate.
Generated and protected events receive the same taper check, including protected
cancelled events. Applied history retains the preparation assessments reviewed.
Older generation policies recommend a full future review; history and protected
prescriptions keep their immutable origins. Rollback is separately scoped.

You can explicitly link a reviewed existing structured plan whose methodology and
entire workout history fit the season. Linking retains its workouts and matches.
After linking, single-event saves and direct revision approvals cannot overwrite
the season's dates. Archiving retains this protection. Existing unlinked single-event
plans remain available on Plan; legacy schedules require explicit conversion before
linking. Updates add empty season tables automatically on the next app startup.

## NiceGUI Pages

The primary desktop interface is implemented under `src/garmin_data_hub/ui_nicegui`:

- **Dashboard** - Garmin history, threshold, metrics-health, and upcoming-plan overview
- **Garmin Sync** - non-blocking sync, cancellation, logs, and derived-metric repair
- **Activities** - filters, splits, local GPS-track inspection with linked measurement charts and interval selection, and complete JSON export
- **Charts** - volume, load, active-plan comparison, and qualified running performance, durability and power views
- **Plan** - offline baseline/workbook generation, event settings, HR thresholds, and active calendar review
- **Seasons** - event priorities and tuning, preparation/conflict review, availability, yearly preview, protected regeneration, reviewed apply, revision history, and explicit plan linking
- **Codex Coach** - account-authenticated generation, deterministic validation, exact diff, and explicit approval
- **Compliance** - planned-versus-completed distance and duration
- **Data Query** - guarded read-only SQL and advanced garmin_mcp calls
- **Settings** - persistent distance units and activity, chart, dashboard, and sync defaults
- **Help & About** - the end-to-end workflow, support links, version, privacy, and licensing information

Charts **Plan comparison** shows weekly planned duration/distance, actual selected
TSS/TRIMP, measurement coverage and due-workout completion. Choose calendar weeks
or one plan's weeks, inspect schedule/activity evidence, and export the weekly table.
Rest and today's sessions are excluded from due-session completion; candidates are
unconfirmed and unmatched activities still contribute to actual totals. Missing
planned load stays unavailable. **Refresh data** rereads plans and reviewed matches.
See [C3 evidence](reports/charts_c3/README.md) for verification and limitations.

Charts **Performance** adds configurable pace-at-HR and HR-at-pace bands, running
efficiency, reported cadence, weekly longest-run distance/duration, qualified
decoupling/drift, and power charts. Select one running subtype; terrain and confirmed
original workout families stay separate. Each chart states its screening rules;
the measurement table and CSV include excluded activities and their reasons.
Power requires usable **running FTP** provenance and sufficient track coverage.
Cycling power cannot use that threshold. **Refresh data** rereads evidence without
recomputing metrics or changing plans. See [C4 evidence](reports/charts_c4/README.md).

## Data Query Page (Advanced)

The NiceGUI **Data Query** page provides guarded read-only SQLite access and an
advanced Model Context Protocol (MCP) tool browser. It discovers the installed
`garmin-givemydata` server's tools, descriptions, argument schemas, and defaults.
Use **Refresh tools** to reconnect. The list depends on the installed version;
common tools include:

Results are summarized locally into labeled metrics, returned status messages,
and record tables. Dated numeric records include selectable charts with latest,
average, and range statistics. Missing values are excluded from statistics.
The complete response remains available under **Raw JSON**; no AI call is needed
to produce these summaries.

#### Common MCP Tools

The exact read-only catalog is discovered from the bundled upstream runtime.
Typical tools inspect the schema, execute guarded SELECT queries, list
activities, and summarize health or trends. Use the dedicated **Garmin Sync**
page for writable synchronization.

#### Requirements

- **garmin-givemydata** must be installed in the active Python environment (included in app dependencies)
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
- **Tool discovery:** searchable installed tools with descriptions and JSON argument schemas
- **Error handling:** Timeouts, retries, and clear error messages
- **Read-only enforcement:** lookups use SQLite read-only connections and skip upstream startup migrations
- **Exact database selection:** custom filenames and sandbox databases are supported
- **No writable alternative:** Data Query does not replace the canonical Garmin
  Sync workflow

#### Troubleshooting

If MCP Query shows "sidecar unavailable":
1. Verify the dependency is installed: `python -m pip show garmin-givemydata`
2. Use **Refresh tools** and verify the selected database exists
3. Restart Garmin Data Hub

---

## Ask Coach Garmin Lookups

On **Ask Coach**, enable **Look up additional Garmin data** to let Codex select
up to three relevant MCP lookups per question. Supported lookups include recent
training status, race predictions, endurance scores, and hill scores. Enabling
**Share sleep & recovery summaries** also permits sleep, HRV, body battery,
resting heart rate, and stress lookups. Tool results are sent to Codex alongside
the existing plan context and can be inspected under the context expansion.

The app uses the upstream server's stdio transport automatically; no global
Claude or Codex configuration is required. Chat cannot request sync, arbitrary
SQL, profile/device data, or GPS tools. Failed or oversized lookups are reported
as data gaps. Changing either sharing switch clears the conversation. Ordinary
summary-only chat remains available with additional lookups off.

See the [upstream MCP instructions](https://github.com/nrvim/garmin-givemydata#connect-ai)
for connecting other MCP clients.

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
Browser mode remains bound to `127.0.0.1`; this desktop app is not exposed as a
remote web service.

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
garmin-sync --visible
```

or:

```powershell
python -m garmin_data_hub.cli_backup_ingest --visible
```

Helpful sync options:

- `--days <N>` — limit sync window
- `--db <path>` — choose the writable database directory; the filename must be
  exactly `garmin.db`

Writable sync accepts arbitrary directories but rejects alternate database
filenames. Read-only application features can still open custom SQLite
filenames. Sync is user-initiated; automatic Windows scheduled sync is not a
supported feature. The canonical flow pins `garmin-givemydata` 0.1.12, invokes
it with `--no-trackpoints`, then performs app-owned archive ingestion and
targeted metric/provenance refresh. See
[Validated Data and Sync Architecture](docs/validated-architecture.md).

For local recovery, exit the application before copying `garmin.db` and keep
the backup separate from the live data directory. The database contains Garmin
history plus app-owned settings, plans, metrics, and provenance; local activity
archives are also useful when recovering trackpoints without a new download.

### Garmin Login and MFA

On first use, the Garmin Sync page opens a one-time login editor. Enter the
Garmin Connect email and password and leave **Remember on this Windows account**
selected to protect them in Windows Credential Manager for the current Windows
user. After the login is saved, the editor is hidden and the page reports that
the Garmin session is ready.

For later syncs, click **Run sync** without re-entering either value. The Garmin
client restores its saved browser session first. The app silently retrieves the
backup login from Windows Credential Manager because the sync helper requires a
credential pair even when that browser session is still valid; it is used only
if Garmin requires a fresh sign-in. The password is never filled back into the
page or added to command-line arguments or sync logs.

Choose **Update login** only when the Garmin email or password changes. Saving an
updated login asks for confirmation before resetting the old browser session so
that its cookies cannot take precedence. The new login is not saved or used if
that reset is cancelled or fails. The same safeguard applies when **Run sync** is
used directly from the login editor. If a fresh sign-in triggers MFA, enter the
separate code in the Chrome window; the sync continues after Garmin accepts it.
Garmin Data Hub does not collect or save the MFA code.

Choose **Forget saved login** to remove the saved email/password credential.
This does not remove the separate Garmin browser profile or revoke an already
active Garmin session. Credentials are passed through the hardened sync worker,
not through command-line arguments or sync logs. Legacy plaintext `.env`
credential storage is unsupported.

The one-time editor also permits a sync without saving the login by clearing
**Remember on this Windows account**, but the editor will be needed again when no
saved credential is available. Garmin's browser session can persist separately.
Use the confirmed **Reset browser login** action to remove the local browser
profile and session-cookie backup before a fresh sign-in. Use a separate
database when changing Garmin accounts so activity data is not mixed.

## Tests

Install Chromium for the browser tests, then run the project test suite:

```powershell
python -m playwright install chromium
python -m pytest
```

Targeted regression checks used during recent sync/metrics work:

```powershell
python -m pytest tests/test_sync_progress.py tests/test_metrics_refresh.py
```

## Packaging / Release

The Windows packaging pipeline lives in `packaging/build.ps1`.

Install its pinned build toolchain with `.venv\Scripts\python.exe -m pip install -e ".[dev]"`.

Example build command:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.3.2
```

Signed installer-only release:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.3.2 -InstallerOnly -SignInstaller -CertificateThumbprint "<cert-sha1-thumbprint>"
```

`garmin-givemydata` packaging behavior:

- The supported runtime is exactly `garmin-givemydata==0.1.12`
- The default build specification is pinned to that version; use
  `-SkipGivemydataUpdate` only to validate an already installed matching pin

Examples:

```powershell
# Reproducible release build
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.3.2 -GivemydataPypiSpec "garmin-givemydata==0.1.12"

# Validate the already-installed upstream version without updating it
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 1.3.2 -SkipGivemydataUpdate
```

It will:

- build the NiceGUI desktop app directory (`GarminDataHub`)
- build the CLI directory (`cli_backup_ingest`)
- bundle the `garmin-givemydata` runtime inside the frozen sync CLI
- copy outputs under `release/<version>/`
- build a complete portable ZIP, corresponding-source archives, and SHA-256 checksums
- write `SOURCE-COMMIT.txt` so artifacts carry their source commit provenance
- optionally build the installer via Inno Setup when available
- optionally sign the installer with Authenticode

Distribute `GarminDataHub-<version>-installer.exe` or the complete portable ZIP,
not an individual executable. Public binary releases include AGPL-covered
`garmin-givemydata` code; retain the bundled license, notices, and corresponding
source files.

Before evaluating a new upstream version, use the [isolated upgrade checker](docs/givemydata-upgrade-check.md). It backs up the database, compares candidate schemas, checks app compatibility on copies, and can optionally test a real Garmin sync.

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
- Validated sync/database architecture: `docs/validated-architecture.md`

## License

See `LICENSE`.
