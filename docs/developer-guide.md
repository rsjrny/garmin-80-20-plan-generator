# Developer guide

This guide is the starting point for maintaining Garmin Data Hub. It describes
the implemented application, its data contracts, and the workflow for changing
and releasing it. Design plans and historical reports provide context; current
code and regression tests remain authoritative.

Verified against the repository on **2026-10-03**:

| Contract | Current value | Source |
| --- | --- | --- |
| Application version | 2.1.1 | [pyproject.toml](../pyproject.toml) |
| Python requirement | >=3.10 | [pyproject.toml](../pyproject.toml) |
| Supported upstream runtime | garmin-givemydata==0.1.12 | Project dependency and [sync CLI](../src/garmin_data_hub/cli_backup_ingest.py) |
| Application schema version | 15 | [db/migrate.py](../src/garmin_data_hub/db/migrate.py) |
| Primary platform | Windows | Desktop UI and packaging pipeline |
| Local UI listener | 127.0.0.1:8090 by default | [ui_nicegui/app.py](../src/garmin_data_hub/ui_nicegui/app.py) |

Update this table when changing those contracts.

## Contents

- [Set up and run](#set-up-and-run)
- [Architecture and source map](#architecture-and-source-map)
- [Database ownership and compatibility](#database-ownership-and-compatibility)
- [Schema migrations](#schema-migrations)
- [Synchronization and archive ingestion](#synchronization-and-archive-ingestion)
- [Metrics and provenance](#metrics-and-provenance)
- [Planning and seasons](#planning-and-seasons)
- [Credentials and external processes](#credentials-and-external-processes)
- [Testing and change workflow](#testing-and-change-workflow)
- [Checking an upstream upgrade](#checking-an-upstream-upgrade)
- [Packaging and release](#packaging-and-release)
- [Troubleshooting](#troubleshooting)
- [Maintaining this documentation](#maintaining-this-documentation)

## Set up and run

Run these commands in PowerShell from the repository root:

~~~powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
~~~

Use the environment's Python explicitly; activation is optional. The dev extra
installs pytest and the pinned packaging toolchain. Core dependencies include
NiceGUI, pywebview, pandas, NumPy, Plotly, openpyxl, FIT/GPX parsers, and the
supported upstream package. See [pyproject.toml](../pyproject.toml).

Start development with a copied database:

~~~powershell
.\.venv\Scripts\garmin-data-hub.exe --sandbox --browser
~~~

For an explicit source and preview destination:

~~~powershell
.\.venv\Scripts\garmin-data-hub.exe --sandbox --browser --source-db "C:\path\to\garmin.db" --preview-db ".tools\developer-preview\garmin-preview.db"
~~~

Sandbox startup uses a SQLite snapshot and applies app schema to the copy.
Opening the normal UI without --sandbox uses the selected live database and
can apply migrations. A preview is a writable copy, not a read-only session.
The normal writable sync requires the filename garmin.db, so a preview named
garmin-preview.db cannot use that sync path.

Use --browser for browser development; omit it for the native Windows window.
Use --port to change the local listener. The listener remains on 127.0.0.1.

| Entry point | Purpose |
| --- | --- |
| garmin-data-hub | Primary NiceGUI application |
| garmin-nicegui-preview | Alias of the same application entry point; pass --sandbox explicitly |
| garmin-sync | Supported Garmin download, archive ingestion, and refresh flow |
| garmin-reconcile-trackpoints | Local historical archive reconciliation; dry-run by default |

These console scripts are declared in pyproject.toml.

### Database paths

[paths.py](../src/garmin_data_hub/paths.py) resolves the default database in this order:

1. GARMIN_DATA_DIR selects a directory containing garmin.db.
2. GARMIN_DATA_HUB_DB selects an explicit SQLite path.
3. The default is %LOCALAPPDATA%\GarminDataHub\garmin.db.

The UI's --source-db and the sync CLI's --db select explicit paths. When both
environment variables are set, GARMIN_DATA_DIR wins for default resolution.
Writable sync accepts arbitrary directories but requires the exact filename
garmin.db. Data inspection can use alternate SQLite filenames.

Archives normally live in the database directory's fit subdirectory. The UI
sync log is logs\garmin_sync_latest.log beside the selected database.

## Architecture and source map

Garmin Data Hub is a local desktop application backed by SQLite. UI handlers
call application services and query helpers. Garmin access runs in a child
process; archive ingestion and analytics refresh run after upstream succeeds.

~~~mermaid
flowchart TD
    UI["NiceGUI desktop or browser UI"] --> Services["Planning and query services"]
    Services --> DB[("garmin.db")]
    UI --> Job["SyncJob"]
    Job --> CLI["App sync CLI"]
    CLI --> Upstream["Pinned givemydata worker"]
    Upstream --> DB
    CLI --> Archives["App archive ingestion and reconciliation"]
    Archives --> DB
    CLI --> Metrics["Threshold and metric refresh"]
    Metrics --> DB
~~~

| Location | Responsibility |
| --- | --- |
| [ui_nicegui/](../src/garmin_data_hub/ui_nicegui/) | Pages, workspace review, data adapters, background job lifecycle |
| [services/](../src/garmin_data_hub/services/) | Plan persistence, generation, season operations, thresholds, credentials |
| [plan_methodology/](../src/garmin_data_hub/plan_methodology/) | Canonical plan domain, prescriptions, methodology policy, immutable revisions, activity matches |
| [db/](../src/garmin_data_hub/db/) | Connections, schema, migrations, calendar-day semantics, queries |
| [ingest/](../src/garmin_data_hub/ingest/) | FIT/GPX/TCX parsing, archive identity, trackpoint writes, reconciliation |
| [analytics/](../src/garmin_data_hub/analytics/) | Derived metrics, threshold refresh, training load, charts |
| [exports/](../src/garmin_data_hub/exports/) | Workbook/PDF and training-plan export support |
| [cli_backup_ingest.py](../src/garmin_data_hub/cli_backup_ingest.py) | Canonical writable synchronization orchestrator |
| [cli_archive_reconcile.py](../src/garmin_data_hub/cli_archive_reconcile.py) | Local archive maintenance command |
| [mcp_sidecar_client.py](../src/garmin_data_hub/mcp_sidecar_client.py) | Upstream MCP integration for read-only Garmin queries |
| [scripts/](../scripts/) | Developer and maintenance utilities; inspect each before use |
| [tests/](../tests/) | Active pytest suite |
| [packaging/](../packaging/) | Frozen runtime and Windows installer build |

The configured pytest root is tests/, not the separate test/ directory.
Implementation roadmaps and reports describe particular work stages and should
not be treated as the current runtime specification.

## Database ownership and compatibility

**App-owned tables and upstream tables share the same garmin.db file. Separate
ownership does not provide full schema isolation.**

| Owner | Representative tables | Purpose |
| --- | --- | --- |
| garmin-givemydata | activity, activity_splits, daily_summary, sleep, other Garmin source tables | Downloaded source summaries and health data |
| Garmin Data Hub | activity_trackpoints | App-managed per-sample archive data |
| Garmin Data Hub | activity_metrics, athlete_profile, threshold_calculation | Cached analytics and threshold provenance |
| Garmin Data Hub | planned_workout, training_plan, plan_revision, plan_revision_workout, plan_workout_segment | Calendar projection and canonical plan revisions |
| Garmin Data Hub | activity_workout_match, legacy_plan_conversion, legacy_plan_conversion_source_workout | Explicit matches and legacy plan conversion provenance |
| Garmin Data Hub | season_plan, season_event, season_revision_application, season_workout_state, plan_revision_workout_origin | Season intent, applications, protection, and workout origin |
| Garmin Data Hub | app_settings, plan_import_history, archive_reconciliation, schema_migrations | Settings, recovery history, ingestion ledger, migration state |

[db/schema.sql](../src/garmin_data_hub/db/schema.sql) defines app structures.
The active_planned_workout view selects the calendar projection used by app
queries; code should not assume every planned_workout row is active.

App tables such as activity_trackpoints, activity_workout_match, and
archive_reconciliation reference upstream activity(activity_id). App queries
also read upstream columns directly. Some helpers inspect available columns and
fall back to NULL; that behavior is not universal, and it cannot detect changed
units or meaning. Removing or renaming upstream fields can cause errors or empty
UI results.

The exact dependency pin and runtime version guard prevent the supported app
sync from adopting an unreviewed upstream version. They do not protect a database
that another tool has already migrated. A separately installed Python package
also does not replace the upstream code bundled in the frozen application.

Use [the upgrade checker](givemydata-upgrade-check.md) before changing the pin.
A future design could normalize upstream data into stable app-owned tables
behind one compatibility adapter. That stronger isolation is not implemented.

## Schema migrations

Use [apply_schema()](../src/garmin_data_hub/db/migrate.py) rather than replaying
schema.sql manually against an existing database. It coordinates version records,
legacy repairs, and current structures. Migration state is stored in
schema_migrations; CURRENT_SCHEMA_VERSION is currently 15.

| Versions | Main evolution |
| --- | --- |
| 1–5 | Baseline tables, profile/metric extensions, trackpoint cascade correction, import history |
| 6–8 | Metric freshness, threshold provenance, historical archive reconciliation |
| 9–11 | Immutable plan revisions, explicit activity matches, legacy conversion |
| 12–14 | Season intent, application audit, protection and workout origin |
| 15 | Explicit participation duration for completion events |

Connections from [connect_sqlite()](../src/garmin_data_hub/db/sqlite.py) use SQLite
row objects, foreign key enforcement, WAL journaling, and NORMAL synchronous
mode. Use existing helpers so connection behavior remains consistent.

When changing schema:

1. Update current definitions and add a versioned migration.
2. Preserve existing app and upstream data. Prefer additive changes where possible.
3. Keep fresh and upgraded databases structurally consistent.
4. Test repeated initialization, realistic older schemas, failed migration rollback,
   version-record rollback, and foreign key integrity.
5. Update the schema version and relevant documentation together.

Version 7 onward uses savepoints around a migration and its version record.
Do not assume every historical migration is one atomic transaction. Legacy
trackpoint repair can recreate a table: upstream INSERT OR REPLACE on activity
historically interacted with ON DELETE CASCADE and deleted trackpoints.
Preserve the current protective behavior when changing this area.

## Synchronization and archive ingestion

The [validated sync architecture](validated-architecture.md) documents the
supported flow. The main implementation is cli_backup_ingest.run_sync():

1. Validate the database filename and allowed upstream arguments.
2. Require exactly upstream version 0.1.12 in the selected runtime.
3. Snapshot local archive identities and run upstream with --no-trackpoints.
4. After upstream succeeds, apply app schema.
5. Parse new/changed archives through the app-owned identity-validated lane.
6. Reconcile eligible historical backlog using the durable ledger.
7. Refresh threshold/profile data and targeted metrics, with a bounded stale-row top-off.
8. Report success, historical warnings, or a failing exit code.

In source mode, SyncJob starts the current Python interpreter's app CLI module.
In a frozen build it locates the packaged sync helper. The upstream worker runs
separately in both cases. Upstream failure skips app post-sync work.

Archive replacement requires validated activity identity. Ambiguous identities
and disappearing archives fail the changed-archive path. Historical
reconciliation records outcomes and fingerprints so unchanged archives do not
need repeated parsing. Existing populated activities receive protective handling.

A typical source sync command is:

~~~powershell
.\.venv\Scripts\python.exe -m garmin_data_hub.cli_backup_ingest --db "C:\path\to\garmin.db" --visible --days 7
~~~

Run it against the intended account and database. Normal sync is user-initiated;
automatic Windows scheduled synchronization is not supported.

Local historical inspection does not log in to Garmin:

~~~powershell
.\.venv\Scripts\garmin-reconcile-trackpoints.exe --db "C:\path\to\garmin.db" --fit-dir "C:\path\to\fit"
~~~

That command defaults to dry-run. --apply writes trackpoints and ledger rows;
--force re-evaluates unchanged terminal fingerprints. Review the dry-run before
choosing those modes.

Legacy standalone ingestion/rebuild callables and unsupported upstream utility
modes are quarantined. New write paths should use the supported orchestrator and
existing ingestion services.

## Metrics and provenance

Read [activity metrics data lineage](activity-metrics-data-lineage.md) before
adding a chart, changing a formula, or writing activity_metrics.

activity_metrics combines copied summary values and app-derived values.
Refresh rebuilds the columns it owns from current sources within its transaction.
Missing data becomes NULL rather than inheriting an old value. Numeric zero
remains distinct from missing data.

Metric rows record refresh and threshold provenance. Automatic threshold
calculations and manual overrides remain separate. Preserve that distinction
when presenting an effective HR or power threshold.

Calendar-day queries use db/activity_dates.py. A usable recorded
start_time_local date is authoritative; its offset is not converted to the
computer's timezone. start_time_gmt supplies the UTC date when the local value
is unavailable or invalid.

Elapsed-time integration, missing samples, power peaks, zones, and stale
provenance have dedicated regression coverage. Reuse those semantics rather
than introducing a second calculation in a page or export.

## Planning and seasons

There are related persistence paths with different histories:

- The workspace/import path validates an AI proposal and saves an approved date
  window with recovery/import history through services/plan_persistence.py.
- The canonical methodology path stores immutable approved revisions and their
  workouts/segments through plan_methodology/revision_repository.py.
- Season services combine persistent event intent, generated revisions, application
  audit, and protected workout state.

Do not treat those paths as interchangeable or update revision tables directly.

The deterministic baseline and AI proposals use local training-policy checks.
Generation is separate from approval. A proposal must pass validation and the
user's review before persistence. Preserve stale-state checks, bounded replacement
windows, recovery state, and transaction rollback.

Canonical revisions capture methodology, athlete, parameter, and goal snapshots.
Approvals create revisions; approved revisions are not edited in place.
Activity/workout matches have their own confirmation and immutability rules.

Season changes are previewed before apply. Historical, completed, and unresolved
matched workouts remain protected; future manual/locked occurrences have explicit
override rules. Apply is transactional, audited, and idempotent. Revalidate stale
previews rather than applying an old result to changed planning state.

Read [yearly scheduling architecture](yearly-scheduling-architecture.md) for
affected ranges, event conflicts, origin lineage, protected sessions, and migration
decisions. Runtime compliance lives in plan_methodology/runtime_compliance.py;
methodology policy belongs in that domain layer rather than UI handlers.

## Credentials and external processes

Garmin credentials can be remembered in Windows Credential Manager for the
current Windows user. Browser profiles/session cookies are separate state.
Forgetting a saved password does not remove a browser session; resetting browser
login is an explicit operation. Use a separate database when changing accounts.

The canonical worker uses a restricted environment, transient credentials, and
sanitized output. Passwords are not placed on command lines or saved to SQLite.
The logging boundary prevents normal upstream debug.log creation. Retain these
protections when changing subprocess behavior.

Codex generation uses the user's separately authenticated CLI and a minimized
coaching packet. The integration does not use a direct OpenAI SDK/API-key path.
Keep Garmin credentials, raw Garmin JSON, routes, and device identifiers out of
that packet. Local proposal validation remains authoritative.

UI background jobs use process-tree ownership and cancellation. Reuse
ui_nicegui/process_tree.py so browser or generation descendants do not survive
cancellation or shutdown. The Data Query integration is read only and is not an
alternate synchronization or planning-write mechanism.

## Testing and change workflow

Install the dev extra, then run the suite from the repository root:

~~~powershell
.\.venv\Scripts\python.exe -m pytest
~~~

Select coverage according to the changed behavior:

| Area | Representative tests |
| --- | --- |
| Migrations | test_schema_migrations.py, test_season_migrations.py, test_season_refinement.py |
| Upstream pin and upgrades | test_phase3m9b_failure_contracts.py, test_phase3m9c_runtime_enforcement.py, test_givemydata_upgrade_check.py |
| Sync/process lifecycle | test_sync_job_lifecycle.py, test_sync_process_tree.py, test_phase3e1_failure_contracts.py |
| Archives | test_phase3m5a_identity_and_gpx.py, test_phase3m6a_baseline_idempotency.py |
| Analytics | test_metrics_refresh.py, test_temporal_metrics.py, test_activity_calendar_days.py |
| Plans and matches | test_plan_persistence.py, test_plan_revision_persistence.py, test_activity_workout_match.py |
| Seasons | test_season_generation.py, test_season_regeneration.py, test_season_refinement.py |
| Frozen runtime | test_frozen_sync_packaging.py, test_phase3h1_bundled_runtime_diagnostic.py |

For example, an upstream compatibility change starts with:

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests/test_givemydata_upgrade_check.py tests/test_phase3m9b_failure_contracts.py tests/test_phase3m9c_runtime_enforcement.py
~~~

Prefer temporary SQLite databases and meaningful failure cases. Mock true
external boundaries, such as Garmin access and generation subprocesses, while
exercising app persistence and validation logic. Test fresh and upgraded schema
paths for database changes. See [Unit Test Strategy](../Unit_test_strategy.md).

Browser tests have environment requirements defined in their files; inspect them
before running. Unit tests alone do not validate real Garmin login, full archive
downloads, native-window behavior, or installer upgrades.

For each change, locate the owning service/domain, reproduce the behavior on a
copy, make the change, run relevant regressions, and review the UI when affected.
Update docs with changed contracts. Do not use a personal production database as
a test fixture.

## Checking an upstream upgrade

Use [scripts/check_givemydata_upgrade.py](../scripts/check_givemydata_upgrade.py)
with a candidate installed in a separate environment. The
[upgrade checker guide](givemydata-upgrade-check.md) includes environment setup,
commands, outputs, and exit codes.

An offline check:

~~~powershell
.\.venv\Scripts\python.exe scripts\check_givemydata_upgrade.py --db "C:\path\to\garmin.db" --candidate-python ".tools\givemydata-candidate\Scripts\python.exe"
~~~

The checker creates a consistent backup, compares a fresh candidate schema and
a migrated copy, checks app-row preservation and activity identities, and runs
app checks on another copy. It records whether the candidate version differs
from the supported version. It runs when invoked; it is not an update monitor.

--sync adds a real summary-ingestion test after offline checks pass. It requires
saved Garmin credentials and uses --no-trackpoints and --no-files. It requires a
new successful sync-log entry and flags upstream error logs. It does not validate
the complete archive-download/ingestion pipeline.

A passing result is checks_passed_review_required. Review schema additions,
changed semantics, release notes, real sync/archive behavior, UI behavior, and
the wider test suite before changing the supported version. The checker does not
install an upgrade or modify the production pin/database. Copies and reports
contain personal data and should remain private.

## Packaging and release

[Packaging Pipeline](../Packaging_Pipeline.md) describes the Windows build.
The declared app version is 2.1.1 at this guide's verification date; older command
examples elsewhere may name earlier releases. Select the intended release version
explicitly. For a build using the version currently declared in pyproject.toml:

~~~powershell
$projectVersion = (Select-String -Path .\pyproject.toml -Pattern '^version = "([^"]+)"$').Matches[0].Groups[1].Value
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version $projectVersion -GivemydataPypiSpec "garmin-givemydata==0.1.12"
~~~

The build updates project version metadata and recreates build/release staging
outputs. It creates the NiceGUI runtime, frozen sync helper, complete portable
distribution, corresponding-source archives, provenance, and checksums. Inno
Setup and signing tools enable optional installer/signing steps.

Validate public help and the safe bundled-runtime diagnostic:

~~~powershell
.\release\<version>\cli_backup_ingest\cli_backup_ingest.exe --help
.\release\<version>\cli_backup_ingest\cli_backup_ingest.exe --_check-bundled-givemydata
~~~

Replace <version> with the built version. The diagnostic checks bundled imports
and the exact upstream version without authentication or synchronization.
--help on the private upstream worker is intentionally unsupported.

Also check packaged UI startup, real sync, schema resource inclusion, complete
distribution contents, source commit provenance, checksums, and installer upgrade
behavior. Distribute the complete installer or portable ZIP rather than a lone
executable. Retain the project licenses, upstream notices, and corresponding
source described by the packaging documentation.

## Troubleshooting

| Symptom | First check |
| --- | --- |
| Version mismatch prevents sync | Check the selected interpreter and installed upstream version; use the upgrade audit instead of bypassing the guard |
| Empty activity/chart results | Confirm the selected database, activity schema, and metric freshness; some query helpers return empty fallbacks |
| Foreign key or migration failure | Reproduce on a consistent backup, inspect schema_migrations and the failing migration, and check foreign key integrity |
| Post-sync failure | Inspect the log beside the selected database and distinguish upstream, archive, and metric stages |
| Packaged app cannot sync | Confirm the complete distribution contains the helper; run the bundled-runtime diagnostic |
| Preview appears different from live data | Confirm source/preview paths and when the snapshot was created |
| Unexpected account session | Inspect remembered credentials and the separate browser login state |
| Generation cannot start | Use the workspace prerequisite/login status checks; local baseline generation remains available |

Create consistent backups through SQLite's backup API, as the preview and upgrade
checker do. A live WAL database cannot be recovered reliably by copying only its
main file. For manual recovery, stop the app and active writers first and retain
a verified backup outside the live directory. Keep local archives when possible;
they can restore trackpoints without a fresh download. Restore and inspect a copy
before replacing live data.

## Maintaining this documentation

Update the guide in the same change as a new schema version, supported upstream
pin, entry point, path rule, write boundary, or packaging contract. Keep details
in their owning references and link them here:

- [README](../README.md): user-facing overview and quick start.
- [Validated architecture](validated-architecture.md): sync/database contracts.
- [Metrics lineage](activity-metrics-data-lineage.md): field sources and refresh semantics.
- [Scheduling architecture](yearly-scheduling-architecture.md): canonical seasons and protection.
- [Upgrade checker](givemydata-upgrade-check.md): candidate-version audit workflow.
- [Test strategy](../Unit_test_strategy.md): regression conventions.
- [Packaging pipeline](../Packaging_Pipeline.md): release operations.
