# Validated Data and Sync Architecture

This document records the supported writable Garmin data path and the database
contracts that other documentation should reference. Current code and tests are
authoritative if this document ever becomes stale.

## Supported Windows operation

Garmin Data Hub is a local Windows desktop application. Normal synchronization
is started by the user from **Garmin Sync** or by invoking the packaged sync CLI.
Automatic Windows scheduled synchronization is not part of the supported
design.

The canonical writable flow is:

1. NiceGUI's `SyncJob` starts the app sync CLI using the current Python
   interpreter in source mode or the packaged helper in a frozen build.
2. The CLI invokes pinned `garmin-givemydata` 0.1.12 with `--no-trackpoints`.
3. Garmin Data Hub detects new or changed local activity archives and performs
   app-owned FIT/GPX/TCX parsing and targeted trackpoint replacement.
4. Eligible historical archives are accounted for through the durable
   reconciliation ledger without replacing already populated activities.
5. The app refreshes the athlete profile and targeted metric/provenance rows,
   including a bounded top-off for rows still known to be stale.

This ordering is deliberate: upstream owns Garmin summary ingestion, while the
application owns archive identity validation, `activity_trackpoints`, derived
metrics, and refresh provenance. Upstream trackpoint rebuilding and legacy
standalone rebuild/import callables are quarantined and unsupported as normal
operational paths. The Data Query MCP connection is for read-only inspection;
it is not an alternate writable synchronization path.

Writable synchronization accepts an arbitrary directory but requires the exact
database filename `garmin.db`. A different filename is rejected before Garmin
access. Read-only application features may open custom SQLite filenames.

## Database ownership and migrations

The current application schema version is **15**, defined by
`CURRENT_SCHEMA_VERSION` in `src/garmin_data_hub/db/migrate.py`. Migrations are
recorded in `schema_migrations`. See the [Developer guide](developer-guide.md)
for setup, schema evolution, planning, testing, and release workflows.

- `garmin-givemydata` owns Garmin source and summary-ingestion tables such as
  `activity` and `activity_splits`.
- Garmin Data Hub owns its analytics and provenance extensions, including
  `activity_trackpoints`, `activity_metrics`, `athlete_profile`, threshold
  provenance, planning tables, and `archive_reconciliation`.
- App migrations should remain additive where practical. Do not change an
  upstream-owned structure without a demonstrated need and compatibility
  evidence.

App-owned and upstream-owned tables share the same `garmin.db` file. App
foreign keys and direct queries depend on upstream activity IDs and columns,
so separate ownership does not provide full schema isolation. The exact
upstream version pin and runtime guard prevent unreviewed versions from running
through supported sync; they do not protect a database migrated by another
tool. Use the [upgrade checker](givemydata-upgrade-check.md) before changing
the supported pin.

The reconciliation ledger gives each eligible historical archive a durable
outcome. A completed baseline lets routine sync avoid repeatedly parsing an
unchanged historical backlog; new and changed archives still use the strict
identity-validated ingestion lane.

## Automatic thresholds

Calculated values and manual overrides retain distinct provenance. An override
changes the effective value without being represented as an automatic result.

- **HRmax:** highest candidate from 100 through 220 bpm that has at least five
  seconds of measured trackpoint HR within 2 bpm. Evidence becomes stale after
  90 days.
- **Estimated LTHR:** `round(validated HRmax * 0.86)`, linked to its HRmax
  calculation. Evidence becomes stale after 90 days.
- **Running FTP:** `round(highest complete exact 1200-second running power peak
  * 0.95)`. Only running, trail-running, and indoor-running activities are
  candidates. Evidence becomes stale after 180 days.
- **Resting HR:** rounded median of the latest seven available Garmin calendar
  dates. `daily_summary` wins over `sleep` for the same date. Evidence becomes
  stale after 14 days. When no usable Garmin value exists, metric calculations
  use the explicit default of 60 bpm.

See [Activity Metrics Data Lineage](activity-metrics-data-lineage.md) for the
elapsed-time metric definitions and cache-provenance behavior.

## Activity calendar day

For a valid ISO-like `start_time_local`, its recorded `YYYY-MM-DD` prefix is the
canonical activity day; embedded offsets are not converted through UTC or the
computer's timezone. If the local value is absent or malformed, a valid
`start_time_gmt` supplies its UTC date. If neither value is usable, the activity
day is `NULL`.

## Credential and logging boundary

Credentials are not stored in the repository. The supported GUI can protect a
remembered Garmin login in Windows Credential Manager. The canonical worker
passes only the minimal practical environment, exposes credentials transiently
inside the pinned upstream process, rejects unsupported upstream arguments, and
sanitizes diagnostic output. Its logging boundary prevents the upstream runtime
from creating `debug.log` in the canonical flow.

The packaged runtime check is:

```powershell
cli_backup_ingest.exe --_check-bundled-givemydata
```

This is an internal build/runtime diagnostic. It does not authenticate, open a
browser, access the database or archives, or synchronize. The internal upstream
worker accepts only its sync allowlist; rejection of unsupported arguments such
as `--help` is intentional. Use top-level `cli_backup_ingest.exe --help` only
for the public CLI's own usage text.
