# Y3 verification and handoff

Date: 2026-10-02 (America/New_York). User authorized Y3 from the Y2 handoff.
Implementation is local and uncommitted. The source database and live schedule were
not changed; no push, deployment or packaged executable rebuild occurred.

## Delivered behavior

- Calculated affected, full-from-today and custom future replacement ranges, with
  old/new windows, Monday-week/neighbor expansion and visible clipping reasons.
- Protection of history, origin-confirmed/explicit completion and unresolved matches;
  future locked/manual overrides are explicit occurrence IDs.
- Complete merged revisions, capacity reserved for fixed workouts/rest, combined
  event/availability/load/taper/recovery validation and exact additions/removals/
  replacements/preservation/ordinal diffs. Infeasible reconnection blocks apply.
- Flattened immutable origins, exact content checks on load, matching at the first
  prescribing revision, and original parameter interpretation for preserved sessions.
- Versioned lock/completion editor and reviewed timed prescription edits. Manual
  creation is available through the canonical service API, without a dedicated UI.
- Explicit confirmed parameter refresh for new prescriptions, calendar status labels,
  and recorded revision ranges, preservation and overrides in Seasons history.
- Composite stale checks and deterministic recomposition under BEGIN IMMEDIATE;
  graph, full projection/pointer, origins, new manual state, nutrition, intent version
  and immutable audit commit together. Duplicate retry is a no-op.
- Additive v14 protection/origin schema and row-preserving expansion of the v13 audit
  CHECK. Migration/repair failures roll back. No old revision hashes are rewritten.

## Verification commands

Use repository .venv/Scripts/python.exe, -q -p no:cacheprovider, and separate
--basetemp directories under .tools. Final combined scope verifies **646 distinct
checks: 642 non-browser and four browser workflows**; reruns are not added.

The combined non-browser command is python -m pytest with these paths:

- tests/test_season_regeneration.py, test_season_generation.py, test_season_plans.py,
  test_season_migrations.py, test_season_browser_logs.py
- tests/test_plan_revision_persistence.py, test_activity_workout_match.py,
  test_legacy_plan_conversion.py, test_baseline_plan_builder.py, test_plan_persistence.py
- tests/test_schema_migrations.py, test_calendar_builder.py, test_training_policy.py,
  test_plan_methodology_policy.py, test_plan_ai_v2_boundary.py,
  test_runtime_methodology_compliance.py, test_nicegui_data.py, test_nicegui_workspace.py
- tests/plan_methodology_contracts

The final combined non-browser suite passed 639 checks in 140.64s.
[Final combined output](verified-suite.txt). Three additional offline Plan UI checks
passed alongside the final calendar browser rerun (four checks total);
[grid/Plan UI output](grid-verified.txt). Together with the four distinct browser
workflows, this verifies 646 distinct checks. Earlier broad run: 635 passed in
136.91s, before the final calendar/adoption/log-classification cases. The intermediate
combined run collected an adoption fixture before its conformance-purpose correction
and reported 638 passed/one failed. A targeted corrected rerun passed both adoption
cases; the final combined run uses the corrected fixture. Raw [intermediate output](final-suite.txt)
and [targeted adoption confirmation](adoption-verified.txt) are retained.

Focused final regeneration/generation/migration checks: 102 passed in 100.37s
before calendar/adoption additions. [Output](focused-final.txt). Calendar reader,
regeneration and log parser checks: 54 passed in 72.68s. [Output](calendar-final.txt).

Browser command: python -m pytest tests/test_season_regeneration_browser.py
  tests/test_season_generation_browser.py tests/test_seasons_browser.py
  -q -p no:cacheprovider --basetemp .tools/y3-browsers-4

Four passed in 77.17s. [Output](browser-verified.txt). The new workflow verifies lock,
manual prescription apply, affected diff, stale lock rejection, protected regeneration
apply and persistent history at desktop/390px. The final calendar/visual rerun is
recorded in [visual output](visual-final.txt). Compatibility captures are in
compat_y1 and compat_y2; original prior-phase visual evidence was restored.

Raw browser logs retain Python/Windows Proactor _call_connection_lost WinError 10054
socket-close noise. The test helper recognizes only this exact stdlib callback with
two asyncio frames, retaining the raw output. It rejects genuine application
tracebacks, other transport errors and browser runtime errors. The parser test
verifies that narrow distinction. Earlier failures and [raw log](browser-server.txt)
are retained; no runtime error handler was added to the app.

Acceptance covers moved/cancelled events, custom incomplete ranges, history/today,
explicit completion, provisional matches, locks/manual overrides, original targets,
three-revision flattened origins, matching at origin, origin corruption, version
concurrency, concurrent/duplicate/stale apply, all transaction failure boundaries,
manual state rollback, taper conflicts, fixed-point neighbors and changed fitness
history. Native and converted adoption retain original prescriptions and source rows.

## Migration and timing

python reports/yearly_y3/verify_real_snapshot.py <installed database path>

A private read-only SQLite backup of the installed v11 database upgraded/replayed
v14 with all prior app rows, active schedule and foreign-key state unchanged.
The source was never migrated, and the temporary full backup was removed.
[Aggregate evidence](real_snapshot.txt). No private source data is retained.

python reports/yearly_y3/benchmark.py

For synthetic 366-day/three-event seasons, affected regeneration preview took
0.404s (80/20) and 0.453s (Maffetone); atomic apply took 1.275s and 1.463s,
preserving 259 occurrences. [Output](performance.txt). Synthetic DBs were removed.
These are local measurements, not a general performance promise.

## Visual inspection

Desktop/390px settings, diff, manual edit, review/apply and recorded history were
inspected; text wraps, tables scroll within the modal, and apply remains reachable.
The calendar status column displays Manual/Locked/Completed/Preserved as appropriate.
The existing phone shell drawer is closed in browser tests before interaction.

[Settings](regeneration_settings_desktop.png), [desktop diff](affected_preview_desktop.png),
[phone diff](affected_preview_narrow.png), [manual edit](manual_preview_desktop.png),
[phone apply](review_apply_narrow.png), [history](history_narrow.png),
[calendar protection](calendar_protection_desktop.png).

## Limits and next scope

Deterministic running seasons remain bounded to 366 days and one methodology.
Unsupported adopted prescriptions, unknown protected running duration, unresolved
matches and infeasible fixed workouts block unsafe replacement. Automatic fitness
coverage collection, AI composition, mixed methods/sports, peak refinement, optional
rollback, packaging and a live field trial remain separately scoped. Timed manual
editing preserves native targets and segment proportions; rest/event editing uses
season regeneration. No historical rewrite or completion/manual-provenance clearing.

New modules: services/season_regeneration.py, services/season_workouts.py,
plan_methodology/workout_origin.py. Integration: season generation/schedule,
canonical repository/matching, v14 schema/migration, Seasons/calendar readers.
README, roadmap, source plan and architecture contain the Y3 handoff. Client settings
were unchanged; attributable token/account-credit usage is unavailable.
