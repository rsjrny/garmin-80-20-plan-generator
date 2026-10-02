# Y2 verification and handoff

Date: 2026-10-02 (America/New_York). Y2 is locally verified and uncommitted.
Prior C2/Y0/Y1 work was retained. No live apply, source database migration, commit,
push, deployment or packaged executable rebuild. Private backup was removed.

## Implemented scope

One deterministic 1–366-day running season, including zero-event maintenance;
versioned A/B/C windows; lower overlapping taper/recovery load; explicit conflicts;
continuous conservative duration progression and canonical workout composition.
Confirmed running LTHR or confirmed ordinary MAF adjustment is required. Optional
covered completed weeks are service inputs; the editor uses an explicit starter
load. Unknown running distance/TSS is not invented. Event target duration is an
estimate or remains unknown; targets do not prove readiness or goal achievement.

Rest/strength/mobility have explicit validated types, no endurance prescriptions
and no running distribution/compliance contribution. Rest has no training duration.
Goals and V2 AI parsing remain running-only. Preview is read-only. Initial apply only
accepts an empty unarchived season starting today or later, with visible warning
acknowledgement. Linked full previews use original parameters and cannot replace
workouts yet. Known neighboring recovery conflicts prevent initial apply.

Composite stale checks and deterministic recomputation happen under one BEGIN
IMMEDIATE. Season ownership, immutable full revision graph, projection/pointer,
affected nutrition and immutable v13 audit commit together. Retry is a no-op. Ten
injected failure stages roll back all state. Old writer guards have no public bypass.

## Tests and commands

Use repository .venv Python, -q -p no:cacheprovider and separate --basetemp directories.

Broad command: python -m pytest followed by these paths:

- tests/test_season_generation.py, test_season_plans.py, test_season_migrations.py
- tests/test_plan_revision_persistence.py, test_activity_workout_match.py,
  test_legacy_plan_conversion.py, test_baseline_plan_builder.py, test_plan_persistence.py
- tests/test_schema_migrations.py, test_calendar_builder.py, test_training_policy.py,
  test_plan_methodology_policy.py, test_plan_ai_v2_boundary.py,
  test_runtime_methodology_compliance.py, test_nicegui_data.py, test_nicegui_workspace.py
- tests/plan_methodology_contracts

607 passed in 65.55s. [Raw regression output](regression.txt).

Final focused command: python -m pytest tests/test_season_generation.py
  tests/test_runtime_methodology_compliance.py tests/test_plan_methodology_policy.py
  tests/test_plan_ai_v2_boundary.py tests/test_season_generation_browser.py
  tests/test_seasons_browser.py -q -p no:cacheprovider --basetemp <temporary>

295 passed; the existing browser workflow's server-log assertion hit a transient
Windows ConnectionResetError during teardown, after all interaction assertions passed.
[Unaltered output](final.txt). Clean confirmation command:

    python -m pytest tests/test_season_generation_browser.py tests/test_seasons_browser.py
      -q -p no:cacheprovider --basetemp <temporary>
    3 passed in 41.71s

[Clean confirmation](browser-final.txt). [Latest Y2 visual rerun](browser.txt).
Across combined scope, 611 distinct tests are verified: 608 non-browser plus three
browser workflows. The final focused run includes the additional auxiliary runtime
case; repeated runs are not added to this total.

The test suite covers one-event/multi-event seasons, exact windows, close A peaks,
B/C taper/recovery conflicts, year/leap boundaries, completed/cancelled intent,
missing fitness history, all run-day counts, actual/estimated measure roles,
conservative return load, known-distance growth, unavailable dates and neighboring
recovery. Persistence cases cover read-only preview, warning acknowledgement,
projection/hash round-trip, immutable audit/owner checks, all failure stages,
input/match/date/raw-state staleness, forgery, duplicate/concurrent apply and nutrition.

## Migration and performance

    python reports/yearly_y2/verify_real_snapshot.py <installed database path>

A private read-only SQLite backup of the installed v11 database upgraded/replayed
to v13. Every prior app-owned row, active schedule and foreign-key state was retained;
added tables were empty. The private backup was removed and source never migrated.
[Aggregate output](real_snapshot.txt). No private snapshot or source data is retained.

    python reports/yearly_y2/benchmark.py

Synthetic 366-day/three-event previews: 80/20 0.021–0.027s; Maffetone 0.022–0.030s.
Atomic initial apply: 0.558s and 0.637s respectively. [Output](performance.txt).
These are local measurements, not general performance guarantees. Synthetic databases
were automatically removed; no live schedule was changed.

## Visual verification

Inspected 1440px/390px preview, warnings, phases, workload/session tables, conflicts,
review/apply, reload/history and canonical calendar. Text wraps; tables scroll inside
the modal; warning acknowledgement/apply remains reachable; conflicts disable apply.
Stale apply leaves the calendar unchanged. At initial phone-width load the existing
shell navigation drawer opens; tests close it via the backdrop before interaction.

[Desktop preview](preview_desktop.png), [phone preview](preview_narrow.png),
[workload](workload_narrow.png), [review/apply](apply_narrow.png),
[applied history](applied_narrow.png), [conflicts](conflicts_narrow.png),
[canonical calendar](calendar_narrow.png).

## Handoff and limitations

New services: season_schedule.py (pure orchestration/composition) and
season_generation.py (preview/atomic initial apply). Integration: auxiliary enums,
segment/goal invariants, named policies/runtime accounting, private owner-aware
approval, v13 schema/migration, season UI, calendar reader and policy normalization.
New test modules: test_season_generation.py and test_season_generation_browser.py;
existing schema fixtures were updated. Roadmap/source plan/architecture/README explain
the scope. Prior work remains in the working tree, so do not stage it indiscriminately.

Y3 is separately authorized: protected-workout state, immutable origins, affected
ranges, merged revisions, overrides and parameter refresh. Existing/adopted apply,
mixed methods/sports, AI season composition, readiness prediction, ordinary automatic
history-coverage collection and manual MAF age exceptions are outside Y2. Packaging
and real-user field trial were not performed. Token/account-credit attribution was
unavailable; client settings were unchanged.
