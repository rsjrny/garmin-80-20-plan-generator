# Y1 verification — 2026-10-02

Verified using the repository `.venv` Python and bundled Playwright Chromium.
All schedule mutations in automated tests used synthetic temporary databases.
The installed database was read only for a private snapshot migration check.

## Automated checks

`pytest.txt`: 182 passed. Invocation:

```text
python -X utf8 -m pytest tests/test_season_plans.py tests/test_season_migrations.py
  tests/test_schema_migrations.py tests/test_plan_revision_persistence.py
  tests/test_activity_workout_match.py tests/test_legacy_plan_conversion.py
  tests/test_baseline_plan_builder.py tests/test_plan_persistence.py
  -q -p no:cacheprovider --basetemp <unique temporary directory>
```

`shared.txt`: 225 passed. Invocation:

```text
python -X utf8 -m pytest tests/test_training_policy.py tests/test_calendar_builder.py
  tests/test_plan_ai_v2_boundary.py tests/test_plan_offline_ui.py
  tests/test_nicegui_data.py tests/test_nicegui_workspace.py tests/test_help_about_ui.py
  tests/test_nicegui_app_security.py tests/test_phase3b1_characterization.py
  tests/test_phase3b1_failure_contracts.py tests/test_activity_calendar_days.py
  tests/test_mcp_results.py -q -p no:cacheprovider --basetemp <unique temporary directory>
```

`browser.txt`: one passed. Invocation:

```text
python -X utf8 -m pytest tests/test_seasons_browser.py
  -q -p no:cacheprovider --basetemp <unique temporary directory>
```

Total: **408 passed** across disjoint suites. Migration checks include fresh,
legacy, native and converted v11 snapshots, repeated upgrades, rollback after
injected failure, missing additive objects and incompatible table detection.
Season checks include invalid dates/targets/availability, leap and year boundaries,
priorities, stale/concurrent edits, completed history, linking projection drift,
retained confirmed matches and all guarded schedule writers (including another
canonical plan targeting season-owned dates).

The browser workflow verifies creation, event targets, duplicate date rejection,
reload, two-editor concurrency, explicit plan linking, linked cancellation,
archive/restore, navigation, unchanged active schedule hashes, responsive width and
scrolling dialog controls. No browser or server errors were recorded. Windows socket
clients disconnect and drain before teardown to avoid unrelated Proactor resets.

## Visual checks

Inspected `desktop.png` at 1440px, `narrow.png` at 390px,
`event_editor_narrow.png` and `event_editor_narrow_bottom.png`. Event actions move
under metadata on narrow screens; no horizontal overflow. Modal fields scroll
vertically and the save/close controls remain reachable. All evidence uses synthetic
data. No packaged executable rebuild or real-user UI field trial was performed.

## Installed database snapshot

`verify_real_snapshot.py` creates a private full SQLite backup, upgrades it from
v11 to v12, repeats schema application and checks every checked plan/match/settings row, the complete
active schedule, and foreign-key state. It checks that the new season tables are
empty. `real_snapshot.txt` records all checks passing with zero prior FK violations.
The checker's "app-owned" row scope is the ten planning, matching and settings
tables explicitly listed in `state()`.

Invocation:

```text
python -X utf8 reports/yearly_y1/verify_real_snapshot.py <installed database path>
```

The source uses immutable read-only mode because creating SQLite shared-memory
sidecars beside the installed database is not permitted. This mode is used only
after rejecting any existing WAL; a source that changes size/mtime or develops a
WAL during backup is rejected. The private copy is removed after verification.
No source migration, schedule apply or sidecar write occurred. No database content
or personal activity data is retained in these reports.

## Handoff

Y1 is locally complete and uncommitted. The roadmap and source plan record the
implementation files and decisions. Y2 generation and Y3 regeneration remain
separately authorized phases. Old-writer guards moved forward into Y1 to protect
explicitly linked seasons; later season apply needs an owner-aware transaction path.
