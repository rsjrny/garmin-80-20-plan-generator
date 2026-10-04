# Test suite necessity review

Reviewed: 2026-10-04. This document and its CSV preserve the **pre-cleanup audit** of 1,453 cases, including the pending CI fixes. Subsequent implementation is recorded in [Test cleanup](test-cleanup.md); the findings and inventory below describe the suite before those changes.

Most tests protect useful behavior and should stay. The main opportunities are weak assertions, fixtures that cannot reach the behavior named by the test, duplicate regressions, and expensive browser setup. Do not remove database rollback, migration, privacy, runtime isolation, or numerical boundary tests merely to make CI green.

## Scope and method

- Collected all **1,453 cases in 93 test modules**, defined by **958 test functions**. Parametrization explains the difference between function and case counts.
- Inventoried each function's body, decorators, calls and assertions; compared normalized AST bodies for duplicates; examined fixtures and production implementations for flagged cases.
- Reviewed browser waits, generated artifacts, historical characterization tests, contract adapters and the CI configuration.
- Ran the full suite with duration reporting and JUnit output. Results are recorded below.
- Tested three assertion blind spots with process-local substitutions. No production file or test implementation was changed by these probes.

This is a static and execution-based review, not an exhaustive mutation test or a proof that every retained case is indispensable. A passing test is evidence that its asserted scenario works, not proof that its assertions detect all relevant defects. KEEP means a meaningful scenario is present and this review found no specific reason to remove it. Removal and consolidation candidates require preserving the named behavior in remaining coverage.

[Complete case inventory](test-suite-review.csv) contains one row for every collected case, including its source location, recommendation and rationale. The module table below accounts for every module.

## Recommendations by case

| Recommendation | Cases | Meaning |
| --- | ---: | --- |
| Keep | 1,412 | Retain the behavior coverage. |
| Keep and harden | 14 | Real browser tests: preserve interactions; improve waits, independence and outputs. |
| Strengthen | 14 | Intended behavior matters, but the present assertions or inputs are insufficient. |
| Replace with behavior | 8 | Exercise UI actions/state instead of matching Python source text. |
| Consolidate | 4 | Combine overlapping or shallow checks while preserving distinct guarantees. |
| Removal candidate | 1 | Constant-only assertion adds no verification of the claimed artifact. |
| **Total** | **1,453** | No tests were deleted or disabled during this review. |

## Findings

### 1. Architecture success flags do not establish the claimed behavior

Locations: [revision contract wrapper](../tests/plan_methodology_contracts/test_revision_registry_snapshot_contracts.py), [ARCH-REV cases](../tests/plan_methodology_contracts/architecture_cases.py), and [production compatibility helpers](../src/garmin_data_hub/plan_methodology/revisions.py).

ARCH-REV-004 checks algorithm/canonical/includes/excludes flags but does not require a hash or compare hashes after changes. ARCH-REV-011 checks revision_content_unchanged and stored_outside_revision flags without inspecting persisted runtime state or a revision before/after.

Evidence: replacing compute_content_hash with a function that returns only those flags, and attach_runtime_state with a function that does nothing except return the flags, still yields two passing contract tests. This establishes a limitation of these two tests, not absence of all hash/persistence coverage: test_plan_revision_persistence.py already exercises real round trips and frozen revision behavior.

Action: preserve the contract IDs and connect the checks to observable behavior. Compare hash equality when only UI/runtime input changes, hash inequality when prescription changes, and actual stored revision/runtime state. Do not remove all 82 behavioral contract cases because their wrapper bodies look alike; their case payloads and requirements differ.

### 2. The installer signing-order test can pass without signing

Location: [test_signed_installer_path_runs_before_checksums](../tests/test_installer_packaging.py:78).

build_script.index("Invoke-AuthenticodeSign") finds the function declaration at build.ps1 line 300. The actual invocation is at line 645; checksums start at line 692. Consequently the first assertion does not prove invocation order.

Evidence: removing the actual invocation from the text supplied to the test still produces a pass. The production build script was not modified.

Action: check the invocation block, and preferably verify signing/checksum sequencing with a controlled Windows build or command recorder. Also strengthen test_build_script_supports_signed_installer_only_release: finding function names and log messages does not establish successful signing. Retain the installer original-user flags check; it verifies security-relevant declarative configuration.

### 3. Ten FTP cases cannot prove their named boundaries

Location: [historical FTP characterizations](../tests/test_phase3d1_characterization.py:294).

These five functions expand to ten cases:

- test_current_ftp_uses_summary_norm_power_then_average_power (2 cases).
- test_current_ftp_requires_1200_seconds_of_activity_elapsed_duration (2).
- test_current_ftp_accepts_only_rounded_results_from_80_through_500 (4).
- test_running_ftp_requires_trackpoint_power_coverage (1).
- test_legacy_ftp_helper_does_not_admit_cycling_at_cutoff (1).

Their activities are cycling, lack qualifying running-power evidence, and expect None. An implementation that always rejects cycling can satisfy the expectations without exercising the duration, rounding, trackpoint coverage or cutoff named in the test. Several names still describe replaced summary-power behavior. Dates are fixed but several scenarios do not freeze the clock, further weakening evidence that the intended branch was reached.

Action: consolidate obsolete characterization matrices into the explicit non-running rejection regression, and use valid running evidence, current temporal provenance and a frozen clock for genuine boundary cases. Retain the canonical running-power and cutoff regressions in test_phase3d1_policy_contracts.py and test_phase3d4_corrections.py. Compare valid positive cases to just-outside-boundary cases before deleting old matrices.

### 4. Eight UI tests inspect source text instead of user-visible behavior

Locations and proposed replacements:

| Source-text test | Replacement behavior |
| --- | --- |
| test_dashboard_plan_copy.py::test_dashboard_wires_copy_button_to_visible_upcoming_rows | Click Copy plan, capture clipboard content, and check empty-plan disabled state. |
| test_help_about_ui.py::test_help_about_page_wires_version_workflow_and_about_details | Open Help/About and assert visible version and working destination links. |
| test_plan_offline_ui.py::test_plan_page_offers_offline_workbook_and_codex_generation_paths | Invoke baseline/workbook actions and verify confirmation, refreshed schedule and stale-plan rejection. |
| test_sleep_recovery_analysis.py::test_data_query_page_wires_sleep_recovery_deep_dive | Open the tab and assert real analysis output and empty-data handling. |
| test_phase3a1_contracts.py::test_data_query_fresh_request_uses_shared_job_not_writable_mcp | Invoke the action; record the shared job start and prohibit a writable MCP call. |
| test_phase3a1_contracts.py::test_data_query_fresh_request_uses_saved_credentials_and_actionable_missing_login | Exercise saved-login and missing-login branches. |
| test_phase3a1_contracts.py::test_data_query_sync_rejects_sandbox_and_nonstandard_database_targets | Exercise both rejected targets and assert no process launch or database change. |
| test_phase3a1_contracts.py::test_data_query_observes_the_shared_running_job_snapshot | Exercise a second UI client and assert the same live job state. |

A handler can contain the expected strings in unused code and still pass. A safe refactor can change the spelling and fail. Keep the intended contracts and replace their verification; do not simply discard them. Static checks of declarative installer flags or browser event payload configuration remain useful, provided their limited scope is clear.

### 5. Four checks can be consolidated

- test_phase3d1_failure_contracts.py::test_automatic_refresh_keeps_ftp_override_separate_from_ftp_calc has the same normalized body and values as test_phase3d1_characterization.py::test_automatic_refresh_never_copies_ftp_override_to_calculation. Fixtures differ in extra daily/sleep tables; keep one canonical regression after ensuring that distinction is irrelevant or explicitly parameterized.
- test_phase3d1_characterization.py::test_running_ftp_does_not_fall_back_to_other_sports_or_summary_power overlaps test_summary_power_and_nonrunning_sports_do_not_establish_running_ftp in the same file. Combine into a clear matrix, retaining a valid running-power positive control.
- Both tests in test_trackpoint_parser_wiring.py monkeypatch a one-line forwarding function. Fold these checks into parser adapter/ingestion tests that prove the pinned upstream result crosses the application boundary.

Identical bodies alone are insufficient grounds for removal. The TCX cross-family and unsubstantiated-alias tests have identical bodies but different inputs. The AI type-confusion and missing-structure tests also have different parameter matrices. The contract wrappers select different specifications. Keep these distinct cases.

### 6. One constant-only test is a removal candidate

Location: [test_contract_fixtures_name_the_verified_authoritative_artifacts](../tests/plan_methodology_contracts/test_traceability_contracts.py:25).

It compares two imported hash literals with the same literals written in the test. It neither reads nor hashes the authoritative artifacts. It catches accidental edits to the constants, but cannot establish that the artifacts match or were verified.

Action: retain the documented provenance and remove this tautological verification, or replace it with hashing accessible, versioned specification artifacts. Keep the neighboring contract-ID completeness and uniqueness tests: they validate traceability structure rather than only repeating constants.

### 7. Keep the fourteen real browser cases, improve their execution

The nine browser modules contain 14 browser scenarios plus one static AST event-payload check. Browser cases cover JavaScript event serialization, chart hover/selection, map updates, shared UI state, responsive layout, and season review/apply controls. Python-only tests cannot fully substitute for these boundaries. The recent CI failures show why the coverage matters.

There are **50 fixed wait_for_timeout calls** in these modules. Replace waits that gate assertions with waits for the actual application state, Plotly readiness, or persisted result. Some waits only settle screenshots; classify those separately before changing them. Split long scenarios when independent initialization improves failure diagnosis, preserving tests of state transitions across tabs and clients.

Screenshots are written to tracked reports directories and are not compared against visual baselines. They are diagnostics, not automated visual verification. Use per-test temporary directories or a CI artifact directory, capture on failure or on explicit request, and upload artifacts from CI. This also avoids dirty worktrees and filename collisions in future parallel runs.

### 8. Separate execution environments while retaining checks

Current CI has one Ubuntu/Python 3.11 job that installs all dependencies and runs the entire suite. Keep the checks required, but split fast Python tests and browser tests into separately reported jobs if faster feedback is needed. Add explicit markers before relying on exclusions. Windows credential/process-tree and packaged-runtime behaviors also deserve a Windows validation job, particularly before a release; Linux skips cannot validate those paths.

Do not drop low-cost analytical boundary cases just because parameterization makes the count look large. Likewise, old schema upgrade tests remain relevant while users can upgrade from those versions. Keep mocked service failures and corresponding real SQLite trigger/commit tests when they verify different boundaries. Keep real installed-upstream MCP tests: mocks alone would not catch the FastMCP dependency incompatibility.

Move tests toward domain names such as sync_privacy, archive_identity and threshold_provenance as maintenance permits. Historical phase names and intentional-RED docstrings describe old development checkpoints even though these tests now pass. Update those descriptions; age is not a reason to remove a regression.

## Suggested implementation order

1. Strengthen the two architecture cases, signing-order check, and ten misleading FTP cases.
2. Replace the eight source-text UI checks with observable behavior.
3. Consolidate the four identified checks and remove or replace the constant-only assertion.
4. Harden browser waits and move artifacts out of tracked reports directories.
5. Use the measured durations to separate CI jobs; retain complete required coverage and Windows release validation.

This review changes documentation only. Existing implementation and CI fixes were preserved; tests were not deleted, skipped or weakened.

## Validation and measured runtime

Full local run: **1,453 passed in 382.61 seconds (6 minutes 22 seconds)**, exit status 0, on Windows with the workspace Python environment. This is not a fresh Linux/GitHub run.

The JUnit report contains 1453 cases and 0 failures/errors. Browser modules consumed 168.94 seconds of summed case time, approximately 44.3% of measured case time. Case-time sums include pytest fixture phases and are not identical to wall time.

| Slowest module | Summed case seconds |
| --- | ---: |
| tests.test_season_regeneration | 73.21 |
| tests.test_season_regeneration_browser | 38.87 |
| tests.test_season_generation | 29.77 |
| tests.test_track_visuals_browser | 23.55 |
| tests.test_track_charts_browser | 22.62 |
| tests.test_season_generation_browser | 20.72 |
| tests.test_seasons_browser | 20.10 |
| tests.test_season_refinement | 18.68 |
| tests.test_season_refinement_browser | 16.42 |
| tests.test_chart_explorer_browser | 15.94 |

The isolated assertion probes passed all three selected tests despite removal of the intended behavior. They ran in a separate process and did not alter production files. The 100 relative documentation links were checked; the CSV has exactly 1,453 unique node IDs matching collection.

Reproduce timings with:

```powershell
python -m pytest -q --durations=30 --junitxml=.tools/test-suite-review-results.xml
```

Detailed JUnit and probe logs are retained under .tools as local audit artifacts. Generated tracked reports were restored to their exact pre-run bytes.

## Every test module

This table is a retention summary; the CSV provides individual case decisions. Mixed means at least one case needs the changes described above.

| Module | Collected cases | Review recommendation |
| --- | ---: | --- |
| [plan_methodology_contracts/test_boundary_compliance_legacy_atomicity_contracts.py](../tests/plan_methodology_contracts/test_boundary_compliance_legacy_atomicity_contracts.py) | 13 | Keep |
| [plan_methodology_contracts/test_methodology_contracts.py](../tests/plan_methodology_contracts/test_methodology_contracts.py) | 40 | Keep |
| [plan_methodology_contracts/test_prescription_segment_contracts.py](../tests/plan_methodology_contracts/test_prescription_segment_contracts.py) | 7 | Keep |
| [plan_methodology_contracts/test_revision_registry_snapshot_contracts.py](../tests/plan_methodology_contracts/test_revision_registry_snapshot_contracts.py) | 22 | Mixed: keep; strengthen |
| [plan_methodology_contracts/test_traceability_contracts.py](../tests/plan_methodology_contracts/test_traceability_contracts.py) | 3 | Mixed: keep; remove candidate |
| [test_activity_calendar_days.py](../tests/test_activity_calendar_days.py) | 15 | Keep |
| [test_activity_grid_browser.py](../tests/test_activity_grid_browser.py) | 2 | Mixed: keep harden; keep |
| [test_activity_workout_match.py](../tests/test_activity_workout_match.py) | 7 | Keep |
| [test_ai_plan_diagnostics.py](../tests/test_ai_plan_diagnostics.py) | 22 | Keep |
| [test_ai_plan_import.py](../tests/test_ai_plan_import.py) | 18 | Keep |
| [test_audit_failure_contracts.py](../tests/test_audit_failure_contracts.py) | 10 | Keep |
| [test_baseline_plan_builder.py](../tests/test_baseline_plan_builder.py) | 22 | Keep |
| [test_calendar_builder.py](../tests/test_calendar_builder.py) | 5 | Keep |
| [test_chart_explorer.py](../tests/test_chart_explorer.py) | 5 | Keep |
| [test_chart_explorer_browser.py](../tests/test_chart_explorer_browser.py) | 1 | Keep; harden browser execution |
| [test_chart_overview.py](../tests/test_chart_overview.py) | 8 | Keep |
| [test_chart_overview_browser.py](../tests/test_chart_overview_browser.py) | 1 | Keep; harden browser execution |
| [test_coach_chat.py](../tests/test_coach_chat.py) | 13 | Keep |
| [test_coaching_packet.py](../tests/test_coaching_packet.py) | 16 | Keep |
| [test_codex_plan_generator.py](../tests/test_codex_plan_generator.py) | 9 | Keep |
| [test_codex_prerequisites.py](../tests/test_codex_prerequisites.py) | 16 | Keep |
| [test_dashboard_plan_copy.py](../tests/test_dashboard_plan_copy.py) | 3 | Mixed: keep; replace with behavior |
| [test_frozen_sync_packaging.py](../tests/test_frozen_sync_packaging.py) | 8 | Keep |
| [test_garmin_auth_session.py](../tests/test_garmin_auth_session.py) | 4 | Keep |
| [test_garmin_credentials.py](../tests/test_garmin_credentials.py) | 12 | Keep |
| [test_givemydata_upgrade_check.py](../tests/test_givemydata_upgrade_check.py) | 13 | Keep |
| [test_help_about_ui.py](../tests/test_help_about_ui.py) | 4 | Mixed: keep; replace with behavior |
| [test_installer_packaging.py](../tests/test_installer_packaging.py) | 4 | Mixed: keep; strengthen |
| [test_legacy_plan_conversion.py](../tests/test_legacy_plan_conversion.py) | 25 | Keep |
| [test_mcp_results.py](../tests/test_mcp_results.py) | 4 | Keep |
| [test_mcp_sidecar_client.py](../tests/test_mcp_sidecar_client.py) | 8 | Keep |
| [test_metric_refresh_transactions.py](../tests/test_metric_refresh_transactions.py) | 14 | Keep |
| [test_metrics_refresh.py](../tests/test_metrics_refresh.py) | 9 | Keep |
| [test_nicegui_app_security.py](../tests/test_nicegui_app_security.py) | 2 | Keep |
| [test_nicegui_data.py](../tests/test_nicegui_data.py) | 25 | Keep |
| [test_nicegui_workspace.py](../tests/test_nicegui_workspace.py) | 12 | Keep |
| [test_phase2b_metric_freshness.py](../tests/test_phase2b_metric_freshness.py) | 36 | Keep |
| [test_phase2d1_temporal_freshness.py](../tests/test_phase2d1_temporal_freshness.py) | 5 | Keep |
| [test_phase3a1_characterization.py](../tests/test_phase3a1_characterization.py) | 9 | Keep |
| [test_phase3a1_contracts.py](../tests/test_phase3a1_contracts.py) | 16 | Mixed: keep; replace with behavior |
| [test_phase3a3_corrections.py](../tests/test_phase3a3_corrections.py) | 28 | Keep |
| [test_phase3a4_corrections.py](../tests/test_phase3a4_corrections.py) | 3 | Keep |
| [test_phase3b1_characterization.py](../tests/test_phase3b1_characterization.py) | 7 | Keep |
| [test_phase3b1_failure_contracts.py](../tests/test_phase3b1_failure_contracts.py) | 15 | Keep |
| [test_phase3c1_quarantine.py](../tests/test_phase3c1_quarantine.py) | 11 | Keep |
| [test_phase3d1_characterization.py](../tests/test_phase3d1_characterization.py) | 36 | Mixed: keep; consolidate; strengthen |
| [test_phase3d1_failure_contracts.py](../tests/test_phase3d1_failure_contracts.py) | 18 | Mixed: keep; consolidate |
| [test_phase3d1_policy_contracts.py](../tests/test_phase3d1_policy_contracts.py) | 21 | Keep |
| [test_phase3d2_threshold_migration.py](../tests/test_phase3d2_threshold_migration.py) | 3 | Keep |
| [test_phase3d2_threshold_service.py](../tests/test_phase3d2_threshold_service.py) | 4 | Keep |
| [test_phase3d4_corrections.py](../tests/test_phase3d4_corrections.py) | 18 | Keep |
| [test_phase3e1_characterization.py](../tests/test_phase3e1_characterization.py) | 8 | Keep |
| [test_phase3e1_failure_contracts.py](../tests/test_phase3e1_failure_contracts.py) | 19 | Keep |
| [test_phase3h1_bundled_runtime_diagnostic.py](../tests/test_phase3h1_bundled_runtime_diagnostic.py) | 7 | Keep |
| [test_phase3m4b2_characterization.py](../tests/test_phase3m4b2_characterization.py) | 5 | Keep |
| [test_phase3m4b2_contracts.py](../tests/test_phase3m4b2_contracts.py) | 33 | Keep |
| [test_phase3m4b3_implementation.py](../tests/test_phase3m4b3_implementation.py) | 13 | Keep |
| [test_phase3m4b4a_corrections.py](../tests/test_phase3m4b4a_corrections.py) | 22 | Keep |
| [test_phase3m5a_identity_and_gpx.py](../tests/test_phase3m5a_identity_and_gpx.py) | 42 | Keep |
| [test_phase3m6a_baseline_idempotency.py](../tests/test_phase3m6a_baseline_idempotency.py) | 12 | Keep |
| [test_phase3m9b_characterization.py](../tests/test_phase3m9b_characterization.py) | 17 | Keep |
| [test_phase3m9b_failure_contracts.py](../tests/test_phase3m9b_failure_contracts.py) | 3 | Keep |
| [test_phase3m9c_runtime_enforcement.py](../tests/test_phase3m9c_runtime_enforcement.py) | 6 | Keep |
| [test_plan_ai_v2_boundary.py](../tests/test_plan_ai_v2_boundary.py) | 127 | Keep |
| [test_plan_methodology_policy.py](../tests/test_plan_methodology_policy.py) | 80 | Keep |
| [test_plan_offline_ui.py](../tests/test_plan_offline_ui.py) | 3 | Mixed: replace with behavior; keep |
| [test_plan_persistence.py](../tests/test_plan_persistence.py) | 19 | Keep |
| [test_plan_revision_persistence.py](../tests/test_plan_revision_persistence.py) | 46 | Keep |
| [test_runtime_methodology_compliance.py](../tests/test_runtime_methodology_compliance.py) | 17 | Keep |
| [test_schema_migrations.py](../tests/test_schema_migrations.py) | 7 | Keep |
| [test_season_browser_logs.py](../tests/test_season_browser_logs.py) | 1 | Keep |
| [test_season_generation.py](../tests/test_season_generation.py) | 69 | Keep |
| [test_season_generation_browser.py](../tests/test_season_generation_browser.py) | 2 | Keep; harden browser execution |
| [test_season_migrations.py](../tests/test_season_migrations.py) | 6 | Keep |
| [test_season_plans.py](../tests/test_season_plans.py) | 50 | Keep |
| [test_season_refinement.py](../tests/test_season_refinement.py) | 27 | Keep |
| [test_season_refinement_browser.py](../tests/test_season_refinement_browser.py) | 2 | Keep; harden browser execution |
| [test_season_regeneration.py](../tests/test_season_regeneration.py) | 30 | Keep |
| [test_season_regeneration_browser.py](../tests/test_season_regeneration_browser.py) | 1 | Keep; harden browser execution |
| [test_seasons_browser.py](../tests/test_seasons_browser.py) | 1 | Keep; harden browser execution |
| [test_sleep_recovery_analysis.py](../tests/test_sleep_recovery_analysis.py) | 6 | Mixed: keep; replace with behavior |
| [test_sync_credentials_ui.py](../tests/test_sync_credentials_ui.py) | 21 | Keep |
| [test_sync_job_lifecycle.py](../tests/test_sync_job_lifecycle.py) | 21 | Keep |
| [test_sync_process_tree.py](../tests/test_sync_process_tree.py) | 2 | Keep |
| [test_sync_progress.py](../tests/test_sync_progress.py) | 4 | Keep |
| [test_temporal_metrics.py](../tests/test_temporal_metrics.py) | 48 | Keep |
| [test_track_charts.py](../tests/test_track_charts.py) | 7 | Keep |
| [test_track_charts_browser.py](../tests/test_track_charts_browser.py) | 2 | Keep; harden browser execution |
| [test_track_splits.py](../tests/test_track_splits.py) | 15 | Keep |
| [test_track_visuals.py](../tests/test_track_visuals.py) | 9 | Keep |
| [test_track_visuals_browser.py](../tests/test_track_visuals_browser.py) | 3 | Keep; harden browser execution |
| [test_trackpoint_parser_wiring.py](../tests/test_trackpoint_parser_wiring.py) | 2 | Mixed: consolidate |
| [test_training_policy.py](../tests/test_training_policy.py) | 6 | Keep |
