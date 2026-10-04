# Charts C4 evidence - 2026-10-04

C4 is implemented and locally verified. Starting HEAD: `1c05874`, preserving
uncommitted C3 work. No live database, schedule, migration, package rebuild,
commit, push or deployment was changed.

## Verification

- Final focused C4/shared-log gate: **34 passed in 21.89s**, [focused.txt](focused.txt).
- Initial broad regression: **315 passed, 2 failed in 179.77s**,
  [regression.txt](regression.txt). The failures were a subsequently fixed pandas
  date-bound socket serialization error and an asyncio Windows socket-close
  traceback after the Explorer workflow completed.
- Explorer recheck: **1 passed in 16.16s**, [explorer-recheck.txt](explorer-recheck.txt).
  Assertions were retained; no application/browser error was suppressed.
- Resize-evidence browser gate: **2 passed in 16.17s**,
  [browser-final.txt](browser-final.txt). Both workflows also passed in the final
  focused gate, which regenerated the four retained screenshots.
- **322 distinct checks verified across these runs**: the original 317 checks,
  with both failures fixed/rechecked, plus original-workout-context regeneration,
  two unsupported-elapsed-edge cases, measured-zero power and the existing strict
  Windows-log-classification regression. Repeated checks
  are counted once. This is not a claim that the initial broad command was clean.
- `git diff --check` passed. A 4,000-activity preparation test remains under five
  seconds; this is a local synthetic bound, not a real-history reader benchmark.
- Read-only evidence loading leaves the complete synthetic database dump unchanged.

## Reproduction

From the repository root in PowerShell, using the existing environment:

```powershell
.venv\Scripts\python.exe -m pytest tests/test_chart_performance.py tests/test_chart_performance_browser.py tests/test_chart_plan_comparison.py tests/test_chart_plan_comparison_browser.py tests/test_chart_overview.py tests/test_chart_overview_browser.py tests/test_chart_explorer.py tests/test_chart_explorer_browser.py tests/test_nicegui_data.py tests/test_nicegui_workspace.py tests/test_activity_calendar_days.py tests/test_activity_workout_match.py tests/test_plan_revision_persistence.py tests/test_season_regeneration.py tests/test_temporal_metrics.py tests/test_phase2b_metric_freshness.py tests/test_phase3d2_threshold_service.py tests/test_phase3d2_threshold_migration.py tests/test_metrics_refresh.py tests/test_metric_refresh_transactions.py -q --basetemp .tmp_c4_regression -p no:cacheprovider
.venv\Scripts\python.exe -m pytest tests/test_chart_explorer_browser.py -q --basetemp .tmp_c4_explorer_recheck -p no:cacheprovider
$env:GARMIN_TEST_ARTIFACTS='reports/charts_c4'
.venv\Scripts\python.exe -m pytest tests/test_chart_performance.py tests/test_chart_performance_browser.py tests/test_season_browser_logs.py -q --basetemp .tmp_c4_log_gate -p no:cacheprovider
```

The initial broad result predates the five additional checks and final visual
refinements. Final focused tests cover all C4 calculations, data reads and browser
workflows. Other phase screenshots were written to scratch space and their tracked
reports remain intact. Scratch databases and debug captures were removed afterward.

## Inspected visual evidence

1440px desktop and 390px narrow captures, after plots fill their resized containers:

- [Default performance charts, desktop](test_performance_drilldown_bands_exports_refresh_and_responsive-fb8733ce6d/desktop.png)
- [Default performance charts, narrow](test_performance_drilldown_bands_exports_refresh_and_responsive-fb8733ce6d/narrow.png)
- [Power charts, desktop](test_performance_drilldown_bands_exports_refresh_and_responsive-fb8733ce6d/power_desktop.png)
- [Power charts, narrow](test_performance_drilldown_bands_exports_refresh_and_responsive-fb8733ce6d/power_narrow.png)

Visual review fixed single-day subsecond axis labels and clipped Plotly legends.
ISO date bounds serialize through NiceGUI, reset to the selected period, and show
readable date ticks. Wrapping HTML legends show ordered zone colors and distinguish
observed markers from same-color dashed medians. Screenshot waits check plot width
in both resize directions. Narrow document overflow assertions pass, including the
wide qualification table; tables scroll internally.

The browser clicks a rendered same-day point and opens activity 202, uses a keyboard
link to activity 101, returns with filters intact, validates live HR/pace bounds,
exports both measurement and peak-curve CSVs, exercises saved imperial pace bands,
and verifies empty/unavailable cycling states. A synthetic threshold change leaves
cached views intact until explicit Refresh data, then removes unqualified power.
Browser errors and application tracebacks remain failures. Multi-select changes are
awaited one at a time to avoid test races with server-rendered updates.

A later capture run reproduced the same Windows transport-close exception after
all UI assertions completed: [windows-close-attempt.txt](windows-close-attempt.txt).
C4 now reuses the repository's existing `season_browser_logs.application_logs`
classifier, verified in the focused gate. It recognizes only that exact WinError
10054 callback with two asyncio standard-library frames; application frames, other
errors and browser errors still fail. Raw final output is retained in
[browser-server.txt](test_performance_drilldown_bands_exports_refresh_and_responsive-fb8733ce6d/browser-server.txt).
The final run contains this classified platform-close event; it is not reported as
an application fix. Capture assertions also await the peak-source table's closed
state to avoid retaining a partially completed expansion transition.

## Semantics and limits

Ten views cover comparable-effort bands, running speed/HR efficiency, reported
cadence, weekly longest-run distance/duration, qualified signed durability, named
raw average/normalized power, variability and power/HR efficiency, seven power zones,
and observed peak means at 5/30/60/300/1200 seconds. Three views are selected initially.

Exact running subtypes and original confirmed workout families stay separate.
Unconfirmed identity is Unclassified, not inferred easy running. Terrain groups use
ascent density; unknown values remain unknown. Steady views screen duration, paired
coverage and speed variation and exclude known quality/events. Durability additionally
screens current/override LTHR, aerobic HR, both canonical/observed halves and unknown
elapsed edges. These product filters are stated rather than treated as validated
coaching or readiness cutoffs. Signed drift/decoupling values remain observations.

The app stores **running FTP**. Advanced power uses an explicit override or a current
verified running calculation and sufficient temporal support. It cannot qualify
cycling until a suitable sport-specific threshold exists. Measured zero power/zone
time stays distinct from missing; zero average power leaves variability undefined.
Zone vectors need all seven values/current threshold snapshots and adequate duration.
Peak durations require contiguous support and current stored provenance; missing
durations stay gaps. Period-best curves can span workout/terrain groups and disclose
exact source IDs and contributing counts. They do not predict future capability.

Track qualification reuses the shared elapsed-time engine: sequence-based duplicates,
no support through gaps over 30 seconds and no duration after the final sample.
The new reader streams one activity at a time, retaining exact weighted bins and
summaries; it never recomputes metrics or changes matches/plans. Refresh does not
repair stale persisted metrics. Query missingness is opt-in for Charts; legacy
consumer zero defaults are unchanged. Accessible table/CSV exports preserve excluded
rows, filter bounds, units, period and threshold source/age; the peak curve also has
its own source table/CSV.

Real imported-history performance/field use, packaged executable behavior, dark theme
and screen-reader software remain unverified. Cadence conventions are not converted;
no grade-adjusted pace, inferred workout classification, cycling FTP, coaching diagnosis
or predicted capacity was added. All listed implementation phases are locally verified;
release/field validation is separate. Client settings stayed unchanged; attributable
token/account-credit usage is unavailable.
