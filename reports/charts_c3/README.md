# Charts C3 evidence - 2026-10-04

C3 is implemented and locally verified. Starting tree: clean `1c05874`. No live
database, schedule, migration, package rebuild, commit, push or deployment changed.

## Final gates

- Broad regression: **171 passed in 116.03s**, recorded in [regression.txt](regression.txt).
- Final C3 focused gate: **24 passed in 22.01s**, recorded in [focused.txt](focused.txt).
- Combined unique scope: **174 tests** (171 broad + one additional complete-estimate
  case + two C3 browser workflows). Repeated tests are not added to this count.
- `git diff --check` passed.
- A 1,738-day synthetic multi-year comparison remains below a five-second bound;
  this is a local test bound, not a general real-database performance promise.
- A read-only plan snapshot leaves the complete synthetic database dump unchanged.

## Reproduction

From the repository root in PowerShell, using the existing development environment:

```powershell
.venv\Scripts\python.exe -m pytest tests/test_chart_plan_comparison.py tests/test_chart_overview.py tests/test_chart_explorer.py tests/test_chart_overview_browser.py tests/test_chart_explorer_browser.py tests/test_nicegui_data.py tests/test_nicegui_workspace.py tests/test_activity_calendar_days.py tests/test_activity_workout_match.py tests/test_plan_revision_persistence.py tests/test_season_regeneration.py -q --basetemp .tmp_c3_regression -p no:cacheprovider
$env:GARMIN_TEST_ARTIFACTS='reports/charts_c3'
.venv\Scripts\python.exe -m pytest tests/test_chart_plan_comparison.py tests/test_chart_plan_comparison_browser.py -q --basetemp .tmp_c3_gate -p no:cacheprovider
```

The broad result predates the one additional complete-estimate test, which passed
in the final focused gate. New test screenshots use the existing browser-artifact
fixture; prior phase reports are unaffected.

## Browser and visual evidence

Inspected at 1440px desktop and 390px narrow widths:

- [Charts and variance table, desktop](test_plan_comparison_drilldown_filters_refresh_and_responsive-b242c66eba/desktop.png)
- [Charts and variance table, narrow](test_plan_comparison_drilldown_filters_refresh_and_responsive-b242c66eba/narrow.png)
- [Plan/activity evidence dialog, desktop](test_plan_comparison_drilldown_filters_refresh_and_responsive-b242c66eba/evidence_desktop.png)
- [Plan/activity evidence dialog, narrow](test_plan_comparison_drilldown_filters_refresh_and_responsive-b242c66eba/evidence_narrow.png)

Actual rendered bar clicks open the correct plan week. Keyboard activity links and
Return to Charts preserve section/plan/week settings. The workflow reviews a match
in the synthetic database, refreshes, and verifies that its candidate label becomes
confirmed. Calendar/plan week, sport, time/distance and TSS/TRIMP controls, CSV export,
legacy/no-plan states and projection-corruption explanations are covered. Runtime
browser errors and application tracebacks remain test failures. Narrow tables scroll
inside their containers; document-width overflow assertions pass.

Initial browser attempts exposed test-fixture issues: zero-height empty-period bar
selection, Plotly's pointer-capturing overlay, the existing mobile navigation drawer
opening after reload, refresh replacing a control while its menu was opened, and
expansion reset after plan selection. The final tests click a real nonzero bar,
close the shell drawer, and await the replaced controls before continuing. Initial
dialog captures caught unfinished transitions; final captures wait for layout.
Visual review also found a product bug: Plotly inferred a date axis from ISO week
labels and omitted starred partial weeks. Categorical axes and a regression assertion
fix it. Unavailable actual load retains an explanation instead of an empty plot.

## Scope and limits

Canonical revisions and original match lineage are verified in a read-only snapshot.
Projected numeric totals are not trusted. Whole-workout measures require all leaves
to be accounted for; estimates are labeled. Planned TSS/TRIMP does not currently exist
in the canonical contract, so target load is unavailable. Variance requires complete
measurement and plan coverage of selected days; partial weeks compare selected days.

Due means scheduled before today. Rest does not enter completion denominators.
Confirmed matches and explicit completion are distinct from candidate/no-match rows.
Different recorded date or sport is a substitution; prescription differences are not
inferred. Actual totals follow recorded activity dates and include unmatched activities.
Confirmed activities outside filters can prove workout completion without entering
those actual totals. Unknown recorded dates are labeled; import completeness is unknown.

Calendar weeks start Monday; plan weeks start at the selected plan's first scheduled
date. Multiple active plans sum their sessions. Generic running targets cannot be
split into trail/indoor subtype prescriptions. Legacy schedules need explicit Plan
conversion before canonical comparison. Current revisions are not a historical
reconstruction of earlier intentions.

Real imported-history field use, packaged executable behavior, dark theme and screen-
reader software are unverified. No intensity-adherence evaluator, automatic matching,
inferred load target or schedule editing was added to Charts. C4 is still deferred.
Client settings were unchanged; token/account-credit attribution is unavailable.
