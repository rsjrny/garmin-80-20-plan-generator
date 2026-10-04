# Charts Page Improvement Plan

Status: C1-C4 implemented and locally verified; C4 verified 2026-10-04.

## Objective

Turn the Charts page from a collection of independent plots into a focused
training-analysis workspace that helps an athlete answer:

1. How much training am I doing, and how is it changing?
2. Is the mix of easy, moderate, and hard work appropriate?
3. Are pace, heart rate, efficiency, and durability improving?
4. Which activities caused an unusual week or data point?
5. Is a conclusion based on complete, comparable data?

The page should prioritize interpretation and drill-down over maximizing the
number of charts shown at once.

## Current-State Findings

### Existing behavior

- The page has a start-date filter, optional multi-sport filter, chart
  multi-select, and an explicit Update charts action.
- Eleven charts are selected and rendered by default in a fixed two-column
  grid.
- Available views cover activity count, weekly distance, weekly duration,
  average heart rate, pace or speed, training stress, HR zones, elevation,
  longest activity, load versus duration, and drift/decoupling.
- Charts are generated independently with Plotly and do not share selection,
  hover, zoom, or activity drill-down.
- Unit and pace/speed preferences are respected.
- Missing optional metrics generally cause a chart to be omitted, but the page
  does not explain metric coverage or why a chart is unavailable.

### Data already available to the page

The chart query already exposes more useful data than the page visualizes:

- Moving time
- Cadence
- Average and normalized power
- TRIMP and TSS
- Efficiency factor
- Variability index
- Heart-rate zones
- Power zones
- Aerobic decoupling and heart-rate drift
- Peak power durations

For the most recent one-year period in the current database, the page has 271
activities across running, walking, trail running, strength, hiking, and indoor
activities. Heart rate, speed, load, drift, cadence, power, and elevation have
enough coverage to support richer views. Metric availability varies by sport,
so all-sport aggregation must remain honest about comparability.

### Main usability and analysis gaps

1. **No hierarchy.** Rendering every chart by default creates a long dashboard
   where high-value signals are not distinguished from exploratory plots.
2. **Weak time controls.** There is no end date, quick range, previous-period
   comparison, or indication that the current week is incomplete.
3. **Mixed-sport ambiguity.** Pace, distance, cadence, power, and elevation are
   not equally meaningful across running, walking, strength, and indoor cardio.
4. **No trend context.** Raw activity scatterplots lack rolling trends,
   baselines, thresholds, or prior-period comparisons.
5. **No drill-down.** A surprising point or week cannot be opened as an
   activity or inspected as a contributing activity list.
6. **Missing data can look like zero.** Several aggregations fill missing values
   with zero, which may understate weekly load or elevation without warning.
7. **Elapsed and moving time are not distinguished.** Weekly duration currently
   uses elapsed duration even though moving time is available.
8. **No planned-versus-completed context.** Actual training is not compared with
   the active plan.
9. **Limited responsive behavior.** A fixed two-column grid and fixed chart
   height need explicit narrow-layout handling.
10. **Testing is calculation-focused.** Existing tests verify chart inclusion
    and a few properties, but not missing-data semantics, filter behavior,
    responsiveness, accessibility, or drill-down.

## Product Principles

- Lead with decisions and trends, not a wall of plots.
- Compare like with like; use sport-aware metrics and defaults.
- Distinguish zero, missing, stale, and partial-period data.
- Show the activities behind every aggregate.
- Use consistent colors, week boundaries, units, and hover formatting.
- Avoid presenting derived load or readiness metrics with false precision.
- Keep an exploratory mode for users who want the full chart catalog.

## Recommended Information Architecture

### 1. Overview

Default landing view with a compact summary and four high-value charts:

- Weekly training duration
- Weekly training load
- Intensity distribution
- Sport-specific performance trend

The performance trend should adapt to the selected sport:

- Running, trail running, and walking: pace, optionally paired with heart rate
  or efficiency factor
- Cycling-like activities when present: power and normalized power
- Strength training: duration and activity frequency until strength-specific
  volume is available

### 2. Load and consistency

- Weekly duration and moving duration
- Weekly TSS or TRIMP, with clear source labeling
- Four-week rolling load
- Week-over-week and previous-period change
- Training-day frequency and rest-day distribution
- Optional monotony and strain only after definitions and safety language are
  agreed and tested

### 3. Intensity

- Weekly HR-zone distribution in both absolute time and percentage modes
- Power-zone distribution when FTP-derived metrics are valid
- Easy/moderate/hard summary aligned with the application's training
  methodology where possible
- Threshold provenance and freshness indicator

### 4. Performance and durability

- Sport-specific pace or speed trend with a rolling median
- Heart rate at comparable pace or pace at comparable heart rate
- Efficiency-factor trend
- Aerobic-decoupling trend with sample qualification
- Cadence trend
- Power, normalized power, and variability-index trends where available

### 5. Explorer

Retain the full chart catalog with chart selection, but do not select all charts
by default. This is the appropriate home for activity distribution, elevation,
longest activity, load-versus-duration, and other secondary views.

## First Shippable Slice: Useful Overview

### Filter bar

- Add quick ranges: 4 weeks, 12 weeks, 6 months, 1 year, year to date, and
  custom.
- Add an end-date control for custom ranges.
- Make sport a single primary comparison selection by default, with an explicit
  All sports option for additive metrics such as time and load.
- Preserve filters in the URL or session so refresh and navigation do not reset
  the analysis.
- Refresh automatically after a deliberate filter change, with a short debounce
  if needed; retain an Update button only if query cost justifies it.

### Summary strip

Show for the selected period:

- Activity count
- Training time, preferring moving time and labeling fallbacks
- Distance for distance-based sports
- Training load with its source, such as TSS or TRIMP
- Elevation gain when relevant
- Change versus the immediately preceding equal-length period

Do not calculate percentage change when the comparison value is absent or too
small to be meaningful.

### Default charts

1. **Weekly training volume**
   - Stacked by sport when All sports is selected.
   - Toggle between time and distance.
   - Include zero-activity weeks so gaps remain visible.
   - Mark the current incomplete week.

2. **Weekly training load**
   - Display the chosen load source explicitly.
   - Overlay a four-week rolling average.
   - Show data-coverage warnings instead of converting missing load to zero.

3. **Intensity distribution**
   - Stacked HR zones with absolute-hours and percentage toggles.
   - Use stable, accessible zone colors throughout the application.
   - Explain when the athlete threshold is unavailable or metrics are stale.

4. **Performance trend**
   - Use a sport-appropriate metric.
   - Show individual activities plus a rolling median.
   - Size or fade points by duration rather than using area without explanation.
   - Exclude invalid zero speeds and label indoor/outdoor distinctions where
     relevant.

### Drill-down

- Include activity ID, display name, date, sport, distance, duration, and
  relevant metric values in chart source rows.
- Improve hover text with human-readable units and data provenance.
- Clicking a point opens the corresponding Activities detail view.
- Clicking a weekly aggregate opens a compact list of contributing activities.
- Provide a Reset zoom control consistently across charts.

### Data-quality communication

- Show a small coverage indicator such as `Load available for 267 of 271
  activities` when a metric is incomplete.
- Treat `0` and `missing` as distinct values through query, aggregation, and
  rendering.
- Explain omitted charts in place instead of silently removing them.
- Label partial current weeks and partially covered comparison periods.

## Follow-Up Slice: Planned Versus Completed

- Overlay planned duration, distance, and load by week where available.
- Show completed, planned, and completion percentage without treating rest days
  as missing workouts.
- Make substitutions and unmatched activities visible.
- Let the user switch between calendar weeks and training-plan weeks if plan
  boundaries differ.
- Link a variance to the relevant Plan or Activities view.

This slice should reuse the canonical active-plan and activity-to-workout match
logic rather than implementing a second matching system in the Charts page.

## Follow-Up Slice: Deeper Performance Analysis

### Comparable-effort trends

- Pace at a configurable heart-rate band
- Heart rate at a comparable pace band
- Efficiency factor by sport and workout type
- Decoupling for sufficiently long, steady aerobic sessions only

Each chart must state its qualification rules. Do not combine intervals,
strength sessions, hikes, and steady runs into one trend.

### Running views

- Pace and cadence trends
- Long-run distance and duration progression
- Elevation-normalized filtering or grouping, without claiming grade-adjusted
  pace until a validated model exists
- Indoor versus outdoor distinction

### Power views

- Average versus normalized power
- Variability index
- Power-zone distribution
- Peak-power curves for supported durations

Only show power analysis when FTP provenance and activity coverage are
sufficient.

## Visual and Interaction Direction

- Use one responsive column below the desktop breakpoint and two columns only
  when cards retain readable plot widths.
- Give the primary volume chart a full-width position.
- Keep legends in consistent positions and avoid legends that obscure data.
- Use a shared palette for sports and a separate, ordered palette for zones.
- Apply unified hover behavior to aligned time-series charts when practical.
- Use concise chart subtitles for scope, metric source, and coverage.
- Provide a textual summary of the selected period before the plots.
- Respect reduced-motion preferences and avoid animated redraws for routine
  filter changes.

## Accessibility

- Do not rely on color alone; use labels, patterns where practical, and direct
  hover values.
- Give every filter and chart action an accessible name.
- Ensure keyboard access to filters, chart menus, and activity drill-down.
- Provide a tabular alternative for each chart or a downloadable accessible
  summary table.
- Use sufficiently large axis and legend text at narrow widths.
- Verify chart colors and focus indicators in supported themes.
- Avoid announcing every chart point to screen readers; expose concise summaries
  and the underlying table instead.

## Technical Direction

### Separate preparation from rendering

Move chart data preparation out of the page builder into pure, testable helpers.
Suggested boundaries:

- Filtered activity-series query
- Period and comparison-window calculation
- Weekly reindexing, including empty and partial weeks
- Metric coverage calculation
- Sport/metric compatibility policy
- Summary metrics
- Figure builders consuming prepared frames

### Preserve missingness

- Do not call `fillna(0)` before deciding whether zero is semantically valid.
- Aggregate each metric from its valid observations.
- Carry valid-count and total-count values beside aggregates.
- Render gaps or coverage annotations when data is incomplete.

### Query additions

- Add activity identifier and display name for drill-down.
- Include active-plan weekly totals through a separate query or service boundary.
- Continue using current temporal-metric provenance checks.
- Consider temporary HR-zone recomputation only if its cost is acceptable and
  the user needs current thresholds immediately.

### State and performance

- Build only the active section's charts.
- Cache prepared chart data by database revision, date range, sport, unit system,
  and threshold provenance.
- Avoid requerying when only presentation options change.
- Measure performance with multi-year activity histories.

## Delivery Order

### Phase 1: Correctness and hierarchy

- Add metric-coverage and missing-value semantics.
- Introduce quick date ranges, end date, and sport-aware filtering.
- Add the summary strip and focused default overview.
- Reindex weekly series to show inactive weeks and mark partial weeks.
- Add rolling trends and consistent hover formatting.
- Make the grid responsive.

### Phase 2: Drill-down and exploration

- Add activity identifiers and names to chart data.
- Link activity points to Activities detail.
- Add weekly-contributor inspection.
- Move the complete catalog into an Explorer section.
- Preserve filter and section state.

### Phase 3: Planned-versus-completed analysis

- Add weekly planned totals using canonical plan data.
- Reuse activity/workout matching.
- Add variance and adherence views.
- Test substitutions, unmatched activities, rest days, and partial weeks.

### Phase 4: Advanced sport-specific analysis

- Add comparable-effort running trends.
- Add cadence and qualified durability views.
- Add power, normalized-power, variability, zone, and peak-power views.
- Add data exports and accessible tables.

## Phase 1 Acceptance Criteria

- The initial page shows no more than four prioritized charts.
- An athlete can select common date ranges without manually entering dates.
- Custom ranges support both start and end dates.
- Mixed-sport displays use only additive comparable metrics, or clearly separate
  sports.
- Weekly charts show inactive weeks and distinguish an incomplete current week.
- Missing metric values do not silently become zero.
- Each chart states its metric, units, scope, and relevant coverage.
- Pace axes and hover values use readable time formatting.
- Summary metrics include a previous-period comparison where valid.
- The layout is usable at desktop and narrow viewport widths.
- Chart preparation has deterministic tests independent of NiceGUI rendering.
- Existing units and pace/speed preferences continue to work.

## Phase 3 Acceptance Criteria

- Read verified active canonical workouts and reuse confirmed match/origin identities.
- Compare known duration/distance and the explicitly selected load source without inventing missing targets.
- Show weekly variance, coverage and due-session completion with separate rest, candidate, explicit-completion and substitution evidence.
- Keep unmatched activities in actual totals; distinguish scheduled dates from recorded activity dates.
- Support Monday calendar weeks and first-scheduled-date plan weeks for one selected plan.
- Preserve filters/section/plan/alignment through navigation; provide accessible week evidence, Plan/Activity links and CSV.
- Test missingness, estimates, rest, unmatched/candidate activity, substitutions, partial/inactive weeks, year boundaries and preserved origins; verify desktop/narrow rendering.

## Test Scenarios

- One sport with complete metrics
- Several sports with incompatible pace and distance semantics
- Period containing weeks with no activities
- Current partial week
- Custom start and end dates
- Missing TSS with available TRIMP
- Partial HR-zone coverage
- Stale threshold-derived metrics
- Zero-distance strength and indoor-cardio activities
- Invalid or zero average speed
- Moving time available and unavailable
- Metric and imperial preferences
- Pace and speed display preferences
- Activity-point and weekly-aggregate drill-down
- Plan weeks with completed, substituted, unmatched, and skipped workouts
- Narrow viewport and keyboard-only navigation
- Multi-year history with thousands of activities

## Decisions to Make During Discovery

- Whether TSS, TRIMP, or an explicit user choice is the primary load metric
- The exact sport groupings used for comparable analysis
- Rolling-window length and whether to use median or mean by metric
- Whether chart state belongs in the URL, session storage, or saved settings
- Whether NiceGUI Plotly events provide reliable point-click and shared-hover
  behavior for the required interactions
- How planned-versus-completed weeks align with calendar weeks
- Whether dark theme is currently supported or should remain out of scope

## Out of Scope for the First Slice

- Predictive race-performance modeling
- Injury-risk or medical recommendations
- A single proprietary readiness score
- Automatic causal coaching conclusions
- Grade-adjusted pace without a validated model
- Arbitrary user-authored formulas
- Replacing Plotly or the existing application shell

## Definition of Done

The Charts page is successful when an athlete can identify a meaningful change,
understand which data supports it, inspect the contributing activities, and
compare the result with recent training or the active plan without navigating a
long wall of disconnected plots.

## C1 discovery and decisions — 2026-10-02

- Existing Charts page and legacy 11-chart catalog: `ui_nicegui/pages.py:charts_page` / `_training_chart_figures`. Data: `ui_nicegui/data.py:chart_dataframe` -> `db/queries.py:get_activities_dataframe`. No repository AGENTS.md found. T1/T2 changes retained.
- Chart query uses canonical activity calendar days and provenance checks, but substitutes stale/missing zones with zero. Add opt-in missingness preservation for Charts, bounded end date, and zone provenance status; preserve default behavior for other query consumers.
- New pure overview preparation/figure module. Monday calendar weeks; inclusive date ranges; immediately preceding equal-day comparison; moving duration with labeled elapsed fallback. Empty weeks are zero, observed weeks without measurements are gaps, partial coverage is explicit.
- Default 12 weeks; quick/custom ranges and a single sport selection. TSS and TRIMP are explicit choices, never combined or substituted per activity. Performance is single-sport only: pace/speed for running/walking/hiking, power for cycling, duration/frequency for other sports. Rolling 28-day activity median; weekly load four-week mean only for fully covered complete weeks.
- Four chart slots, unavailable messages in place; volume full width, others responsive. Presentation toggles reuse prepared data. URL/session state, Explorer, point clicks, and weekly contributors remain C2 scope per roadmap. Keep legacy catalog builders intact until C2. Current shell has no dark-mode control.

## C1 verification and handoff — 2026-10-02

### Implemented behavior

- Focused four-slot overview: weekly volume, selected TSS/TRIMP load, HR-zone distribution, and sport-specific performance. Unavailable charts retain explanatory slots; All sports omits a performance plot instead of mixing incompatible metrics.
- Quick 4/12-week, calendar six-month/year, YTD, and inclusive custom dates. Automatic deliberate filter updates, end-date/order validation, primary sport selection, and explicit Refresh data. Presentation changes use page-local cached query results; refresh rereads the database. No persisted filter state yet (C2).
- Summary includes activity counts, moving-preferred training hours with current/previous elapsed-fallback counts, selected load, and distance/elevation for relevant single sports. Equal-day previous period; percentage comparisons require complete metric coverage and a meaningful nonzero baseline. Import completeness is explicitly unknown.
- Pure `analytics/chart_overview.py` owns ranges, normalization, missingness, coverage, Monday-week reindexing, summary, and figures. Real zeros remain zero; empty weeks are zero; measured weeks lacking a metric are gaps; partially measured totals carry counts and notes. Partial range/current weeks use an explained asterisk. Four-week load mean requires four fully covered complete weeks.
- HR zones keep measured-only coverage and existing provenance checks. Stale/absent metrics no longer become zero in the Charts query. Effective LTHR value or unknown-threshold notice is displayed; legacy stored thresholds can remain unknown. No writes or temporary HR recomputation.
- Single-sport performance: readable pace/speed for running/walking/hiking; average/normalized power for cycling; duration plus daily frequency for other sports, including strength. Zero/invalid speeds and powers excluded; 28-day activity median and duration-based point opacity are explained.
- Responsive one/two-column cards, volume full width, wrapped controls, weekly table with readable headers and CSV download. Legacy catalog/builders preserved for C2 Explorer.
- Query additions: activity ID, zone status, optional canonical-calendar end date, opt-in missing zone values. Existing query consumers retain default zone-zero behavior. TSS remains provenance-approved derived TSS with Garmin stored-TSS fallback, as before; TSS and TRIMP are never substituted or combined per activity.

### Acceptance evidence

- Cross-feature suite: 60 passed (new overview/query/browser cases plus existing NiceGUI data, local calendar-day, and T1/T2 tests). After provenance/summary/table refinements: 49 Charts/shared data/date tests passed; final strength-frequency refinement: all eight pure overview tests passed. No failures or warnings in final runs.
- Deterministic cases cover inclusive/leap/calendar ranges, previous periods, current/boundary partial weeks, empty weeks, actual zero load, absent load, moving-time fallback, partial/stale zones, invalid numerics, metric/imperial units, pace/speed, cycling power, strength duration, and empty periods. Query test verifies local day at GMT boundary and stale/missing zones.
- Browser verifies three initial all-sport plots/four slots, four plots for running, quick/custom dates, automatic updates, TSS/TRIMP selection, percentage zones, distance mode, invalid range message, and 390px layout without horizontal overflow. Desktop and narrow screenshots inspected in `reports/charts_c1/`.
- Multi-year preparation test: 4,000 activities, fewer than 260 week rows, under five seconds. This is a synthetic bound, not a live database latency guarantee.

### Remaining scope and limitations

C2 retains activity/weekly-contributor drill-down, Explorer UI, display names, and navigation/refresh filter persistence. Threshold freshness uses the existing stored provenance contract, not a new age-based policy. Current shell has no dark-mode control. Real-history field trial and live usage/credit values are unavailable. Cached frames remain in memory until filter/date changes or explicit refresh; no database-revision cache invalidation was added.

Changes remain uncommitted: `analytics/chart_overview.py`, `db/queries.py`, `ui_nicegui/data.py`, `ui_nicegui/pages.py`, `tests/test_chart_overview.py`, `tests/test_chart_overview_browser.py`, this plan, roadmap, and C1 screenshots. Prior T1/T2 and planning changes preserved; no commit/push/publish or schedule changes. Next milestone is C2.

## C2 verification and handoff — 2026-10-02

### Implemented behavior and decisions

- Overview and Explorer activity points open canonical Activities details by ID, independent of history-grid filters. Positive integer activity_id query parameters are validated; deleted IDs show the existing unavailable-detail message. Return to Charts restores analysis. Rolling medians remain trends; daily-frequency points open the day's sources.
- Weekly bars open paginated source lists within the selected date/sport filters. Volume stacks respect the clicked sport. Missing-metric activities remain visible with blank values and labeled moving/elapsed fallbacks. Empty weeks explain that no activities contributed. Inspect week and Show weekly contributors provide keyboard access.
- The activity source table supplies keyboard detail links, display names, date, sport, distance, preferred duration, chosen load, HR, pace/speed, elevation, power, and stored drift/decoupling. Detail links appear first for narrow screens; wide metric columns scroll inside the table.
- Explorer retains all eleven catalog choices and defaults to average HR. One shared catalog definition preserves the legacy builder interface. Only selected charts in the active section are built. Explorer uses C1 calendar weeks, units, provenance, moving-preferred duration, explicit TSS/TRIMP choice, empty-week reindexing, and missingness. Incompatible pace and unavailable metrics have explanatory slots.
- Plain integer curve/point events resolve against server-owned trace metadata with explicit activity IDs/full ISO Monday dates. Same-day and excluded points retain correct identity. Hovers include source identity and measurements. Reset zoom preserves reversed pace axes and handles secondary axes only when present, using existing Plotly update APIs.
- NiceGUI tab storage, namespaced by database path, remembers filters, custom dates, section, presentation, and catalog selection across reload and shell/detail navigation while the application/tab session lives. New tabs receive defaults. Quick ranges follow today; custom dates stay fixed; invalid/stale selections fall back safely. Application-restart persistence is outside this session-state design.
- The chart query uses existing activity_name/name/title columns when present and falls back to Activity ID; no schema migration or activity-data writes. Empty prepared frames carry an ID column for safe source rendering.

### Acceptance evidence

- Final suite: 76 passed without warnings: C2 pure/browser, C1 pure/browser, NiceGUI data/workspace, activity calendar-day, and track visuals.
- Existing activity-grid browser/serialization checks: two passed using bundled Playwright Chromium with a test-only launcher override. System Chrome was unavailable to the unchanged default launcher; the override run emitted one pytest import-rewrite warning.
- Browser exercised actual rendered overview/Explorer points, same-day IDs, weekly bars, keyboard detail links, custom/quick filters, reload/shell/detail navigation, isolated new tabs, missing detail IDs, missing-metric/empty contributors, Reset zoom, and 390px overflow checks. Final runs had no browser/runtime errors.
- Four desktop/narrow Explorer and contributor-dialog screenshots inspected in reports/charts_c2/. git diff --check passed. Original C1 screenshots preserved.

### Changed files and next scope

C2 remains uncommitted on baseline e79bad7; the working tree was clean at start. New files: analytics/chart_explorer.py, tests/test_chart_explorer.py, tests/test_chart_explorer_browser.py, and four reports/charts_c2/ screenshots. Updated: analytics/chart_overview.py, db/queries.py, ui_nicegui/pages.py, this plan, roadmap, and backlog (source paths are under src/garmin_data_hub/).

G1 is next: review actual usage, remaining allowance/credits, and choose the next authorized scope. Real-history field trial and live usage/credit values remain unavailable. C3/C4 and yearly scheduling stay deferred. No commit, push, deployment, or live schedule changes.

## C3 discovery, verification and handoff - 2026-10-04

User authorized C3 with "lets work on c3". Starting tree was clean at `1c05874`.
Historical C2 next-scope/uncommitted statements above are superseded by this handoff.

### Implemented behavior and decisions

- Charts adds **Plan comparison**, leaving Overview and the eleven-chart Explorer intact. It compares weekly planned duration/distance with all filtered actual activities, selected TSS/TRIMP where measured, and due-session completion evidence. Active-plan selection, calendar/plan week alignment and C2 filters persist in the running tab across refresh/navigation. Only this section reads plan data; presentation changes reuse cached facts. Refresh explicitly rereads plans/matches and activity metrics.
- `services/chart_plan_data.py` opens a read-only SQLite snapshot, uses `active_planned_workout` membership/current plan pointers, verifies immutable revisions and origin lineage with the canonical repository, and reads workout leaves rather than mutable projected totals. Missing/duplicate/moved projection membership and corrupt content produce an explained unavailable state. Legacy/unmanaged rows are counted and excluded until explicitly converted in Plan. No migration or conversion occurs in Charts.
- Duration/distance totals require every leaf to have that measure with an authoritative or estimated role. Estimates remain labeled; partially measured workouts have an unknown whole-workout total. Canonical plans currently prescribe neither TSS nor TRIMP, so planned load and load variance remain unavailable for training sessions even if a mutable projection contains a number. No pace-to-duration or HR-to-load target is invented. Actual measurements reuse C1 provenance and moving-time/elapsed-fallback rules.
- Monday calendar weeks retain inactive weeks. Plan weeks begin on the selected plan's first scheduled date; dates before that anchor are labeled "Before plan". Multiple active plans are additive; plan-week alignment requires exactly one selected plan. Coverage is the union of active plan date extents, with no assumptions outside them. Date-boundary, current-day and incomplete-plan weeks are labeled partial; totals include only selected dates. Volume variance/percentages require complete measurement and plan coverage of selected days. Percentages also require a positive planned denominator.
- Confirmed matches use existing `load_confirmed_match` and immutable origins, including carried workouts from Y3 regeneration. Candidate/rejected records never prove completion. Explicit season completion without a confirmed activity remains separate. Rest is excluded from session denominators; due means scheduled before today, so today's sessions are not prematurely counted as missed. Unconfirmed past rows say "No confirmed completion", not "skipped". Confirmed existing activities with unknown recorded dates retain identity evidence with an explicit unknown-date label; future-dated activities do not prove recorded completion.
- A confirmed moved-date or different-sport activity is labeled a substitution. Same-sport prescription differences are not inferred from totals. Linked activities outside the date/sport filters still provide completion evidence and are labeled accordingly. Actual weekly totals always follow recorded activity dates; plan completion follows scheduled dates. Unmatched and unconfirmed candidate activities remain in actual volume. Confirmed links to another selected-out plan/date are distinguished from unmatched activities. Completion is evidence of occurrence, not native intensity compliance.
- RUNNING plan targets apply to running/trail_running/indoor_running; subtype filters narrow actuals but cannot split a generic running prescription. Strength maps to strength/strength_training, mobility to mobility/yoga/pilates. This limitation is explained. Existing actual sport types are not silently merged in the activity query.
- Server-owned week identities resolve chart clicks. A keyboard week selector, scheduled-workout evidence and activity matching tables expose rest, candidates, estimates, explicit completion, review reasons and substitutions. Activity detail/return links retain Charts state; Plan/Seasons links allow schedule review. Weekly variance/coverage has a labeled CSV export. Missing load charts retain an unavailable message. Categorical axes preserve ISO labels with partial-week asterisks.

### Acceptance evidence and limitations

174 distinct checks are verified: a 171-check Charts/data/calendar/matching/revision/
regeneration regression suite plus the additional complete-estimate case and two new
C3 browser workflows. The final focused suite passed all 24 checks in 22.01 seconds;
reruns are not added to the distinct total. Browser checks cover rendered bar clicks,
keyboard links, matching review followed by explicit refresh, TSS/TRIMP and time/
distance switches, saved state, calendar/plan weeks, CSV, empty/legacy schedules and
corruption explanations. Pure tests include imperial estimates, leap/year boundaries,
overlapping plans, incomplete coverage, rest, substitutions and preserved origin matches.
A multi-year 1,738-day synthetic history stays under the five-second preparation bound.
Read-only inspection preserves the complete synthetic database dump.

Desktop/390px charts, tables and evidence dialogs were inspected. Narrow tables scroll
within their containers; page-width overflow assertions pass. Test fixtures close the
existing mobile shell drawer after reload and wait for refresh controls to be replaced.
Visual inspection caught and fixed Plotly date-axis inference dropping starred week
labels. Detailed commands, results, screenshots and initial fixture failures are in
[reports/charts_c3/README.md](reports/charts_c3/README.md). `git diff --check` passes.

Current revisions reflect the active schedule rather than historical plan intentions.
Import completeness, real-history field use, packaged executable behavior, dark theme
and screen-reader software remain unverified. Planned load estimation, inferred
substitutions, automatic match confirmation and intensity-adherence evaluation were
not introduced. No live database/schedule changes, migration, commit, push, deployment
or packaging rebuild. Client settings were unchanged; attributable tokens/credits
are unavailable. Stop at C3; C4 advanced sport-specific analytics needs separate scope.

Changed files: new `analytics/chart_plan_comparison.py`, `services/chart_plan_data.py`,
`ui_nicegui/chart_plan_comparison.py` and two C3 test modules; updated
`analytics/chart_explorer.py`, `ui_nicegui/pages.py`, README, this plan and roadmap.

## C4 discovery, verification and handoff - 2026-10-04

User authorized C4 with "continue with c4". Starting HEAD was `1c05874` with the
completed C3 changes still uncommitted. This handoff supersedes earlier deferred-C4
statements. All C3 implementation and evidence remain intact.

Charts now has a **Performance** section with ten selectable views; the defaults
are pace at HR band, cadence and qualified durability. HR and pace bounds, minimum
steady duration, terrain and chart selection persist in the current tab. Pace bounds
are stored as m/s and shown as min/km or min/mi without repeatedly rounding stored
values. Invalid live bounds show an explanation while preserving last valid state.
Presentation changes reuse evidence; Refresh data rereads tracks, original confirmed
workout matches, canonical revisions and threshold provenance in a read-only snapshot.
No refresh, matching or schedule-writing engine is invoked by the new reader.

Comparable-effort trends use one exact running subtype, at least the selected duration
(default 20 minutes), >=90% paired temporal coverage, speed CV <=10%, and the selected
ascent-density group. Confirmed quality/event and non-running prescriptions are
excluded. Unknown workout identity is explicitly Unclassified; signal screening does
not establish an easy-session label or exclude every unconfirmed interval pattern.
Groups split by terrain and original confirmed workout family. Low ascent is <=20
m/km; unknown ascent stays separate. No grade-adjusted pace is inferred.

Pace-at-HR and HR-at-pace require ten observed in-band minutes, weighted by exact
supported duration. Running efficiency is paired mean speed / mean HR (m/s per bpm).
Samples use the shared engine's timestamp ordering, sequence-based duplicate resolution,
30-second gap cap and no invented final support. The reader streams one activity at a
time and retains weighted signal bins and summaries rather than full-history tracks.

Durability additionally requires >=40 minutes, usable current estimated/override
LTHR, mean HR <=90% LTHR and >=90% paired support in each half. Both the canonical
metric span and complete observed window are screened; unknown elapsed edges impose
a conservative midpoint-support penalty. Current persisted **speed/HR** decoupling
and HR drift retain signed values; mixed power/speed aerobic decoupling is not used.
These are stated product screening filters, not validated coaching cutoffs or readiness
predictions. Heat, terrain, device conventions and sensor quality remain relevant.

Cadence uses valid imported reported values without doubling or per-leg conversion.
Weekly longest running activity uses largest recorded distance, requiring measured
duration too; deterministic ties use duration/date/ID. It plots both distance and
moving-preferred duration with elapsed fallback sources in the CSV. Running, trail_running and
indoor_running are distinct selections. Partial periods follow the selected date range.

Discovery confirmed that the existing FTP contract is **running FTP**, not cycling FTP.
Advanced power therefore requires an explicit override or current verified running
calculation and >=95% temporal power coverage. Cycling/unrelated sports explain why
that threshold cannot qualify them. Average and normalized power retain their named
imported scalars; variability is NP / average power, and efficiency is NP / average
HR (W per bpm), with >=90% paired power/HR support. These ratios stay separate from
running speed/HR efficiency. Measured zero power is retained; zero average power
leaves variability unavailable. Power trends preserve terrain/workout groups.

Power zones require all seven nonnegative measurements, current metric version,
current FTP snapshot and total zone support within 95-102% of elapsed/window time.
Known zero is preserved; missing/partial/stale zones remain unavailable. Nullable
power-zone projection is opt-in for Charts, preserving legacy zero defaults elsewhere.
Same-day bars use activity-ID categories and ordered zone colors. The peak curve uses
current persisted exact-duration means (5/30/60/300/1200s) and requires a contiguous
observed power run at least that long. Unsupported durations remain gaps. Period-best
values can span workout/terrain groups; source ID and contributor count are explicit.

Point clicks resolve server-owned IDs; dashed median lines have no activity target.
The measurement/qualification table includes all selected activities and keyboard
links. CSV includes units, filter bounds, period, threshold source/age and exclusion
reasons. Peak-power source table/CSV separately records best values and contributing
counts. Reset zoom restores selected date bounds and reversed pace direction. Wrapping
HTML legends and ISO date bounds fix phone clipping and same-day subsecond labels;
the live NiceGUI serializer is tested. Plot-width assertions await actual resizing.

**322 distinct checks verified** across Charts/data/calendar/matching/revision/
regeneration/temporal/threshold suites and targeted rechecks. The initial broad run
had 315 passed and two failures: the subsequently fixed pandas date-bound serialization
and a Windows socket-close traceback after the completed Explorer workflow. Explorer
passed its recheck. Final focused/shared-log gate: 34 passed in 21.89s. The recurring
Windows close event uses the existing narrowly tested stdlib-only classifier;
raw logs are retained and application/browser errors still fail. Four desktop/390px captures
are recorded in [reports/charts_c4/README.md](reports/charts_c4/README.md). Tests include
unsupported elapsed edges, original context after regeneration, threshold provenance,
same-day identities, unit preferences, exports, missing/stale measurements and a
4,000-activity preparation bound. Real imported-history performance, packaged executable,
dark theme and screen-reader software remain unverified.

New files: `analytics/chart_performance.py`, `services/chart_performance_data.py`,
`ui_nicegui/chart_performance.py`, two C4 test modules and `reports/charts_c4`.
Updated chart query/state/page integration, README, this plan and roadmap. Changes
remain uncommitted; no live database/schedule change, migration, deployment or package
rebuild. Client settings unchanged; attributable account usage/credits unavailable.
All planned Charts phases are locally verified. Additional threshold types, prediction,
coaching interpretation and release/field validation remain separate work.
