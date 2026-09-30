# Training app backlog

Last audited against the repository on 2026-09-30. Completed migration notes and other historical implementation logs have been removed; Git history remains the source for that detail.

## Recently completed

- [x] Data Query renders MCP list-of-object responses as structured, bounded tables, with charts for dated numeric data and raw JSON/text fallbacks for nested or non-tabular payloads. Regression coverage is in `tests/test_mcp_results.py`.

## Current priorities

### Deferred validated architecture work

- [ ] Phase 3G metric-refresh performance: deferred because the single-user
  targeted refresh has no demonstrated production performance problem.
- [ ] Windows code signing: deferred for single-user use; revisit before broader
  distribution.
- [ ] In-app version/commit display: release artifacts currently carry
  provenance in `SOURCE-COMMIT.txt`; UI display is deferred.
- [ ] Phase 3F legacy callable surfaces: keep quarantined and revisit only with
  evidence.
- [ ] Phase 3I DB/UI seam refactor: deferred because there is no demonstrated
  correctness need.

### Data Query

- [ ] Add saved query/tool presets for common athlete questions: recent long runs, weekly load, missing metrics, recovery trends, and upcoming workouts.
- [ ] Show MCP execution details beside the Run button: timeout, retry count, and the latest error. Basic connected/unavailable status already exists.

### Dashboard

- [ ] Turn diagnostics into action cards: one-click repair for missing derived metrics, Plan for no upcoming schedule, and Garmin Sync for stale data. The current missing-metrics card only links to Sync.
- [ ] Add a compact Today / next 7 days view combining planned sessions, recent compliance, and recovery signals. The current dashboard has separate upcoming-plan and recent-activity grids.

### Plan and Codex Coach

- [ ] Add a calendar/week view alongside the plan grid so training density, hard-day spacing, long-run placement, strength sessions, and race week are easy to inspect.
- [ ] Add import-history review and restore controls using the existing `plan_import_history` archive.
- [ ] Show the privacy-minimized coaching packet as a friendly summary before generation, with expandable raw packet JSON.
- [ ] Add “regenerate with feedback” controls that append athlete notes while preserving locked hashes, policy checks, and explicit approval.

### Compliance and activities

- [ ] Match planned workouts to completed activities using date window, sport, duration, and distance so moved workouts are recognized.
- [ ] Classify sessions as completed, partial, missed, extra, or moved; summarize streaks and weekly distance/duration variance.
- [ ] Add best-effort personal-record and benchmark badges for longest recent run, fastest common distances, highest TSS, and highest elevation gain.
- [ ] Add route/trackpoint quality indicators for missing FIT trackpoints, sparse GPS, and map-render gaps.

### Analysis and settings

- [ ] Add synchronized chart filters and an “explain this point/week” drill-down to source activities.
- [ ] Promote Sleep & Recovery from a Data Query tab to a first-class Recovery page or dashboard panel.
- [ ] Add database-health tools for backup location, database size, last successful sync, last derived-metrics refresh, and quick export.

### Quality and reliability

- [ ] Reduce card nesting and tighten card radius; reserve large headings for page-level context.
- [ ] Verify narrow-window layouts for the dashboard, plan, coach review, and query results so grids and controls do not overflow.
- [ ] Replace broad silent exception fallbacks in data, query, and ingest paths with logged warnings and user-visible data-quality messages when missing data changes conclusions.
- [ ] Add regression tests for compliance classification, plan restore, and Recovery/Dashboard integration. Data Query tabular rendering is already covered.

## Longer-term ideas

- [ ] Adaptive replanning: after each sync, compare completed load, missed workouts, recovery trends, and event timeline, then suggest a conservative plan adjustment for approval.
- [ ] Readiness-aware coaching: combine sleep, HRV, resting HR, stress, recent load, and athlete notes into daily guidance without automatically changing the plan.
- [ ] Subjective wellness log: add quick local check-ins for soreness, mood, sleep quality, illness, pain, and perceived exertion.
- [ ] Workout library: build reusable structured workouts with intent, target zones, progression rules, and export-friendly structure.
- [ ] Periodization templates: support base rebuild, maintenance, race-specific build, return from injury, and post-race recovery.
- [ ] Route and terrain intelligence: summarize climb distribution, available surface hints, weather context, and course-specific preparation needs.
- [ ] Goal forecasting: estimate whether current trends support the event date, flag schedule risk, and show what would need to change.
- [ ] Fueling planner: turn stored macro ranges and sodium settings into workout-duration-specific reminders while keeping medical and diet claims out of scope.
- [ ] Plan explainability: show the evidence behind each major plan change, including recent load, long-run history, recovery signals, and policy constraints.
- [ ] Scenario planning: compare conservative, standard, and ambitious proposals before accepting one.
- [ ] Multi-sport planning and compliance for cycling, triathlon, hiking/ultra, and strength-focused blocks.
- [ ] Calendar and structured-workout exports for the accepted active plan.
- [ ] Data provenance view: expose source tables, formulas, refresh timestamps, and confidence/coverage scores.
- [ ] Automated packaged-app smoke tests that launch the frozen app, visit every route, and verify key panels are populated.
- [ ] Dedicated privacy/export screen showing what is sent to Codex, what stays local, and how to delete credentials, logs, and browser sessions.
- [ ] First-run onboarding for sync, metric refresh, plan setup, baseline generation, and optional Codex sign-in.
