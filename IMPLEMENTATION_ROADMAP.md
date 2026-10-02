# Training Planner Implementation Roadmap

Created: 2026-10-02
Status: T1, T2, and C1 implemented and locally verified; C2 is next.

## Objective and source plans

Deliver the highest-value improvements first, verify each phase, and review actual
usage before committing remaining capacity to yearly scheduling.

- [Activity Track Visuals](ACTIVITY_TRACK_VISUALS_PLAN.md)
- [Charts Page Improvements](CHARTS_PAGE_IMPROVEMENT_PLAN.md)
- [Yearly Multi-Event Scheduling](YEARLY_MULTI_EVENT_SCHEDULING_PLAN.md)

These feature plans define scope. This roadmap defines order, settings, checkpoints,
and session handoffs. Update both as implementation discovery resolves decisions.

## Recommended first investment

Complete Track Phases 1-2, then Charts Phases 1-2. This delivers route overlays and
an actionable Charts page. Yearly scheduling starts after an actual usage review.

All phases use GPT-6.1 Sol at Standard speed. Medium is the default reasoning level;
High is recommended for scheduling architecture and preservation transactions.
Select these settings in the client before starting a phase. A Markdown file does
not automatically change the running chat's model, speed, or reasoning.

| Order / ID | Deliverable | Model | Speed | Reasoning | Token planning allowance | Chat strategy |
| --- | --- | --- | --- | --- | ---: | --- |
| 1 / T1 | Track discovery and pace-colored route | GPT-6.1 Sol | Standard | Medium | 120k | New implementation chat |
| 2 / T2 | HR, elevation, cadence selector and legends | GPT-6.1 Sol | Standard | Medium | 75k | Reuse T1 chat if context remains focused |
| 3 / C1 | Charts discovery, data correctness, filters, summary, four-chart overview | GPT-6.1 Sol | Standard | Medium | 165k | Fresh chat with T1/T2 handoff |
| 4 / C2 | Activity drill-down, weekly contributors, Explorer, saved filter state | GPT-6.1 Sol | Standard | Medium | 120k | Prefer fresh chat with C1 handoff |
| 5 / G1 | Review actual usage, remaining allowance, credit balance, and next scope | GPT-6.1 Sol | Standard | Light | Small review | Reuse C2 chat |
| 6 / Y0 | Scheduling architecture and migration discovery | GPT-6.1 Sol | Standard | High | 40k | Fresh chat |
| 7 / Y1 | Season/event persistence, event UI, priorities, compatibility | GPT-6.1 Sol | Standard | High | 160k | Reuse Y0 if discovery is compact; otherwise fresh handoff |
| 8 / Y2 | Coherent yearly generation and A/B/C precedence | GPT-6.1 Sol | Standard | High | 210k | Fresh chat after Y1 |
| 9 / Y3 | Protected workouts, affected ranges, diff, atomic apply, revisions | GPT-6.1 Sol | Standard | High | 200k | Fresh chat after Y2 |

T1-T2-C1-C2 total approximately 480k planning tokens. This includes phase tests and
discovery within T1/C1, but allows little room for unexpected integration work.
Keep a separate contingency of roughly 50k-100k for this first investment.
Y0-Y3 total approximately 610k before unexpected integration work.

These are rough effort allowances, not measured forecasts or credit limits. Earlier
estimates did not separate repeated input, cached input, and generated reasoning.
Consequently, a token goal budget may use a different accounting measure from these
estimates. Do not equate a goal token counter directly with billed total tokens.

## Phase completion gates

| ID | Required evidence before marking complete |
| --- | --- |
| T1 | Timestamped route has smoothed pace colors, matching unit-aware legend, start/finish markers, segment details, and usable fallbacks; paused/noisy/sparse/invalid tracks tested; long route and narrow layout verified |
| T2 | Available metrics recolor existing segments; unavailable metrics explained; legends and units correct; partial sensor data tested |
| C1 | At most four default charts; quick/custom date ranges; sport-appropriate analysis; comparison summary; empty/partial weeks; missingness and coverage visible; unit/date tests and responsive verification pass |
| C2 | Points open the correct activity; weekly bars show contributing activities; Explorer retains the catalog; state survives navigation as designed; interactions verified |
| G1 | Actual usage recorded and next authorized goal scope chosen; do not infer remaining credits from planning estimates |
| Y0 | Exact generation/persistence entry points documented; canonical revision compatibility, event orchestration, protected-workout semantics, migration strategy, and conflict examples resolved |
| Y1 | Seasons/events persist and can be managed; priorities/date validation tested; existing single-event plans remain usable; migrations tested on representative database snapshots |
| Y2 | One-event and realistic multi-event seasons generate coherently; taper/recovery precedence and workload conflicts explained; year-boundary cases pass |
| Y3 | Only permitted future workouts replaced; completed/manual/locked workouts protected; stale previews rejected; failed apply leaves active state unchanged; complete revision history recorded |

At each gate, update the source plan with decisions, run proportionate tests, record
visual evidence or an explicit unverified item, and create a handoff. Do not label a
phase complete if required verification remains unavailable. Create a local phase
commit if the execution prompt authorizes commits; stage only the phase's files.
Pushing, publishing, and applying generated plans to the user's live schedule need
their own authorization.

## Deferred phases

Choose these only after reviewing the initial results and actual usage.

| Phase | Value | Model | Speed | Reasoning | Planning allowance | Session |
| --- | --- | --- | --- | --- | ---: | --- |
| Track 3 | Laps/splits and route selection | GPT-6.1 Sol | Standard | Medium | 90k | Fresh chat |
| Track 4 | Linked charts and map/chart cursors | GPT-6.1 Sol | Standard | High | 130k | Fresh chat; reuse shared chart work |
| Charts 3 | Planned versus completed duration/load and matching | GPT-6.1 Sol | Standard | High | 170k | Fresh chat; use canonical matching |
| Charts 4 | Qualified performance, cadence, power, and durability analytics | GPT-6.1 Sol | Standard | High | 200k | Fresh chat; split into individual metrics if useful |
| Yearly 4 | Conflict/peak refinement, event tuning, optional rollback | GPT-6.1 Sol | Standard | High | 110k | Fresh chat; scope rollback separately |

Best value: T1 first, T2 through reuse of its foundation, then C1 and C2. In Charts 3,
start with weekly duration/load comparison. In Yearly 2/3, start with deterministic
priority and affected-window rules. Advanced prediction and peak optimization stay
deferred until the core workflows have been verified.

## Model adjustments

- Use Medium for implementation, discovery, test design, and normal debugging.
- Temporarily use High when migration design, state preservation, numerical edge
  cases, or persistent failures require deeper analysis. Record the reason.
- Use Light for documentation edits and clearly mechanical work. GPT-6 Luna is an
  optional lower-cost choice for narrowly specified tasks with simple validation.
- Use Extra High only for a specific unresolved problem where High was insufficient.
- Keep Standard speed. Do not automatically enable Fast or change to a more
  expensive model. The user changes client settings at the checkpoint as needed.

## Credits and usage checkpoints

The user reported ChatGPT Plus and 82 credits before implementation. That is a
reported starting balance, not a live balance; planning conversations may consume
usage too. Check the account dashboard at the start and end of each phase.

Included Plus usage is consumed before additional credits. Account limits, reset
times, model rates, and caching affect the result. Do not promise a phase fits the
remaining credits without observing actual consumption.

Record available values, leaving unknown fields blank:

| ID | Status | Start/end date | Model/effort used | Measured tokens and accounting source | Credits before/after | Tests / visual checks | Commit / handoff |
| --- | --- | --- | --- | --- | --- | --- | --- |
| T1 | Locally verified | 2026-10-02 | Client settings unchanged | | | 10 final focused tests passed; desktop/narrow and 20k-point route verified | Uncommitted; handoff below |
| T2 | Locally verified | 2026-10-02 | Client settings unchanged | | | 14 focused tests passed; metric switches, partial data, desktop/narrow verified | Uncommitted; handoff below |
| C1 | Locally verified | 2026-10-02 | Client settings unchanged | | | 60 cross-feature tests passed; 49 final Charts/shared checks; desktop/narrow verified | Uncommitted; handoff below |
| C2 | Pending | | | | | | |
| G1 | Pending | | | | | | |
| Y0 | Deferred | | | | | | |
| Y1 | Deferred | | | | | | |
| Y2 | Deferred | | | | | | |
| Y3 | Deferred | | | | | | |

If account balance is inaccessible, report that fact and let the user provide the
dashboard value. Local project files cannot enforce a hard account credit cap.

## Chat continuity and handoffs

Stay in the same chat while debugging and verifying the current phase. Start a
fresh chat when changing feature area or when a finished phase leaves substantial
irrelevant context. Changing reasoning alone does not require a new chat.

Before switching, append a handoff below with:

- Completed phase and acceptance evidence
- Relevant source files and architectural decisions
- Tests run and results; visual checks and remaining limitations
- Commit hash or exact uncommitted changes
- Actual usage when available
- Next phase, scope, settings, and unresolved choices

The new chat should read this roadmap, the handoff, repository instructions, and
the relevant feature plan before inspecting necessary implementation files.
Avoid multiple chats editing the same working tree concurrently.

## Starting autonomous execution

Goal mode can execute a scoped milestone across turns. In clients supporting it,
use `/goal` and give an outcome plus verification criteria. Keep the workspace
available. Existing permissions and approval requirements still apply.

Recommended first goal (select GPT-6.1 Sol / Standard / Medium first):

```text
/goal Complete milestone T1 in IMPLEMENTATION_ROADMAP.md using
ACTIVITY_TRACK_VISUALS_PLAN.md. Perform discovery, implement the pace-colored route,
run the relevant tests, and verify the desktop/narrow layout and representative
long route. Update both documents with findings and a handoff, and create a local
commit containing only this phase's changes. Preserve unrelated user changes.
Work autonomously through routine decisions. Finish at the T1 gate and report
verification and available usage. Do not advance to T2 in this goal.
```

To run T1 and T2 together, explicitly authorize both milestones in the goal and
require the T1 verification gate before advancing. A single goal can keep related
work in the same chat; recommended fresh chats and model changes require client
actions unless a separately configured orchestration workflow supports them.

A roadmap file alone does not launch work. Goal mode is suited to this one-time
implementation; recurring scheduling is unnecessary. Do not start Yearly Scheduling
or deferred phases merely because they appear in this file.

Official guidance, checked 2026-10-02:

- [Long-running work and Goal mode](https://learn.chatgpt.com/docs/long-running-work)
- [Model selection](https://learn.chatgpt.com/docs/model-selection)
- [Pricing and account usage](https://learn.chatgpt.com/docs/pricing)

## Latest handoff

2026-10-02: Roadmap created. All implementation milestones are pending. The Charts
plan is an existing untracked file from earlier planning and must be preserved.
No feature implementation or goal was started while creating this document.
Next action: select GPT-6.1 Sol / Standard / Medium, open a focused implementation
chat, and start the T1 goal above.

### T1 handoff — 2026-10-02

T1 implemented in `analytics/track_visuals.py`, `db/queries.py`, and the Activities Track tab in `ui_nicegui/pages.py`. Added `tests/test_track_visuals.py` and `tests/test_track_visuals_browser.py`. The source plan records discovery, quality rules, smoothing, unit boundaries, Leaflet initialization behavior, and test details.

Final focused suite: 10 passed. Activity-row navigation also passed in the prior 11-test run using installed Playwright Chromium in place of the test's system-Chrome locator. Desktop and 390px screenshot evidence is under `reports/track_t1/`. Long synthetic track: 20,000 points, 167 display sections; actual popup and permanent endpoint labels verified. External map tiles were blocked for deterministic browser tests; live tile loading and real activity field trial remain unverified. No dark-mode control found. No measured token/account credit information available.

Exact changes remain uncommitted, including this roadmap's T1 update; pre-existing untracked Charts plan preserved. No commit/push/publish or live schedule changes. T2 is pending; do not advance to Charts or Yearly Scheduling until the applicable gates.

### T2 handoff — 2026-10-02

T2 implemented in `analytics/track_visuals.py` and the Activities Track tab. Metric configurations classify pace, HR, elevation, and cadence once. GeoJSON geometry is partitioned on combined metric bands/missingness; selector changes styles in place. Missing metrics are explained, partial sensor edges remain gray, numeric legends show exact unit-aware bands and coverage. Per-activity selection survives tab navigation within the page.

14 focused tests passed, including T1 long-track coverage and browser verification that metric changes preserve Leaflet section IDs. Desktop and 390px evidence inspected under `reports/track_t2/`. Source plan records five-band elevation decision, activity-relative zones, sensor quality semantics, and cadence conventions. Live external tiles and real imported sensor-activity trial remain unverified; usage/credit data unavailable.

No commit, push, publish, or schedule changes. Earlier T1 changes and pre-existing Charts plan preserved. Next authorized scope to request is C1: Charts discovery, correctness, filters, comparison summary, and four-chart overview.

### C1 handoff — 2026-10-02

Charts now opens a four-slot overview with quick/custom inclusive date ranges, single-sport analysis, equal-length previous-period summary, explicit TSS/TRIMP choice, moving-duration fallback labels, measured-only HR zones, and responsive layout. Monday weeks retain inactive periods; missing values remain gaps with coverage; partial weeks are labeled. A weekly table/CSV supplies an accessible alternative.

Architecture: `analytics/chart_overview.py` is pure preparation/figures. `chart_dataframe` uses bounded canonical activity days and query opt-in missing HR zones; other callers preserve legacy default behavior. Page caches raw query frames for presentation changes. Existing catalog builders remain intact for C2. Athlete LTHR/legacy threshold uncertainty is visible.

Evidence: 60 cross-feature tests passed; subsequent final Charts/shared refinements passed 49 tests, then eight pure cases passed after adding strength frequency. Browser checked filter updates, four-chart limit, sport/load/unit handling, invalid dates, and 390px widths. Desktop/narrow screenshots inspected under `reports/charts_c1/`. Synthetic 4,000-activity history preparation passes a five-second bound. Actual usage/credits and real-history field trial remain unavailable.

Uncommitted C1 files are listed in the Charts source-plan handoff; all earlier T1/T2/planning changes preserved. No deployment or schedule changes. C2 is next: activity links, weekly contributors, complete Explorer catalog, and saved filter/section state. Do not proceed beyond C1 without the next task authorization.
