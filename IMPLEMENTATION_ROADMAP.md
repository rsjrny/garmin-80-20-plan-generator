# Training Planner Implementation Roadmap

Created: 2026-10-02
Status: T1, T2, C1, C2, Y1, Y2 and Y3 locally verified; G1 scope choice recorded; Y0 discovery complete.

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
| 8 / Y2 | Coherent yearly generation, canonical adapters, A/B/C precedence | GPT-6.1 Sol | Standard | High | 240k | Fresh chat after Y1 |
| 9 / Y3 | Protected workouts, origin lineage, affected ranges, diff, atomic apply, revisions | GPT-6.1 Sol | Standard | High | 240k | Fresh chat after Y2 |

T1-T2-C1-C2 total approximately 480k planning tokens. This includes phase tests and
discovery within T1/C1, but allows little room for unexpected integration work.
Keep a separate contingency of roughly 50k-100k for this first investment.
Y0-Y3 total approximately 680k before unexpected integration work, revised after
Y0 identified canonical adapter, auxiliary-session and origin-lineage dependencies.

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
| C2 | Locally verified | 2026-10-02 | Client settings unchanged | | | 76 Charts/shared checks passed; two activity checks passed with bundled Chromium; desktop/narrow interactions verified | Uncommitted on e79bad7; handoff below |
| G1 | Complete; user selected Y0 only | 2026-10-02 | Client settings unchanged | Included allowance: 34% five-hour / 40% weekly at prior snapshot; user dashboard | Historical 82 / 82 at prior snapshot | Existing evidence reviewed; scope chosen 2026-10-02 | G1 review below; Y0 handoff below |
| Y0 | Discovery complete | 2026-10-02 | Client settings unchanged | | | 264 existing tests and 5 synthetic discovery probes passed; no UI, visual checks not applicable | Uncommitted on e79bad7; docs/yearly-scheduling-architecture.md and handoff below |
| Y1 | Locally verified | 2026-10-02 | Client settings unchanged | | | 408 scheduling/shared/browser tests passed; desktop/390px UI inspected; representative and real v11 snapshot upgrades preserved schedules | Uncommitted on e79bad7; handoff below |
| Y2 | Locally verified | 2026-10-02 | Client settings unchanged | | | 611 distinct checks; desktop/390px; private v11→v13 migration; full-year timing | Uncommitted; reports/yearly_y2 and handoff below |
| Y3 | Locally verified | 2026-10-02 | Client settings unchanged | | | 646 distinct scheduling/shared/browser checks; desktop/390px; private v11→v14 migration; 366-day regeneration timing | Uncommitted; reports/yearly_y3 and handoff below |

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

## C2 handoff — 2026-10-02

Charts now has activity-ID detail links, weekly/day/sport contributors, keyboard source links, the full eleven-chart Explorer catalog, and saved tab-session filters/section/catalog selection. Explorer shares C1 preparation and missingness. Same-day points resolve via explicit trace identities; trend lines do not open arbitrary activities. Activities accepts validated activity_id links independently of its grid filters.

New analytics/chart_explorer.py owns the catalog, saved-state validation, click identities, source rows, contributors, and selected Explorer figures. ui_nicegui/pages.py integrates tab storage and interaction; db/queries.py adds optional-schema display names without migration. State survives refresh and navigation in the running tab/app; new tabs start with defaults. Quick ranges follow today; custom dates stay fixed.

Evidence: 76 Charts/shared tests passed without warnings; two existing activity-grid checks passed with bundled Chromium (system Chrome unavailable; test-only launcher override, one import-rewrite warning). Browser verified rendered clicks, keyboard links, custom/quick filters, reload/shell/detail navigation, separate tabs, empty/missing contributors, missing detail IDs, reset zoom, and 390px layout. Four screenshots inspected in reports/charts_c2/; whitespace check passed.

C2 changes remain uncommitted on e79bad7; the Charts source plan lists exact files. No deployment/schedule changes. Actual usage/credit values and real-history field trial remain unknown. G1 usage/scope review is next; C3/C4 and yearly scheduling remain deferred.

## G1 review — 2026-10-02

Status: Complete. User selected Y0 only on 2026-10-02; prior dashboard snapshot retained.

### Evidence and current state

- T1/T2 and C1/C2 have passed their local implementation gates. C2's final evidence is 76 Charts/shared tests plus two activity-grid checks, with desktop/narrow screenshots. Counts from earlier runs overlap and are not summed into a project-wide unique-test total.
- Latest committed baseline is e79bad7. T1/T2/C1 are in that baseline; C2 implementation, tests, screenshots, and handoffs remain uncommitted. This review preserves those changes.
- Real imported-activity field trials and live route tiles remain unverified. Those are useful product checks before a larger investment, although the recorded local phase gates passed.
- No active goal exists in this thread, so the goal tool supplies no token-usage report. Phase handoffs also contain no measured tokens. Live account values are not exposed by the available account tools; the user supplied the dashboard snapshot below. Attributable phase spending remains unavailable.
- The previously reported 82 credits are a historical starting value, not current remaining credits. No spend or remaining balance has been inferred from the roadmap's token planning allowances.
- Official OpenAI documentation directs users to the usage dashboard for remaining limits/reset times and distinguishes input, cached-input, and output token rates. The account snapshot is now recorded, but phase credit cost still cannot be calculated reliably without measured category totals and attributable consumption. Source checked 2026-10-02: [Pricing and usage](https://learn.chatgpt.com/docs/pricing).

### Usage record

| Item | Recorded value / source |
| --- | --- |
| Historical plan / starting credits | ChatGPT Plus / 82 credits; earlier user report |
| Current credit balance | 82 credits remaining; user dashboard paste on 2026-10-02 |
| Remaining included allowance | Five-hour: 34% left; weekly: 40% left; user dashboard |
| Reset date/time | Five-hour: in 16 minutes; weekly: in 4 days 23 hours, relative to the user snapshot; exact timestamps unavailable (user timezone America/New_York) |
| Measured T1/T2/C1/C2 tokens | Unavailable in handoffs and current goal tool |
| Phase-attributable credit spending | Unavailable; balance differences may include other work or purchases |
| Real-history user feedback | Not yet recorded |

The current 82-credit balance matches the historical report, so no net credit decline is visible between those snapshots. This does not establish zero usage or a phase-specific cost: included limits were consumed, and intervening activity/purchases are not measured. The dashboard states these plan limits are shared across Codex, Work, Workspace Agents, and ChatGPT for Excel, excluding Chat conversations. Preserve the remaining allowance for a bounded next phase; its fit cannot be guaranteed from these percentages alone.

### Concrete next-scope recommendation

Recommend **Y0 only**, if the user wants to continue toward yearly scheduling and accepts the available capacity. Y0 produces a reviewable architecture/discovery handoff before feature implementation:

1. Map existing generation and persistence entry points.
2. Resolve canonical revision compatibility and multi-event orchestration.
3. Define completed/manual/locked workout preservation semantics.
4. Specify backward-compatible migrations and representative conflict examples.
5. Revise Y1-Y3 scope/effort based on the findings, then stop at the Y0 gate.

Y0's existing 40k planning allowance is an effort estimate, not a measured token budget or credit cap. The roadmap recommends a fresh GPT-6.1 Sol / Standard / High chat for Y0; client settings have not been changed here. Y1-Y3 remain separate decisions.

Alternatives: conduct a real-history field trial of the completed track/Charts workflows first, or choose a narrowly scoped Charts 3 weekly planned-versus-completed comparison if that is more valuable than multi-event seasons.

### Remaining decisions

Dashboard snapshot recorded. The user subsequently requested "perform y0 from the
roadmap" on 2026-10-02, authorizing Y0 only and completing G1's scope-choice gate.
Y1-Y3 and deferred features remain separate decisions. No implementation, commit,
push, deployment, or schedule write was performed during G1.

## Y0 handoff — 2026-10-02

Scheduling architecture/discovery gate complete. Exact generation, legacy save,
canonical approval, conversion, matching and calendar/cache entry points are in
[docs/yearly-scheduling-architecture.md](docs/yearly-scheduling-architecture.md).
The Yearly source plan links the selected design and updated scope.

Selected architecture: editable season/event intent over existing immutable
training_plan/plan_revision storage, one active pointer, pure season orchestration,
complete merged revisions and origin lineage for carried prescriptions/completion.
Operational protection and match state require stale checks beyond today's
active-plan hash. Additive migrations preserve existing single-event plans and
require explicit adoption; legacy conversion cannot select only part of its
rowset. All old overlapping writers need guards. Auxiliary strength/rest typing
and calendar/nutrition compatibility are explicit Y2 dependencies.

Verification: 264 focused existing baseline/calendar/persistence/migration/revision/
matching/conversion/V2-boundary/policy tests passed in 16.22s. Five reproducible
synthetic probes passed, confirming completion-hash exclusion, origin-match needs,
full projection rebuild, legacy projection deletion bypass and missing protection
flags/idempotent replay. Evidence: reports/yearly_y0/pytest.txt, discovery_probe.py
and discovery_probe.txt. No season UI added, so visual checks are not applicable.
Actual user snapshot migration and season behavior/performance remain Y1-Y3 gates.

Y0 changes: this roadmap, YEARLY_MULTI_EVENT_SCHEDULING_PLAN.md,
docs/yearly-scheduling-architecture.md and reports/yearly_y0/. Uncommitted on
e79bad7; pre-existing C2 changes preserved. No live DB/apply/commit/push/deployment.
No attributable phase token/credit data available; prior account snapshot is not
a refreshed balance. Client settings unchanged.

Y1 allowance stays 160k; Y2 rises to 240k and Y3 to 240k for discovered canonical
adapter/auxiliary/lineage/concurrency scope. Revised Y0-Y3 total approximately 680k
before contingency; estimates do not promise credit fit. Y2 initial apply is for
an empty future season only; existing-plan regeneration apply waits for Y3.
Next task is separately authorized Y1; stop at this Y0 gate.

## Y1 handoff — 2026-10-02

The user authorized Y1 with "continue to y1". The multiple-event foundation is
implemented and locally verified. `/seasons` is available from navigation and Plan.
It stores 1–366-day running seasons, athlete/availability inputs, and chronological
road/trail events with A/B/C priority, goals, optional time/pace targets, course
notes and taper/recovery overrides. Editors reject invalid dates, duplicate active
event dates, completed-history changes and stale concurrent saves. Seasons can be
archived/restored; linked event removal retains cancellation history.

Schema v12 adds empty season tables with no inferred events or automatic conversion.
Explicit linking accepts reviewed native or previously converted canonical plans,
checks current revision/hash and full projection consistency, and retains all
prescriptions and confirmed matches. Season method/timezone and prior workout
coverage remain fixed after linking. Overlapping unarchived season intent is rejected;
archiving a linked season retains its schedule ownership. All legacy writers and
direct canonical approvals, including a different plan targeting owned dates, are
guarded inside their write transactions. These guards moved from Y2 to Y1 because
linking establishes ownership now. Unlinked intent preserves single-event workflows.

Verification: 182 season/migration/revision/matching/conversion/baseline/persistence
tests, 225 shared policy/calendar/AI/UI checks, and one Chromium workflow passed
(408 total). Browser verification covered creation, targets, duplicate validation,
reload, two-editor concurrency, explicit linking, cancellation, archive/restore,
Plan navigation, unchanged active schedule hashes, and no browser/server errors.
Desktop 1440px, 390px layout and scrolling editor controls were visually inspected.
Evidence and reproduction details are in [reports/yearly_y1](reports/yearly_y1/).

Fresh, legacy, native and converted synthetic v11 snapshots upgrade/replay without
schedule mutation; injected migration failure rolls back tables and version together.
A private SQLite backup of the installed v11 database also upgraded to v12 twice:
every checked plan/match/settings row, active schedule and foreign-key state remained unchanged, and
both new tables were empty. The source database was never migrated or modified;
the private full backup was removed. Startup migration occurs when the updated app
is next opened. Packaged executable rebuilding and real-user field trial were not
performed; source UI and migration gates passed.

Y1 source changes: db/schema.sql, db/migrate.py, services/season_plans.py,
ui_nicegui/seasons.py, app.py, layout.py and the Plan link in pages.py; guards in
services/plan_persistence.py, db/queries.py, plan_methodology/revision_repository.py,
and the legacy export save delegate. Added three season test modules and updated
latest-version assertions in existing revision/matching/conversion tests. README,
the Yearly source plan, architecture notes and this roadmap document the behavior.
Changes remain uncommitted on e79bad7; existing C2 and Y0 changes were preserved.
No commit, push, deployment, live schedule apply or Y2 generation was performed.
Phase token/credit usage is unavailable; client settings were unchanged.

Next separately authorized phase: Y2, coherent yearly generation, canonical adapters,
auxiliary-session typing, A/B/C phase precedence, workload conflicts and preview/apply
for an empty future season. Linked/existing schedule regeneration still requires
Y3 preservation and origin lineage. Stop at the completed Y1 gate.


## Y2 implementation and handoff — 2026-10-02

Y2 is locally verified. **Preview yearly schedule** generates one continuous
1–366-day canonical running schedule: base/build/event-specific/taper/event/recovery/
maintenance phases, local Monday weeks, partial-week labels and continuous cutbacks.
Events replace a session and consume weekly run/hard-day capacity. Recovery constrains
all priorities; intersecting recovery/taper uses the lower load. Close A peaks,
participation in recovery, unavailable dates and incompatible B/C taper participation
block apply with explanations. Unsupported distances have no fallback.

Generation reuses weekday selection, then composes canonical workouts directly,
without concatenating calendars or calling legacy saves. 80/20 requires confirmed
measured running LTHR; Maffetone requires an explicitly selected/confirmed adjustment.
Named-method policy validates the candidate. Target time/speed remains event intent;
event duration is a target estimate or unknown. No running distance/TSS is invented.
These prescriptions do not promise readiness or event goal achievement.

Starting duration uses the mean of four explicitly covered completed local weeks
within eight weeks before preview, otherwise an explicit starter load is required
with a readiness warning. CompletedWeek history can be supplied to the service;
the current editor uses the starter setting. Full normal-week growth is conservatively
5%, validated against 10%; partial/cutback weeks do not raise the reference. Recovery
and subsequent maintenance return at the lower of starter and prior normal load.
Complete distance is checked for growth; missing distance/TSS remains visible.

STRENGTH/MOBILITY/REST have validated sport/family/flag/segment pairs. Rest uses OPEN
NON_TRAINING with no duration. Auxiliary sessions are excluded from running
prescription/distribution validation and runtime compliance/aggregation. Event goals
and the V2 AI parser stay running-only. Calendar rows expose native targets or
auxiliary/rest labels. Initial apply invalidates affected cached nutrition while
preserving unaffected dates and double-encoded legacy caches.

Preview is read-only, showing phases, weekly/event load, sessions, warnings/conflicts
and apply restrictions. Apply requires explicit warning acknowledgement and an empty,
unarchived season starting today or later. Linked/adopted full previews keep the
parent parameters and cannot replace workouts. Unmanaged/partial-provenance overlap
and known neighboring recovery conflicts block initial apply. Old writer guards remain.

Under one BEGIN IMMEDIATE, apply recomputes content and checks intent snapshot/version,
active/raw schedule state, match/completion state, local today/timezone and versions.
Ownership, immutable full revision graph, projection/pointer, affected nutrition and
immutable v13 season_revision_application audit commit together. Identical retries
are no-ops. Audit stores input/settings, versions, exact added IDs, empty initial
preservation/override sets, warnings, rationale, reviewer/time and state identities.
Applied history is visible on Seasons. Schema v13 is additive; existing hashes and
schedules are retained.

Verification: 607 broad checks passed. The final focused run passed 295 checks and
hit one transient Windows ConnectionResetError in the existing browser teardown
log assertion; all interaction assertions had passed. A clean rerun passed all three
season browser workflows. Across combined scope, 611 distinct tests are verified
(608 non-browser plus three browsers), including the additional auxiliary runtime
case. Repeated runs are not added. Desktop/390px preview/load/session/conflict/apply/
history/calendar captures were inspected. Ten injected failure stages roll back;
stale input/completion/date/raw-state/forgery and concurrent/idempotent apply pass.

A private read-only backup of the installed v11 database upgraded/replayed to v13
without changing prior app rows, active schedule or foreign-key state. The backup
was removed; the source was never migrated. Synthetic 366-day/three-event previews
took 0.021–0.030 seconds and atomic apply 0.558–0.637 seconds locally, not a general
performance promise. [Y2 evidence](reports/yearly_y2/README.md) records commands,
logs, screenshots and limitations.

Source: new services/season_schedule.py and services/season_generation.py; canonical
auxiliary domain/segments/goals/policies/runtime accounting; private owner-aware
revision approval; v13 DDL/migration; season UI, calendar reader, policy normalization
and ownership messages. Two new test modules plus schema/migration fixtures cover
Y2. Prior C2/Y0/Y1 work remains. No commit, push, deployment, packaging rebuild or live
schedule apply. Token/account-credit attribution is unavailable; client settings
were unchanged.

Next separately authorized phase: Y3 protection state, immutable origins, affected
ranges, merged revisions and safe regeneration of existing seasons.

## Y3 implementation and handoff — 2026-10-02

Y3 implements reviewed regeneration of existing and explicitly adopted seasons.
Calculated affected ranges envelope old/new event influence, add seven transition
days, expand to Monday weeks and intersecting neighbors to a fixed point, then
clip to the season/local today. Availability, athlete, fitness-history, bounds or
explicit prescribing refresh changes recommend the full future season. From-today
and custom ranges are selectable; full merged event/load/boundary validation blocks
infeasible narrow ranges without silently enlarging the applied range.

History, origin-confirmed/explicit completion, and unresolved candidate matches
remain protected. Locked/manual future occurrences require exact per-preview
opt-in IDs. Fixed dates reserve capacity, including rest; fixed running/hard/strength
load consumes weekly capacity before generation. Unknown protected running duration
and incompatible taper/recovery sessions block replacement. Cancelled protected
races are retained with a warning until an eligible explicit override is reviewed.

Unchanged/carried workouts retain their IDs and complete prescriptions except
contiguous ordinal position. Changes get new identities with exact replacement
diffs. Immutable flattened origins reference the first prescribing revision and
are included in hashed revision content. Load verifies relational origins, original
content, same-plan ancestor identity and flattened lineage. Activity matching routes
carried identities to their origins; confirmed evidence and original parameter
interpretation stay intact. Explicit confirmed parameter refresh affects new
prescriptions only. Unsupported adopted prescriptions continue to block generation;
confirmed refresh can introduce new running HR prescriptions alongside supported
preserved pace prescriptions.

Workout protection provides optimistic-version lock/completion controls and timed
prescription previews. Manual edits use the same apply transaction and mark their
new occurrence manual; the service also supports reviewed canonical manual creation.
The editor supports timed sessions, retaining segment proportions/native targets;
rest/event changes use season regeneration. Manual creation has a service API, no
separate UI. Completion/manual provenance cannot be cleared. Removed state stays
retained. The active calendar displays generated/adopted, locked, manual, completed
and preserved status; revision history shows recorded preservation and overrides.

Apply rechecks intent, all active/raw schedules, origins/protection/matches, parent
hash, policy/generator, local date/timezone and override/range decisions under one
BEGIN IMMEDIATE. It deterministically recomposes the reviewed candidate. Complete
revision graph, projection/pointer, origins, new manual state, affected nutrition,
season version and immutable audit commit together. Identical retry is a no-op;
other stale previews reject. Failure injection covers all these transaction stages.

Schema v14 adds empty protection/origin tables and expands the v13 audit mode CHECK
through a row-preserving table rebuild. Current-version additive repairs also use a
savepoint so failure leaves no partial repair. No prior revision document/hash is
rewritten. Representative migrations, native/converted adoption and a private real
v11 backup upgrade/replay retain source rows, hashes, matches, active schedule and
foreign-key state. The private backup was removed; the source was not migrated.

Verification and exact commands are in [Y3 evidence](reports/yearly_y3/README.md).
646 distinct checks are verified across final combined scope (642 non-browser,
four browser workflows). Desktop/390px diff, settings, manual preview, apply/history
and calendar status were inspected. Raw browser logs retain a known Python/Windows
Proactor socket-close WinError 10054; tests narrowly classify that stdlib callback
while still rejecting application tracebacks/browser errors. A focused test ensures
application frames and other transport errors remain failures. Prior Y1/Y2 visual
artifacts were restored; compatibility captures live under yearly_y3.

Synthetic 366-day/three-event regeneration preview took 0.404–0.453s, atomic apply
1.275–1.463s, preserving 259 occurrences; these are local measurements. No live
schedule apply, commit, push, deployment or packaged executable rebuild occurred.
Client settings were unchanged; token/account-credit attribution is unavailable.

The final combined suite passed 639 checks in 140.64s; the final calendar/browser
and offline Plan UI run passed four checks, including three additional distinct
Plan UI checks. git diff --check passed.

Source: new season_regeneration.py, season_workouts.py and workout_origin.py;
season_generation.py, season_schedule.py, canonical repository/matching, v14 DDL/
migration and Seasons/calendar readers. Tests add regeneration/browser/log coverage
and update older schema and Y2 UI expectations. README, source plan and architecture
record the workflow. Stop at Y3; deferred Yearly 4 refinement/rollback, mixed methods,
AI composition, automatic fitness-coverage collection, packaging and a live field
trial need separate scope.
