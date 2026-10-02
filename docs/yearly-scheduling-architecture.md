# Y0: Yearly scheduling architecture and migration discovery

Date: 2026-10-02 (America/New_York)
Status: Y0 discovery complete; Y1/Y2 locally verified; Y3 preservation/lineage pending.

Scope: Repository discovery and isolated verification. User authorized Y0 only.
No production schema, UI, generator, or live schedule changes.

## 1. Selected architecture

Add a season orchestration service above the current workout/methodology code.
Keep `training_plan` / `plan_revision` as the authoritative generated schedule;
do not create a competing season revision store. Editable seasons/events express
planning intent. An applied schedule is one immutable, complete revision even
when regeneration changes only a future window.

The single-event calendar calculates one taper and progression, stops at its
event, and has no post-event recovery. Overlaying independently generated plans
would reset progression and conflict with recovery. Reuse weekday templates and
the workout catalog through an explicit phase/load adapter; the season service
owns the complete timeline. Never call a save operation per event.

Initial scope: running road/trail events at supported distances, one running
methodology per plan, up to 366 inclusive calendar days. Auxiliary strength,
mobility and rest need explicit canonical support in Y2. Other endurance sports,
mixed methodologies, peak optimization and AI season composition are deferred.

## 2. Exact current entry points

Paths are relative to `src/garmin_data_hub/`; function names are searchable anchors.
Inspected baseline e79bad7 and pre-existing C2 working changes, which Y0 preserves.

| Entry | Current behavior | Integration decision |
| --- | --- | --- |
| `ui_nicegui/pages.py::generate_reserved_baseline` (nested Plan handler) | One event/start/distance, athlete values, run days, long-run weekday, philosophy; range acknowledgement then build_and_save_baseline | Retain single-event UX; reject overlapping season writes and direct to season preview |
| `services/baseline_plan_builder.py::BaselinePlanRequest`, `build_and_save_baseline` | Single-event validation, maximum 730-day date span; generates, validates policy, prepares optional workbook, persists | Separate pure generation from save; season cap is independently 366 inclusive days |
| `exports/master_export.py::generate_plan_data` | Legacy AthleteProfile/EventProfile/Inputs; analysis, DayPlan list and weekly rows | These DTOs are not canonical revision values |
| `exports/forever/calendar_builder.py::build_calendar`, `_get_phase` | One race anchor; weekday/age/distance rules and four-week cutbacks; race-relative phases/progression | Reuse templates, not independent calendars; orchestration adds continuous progression and recovery |
| `services/plan_persistence.py::save_generated_plan` | BEGIN IMMEDIATE; optional active hash; deletes ALL base-table rows in min/max dates, inserts legacy rows and double-encoded last_generated_plan cache | Guard before mutation; never use for season apply |
| `ui_nicegui/workspace.py::review_proposal`, `save_review_to_database` | Strict single-event import, locked context, local policy, date-grouped diff; apply calls save_imported_plan | Reuse presentation ideas; season diff needs workout identity/preservation reasons |
| `services/coaching_packet.py::build_coaching_packet` | One event and replacement window; existing response requires a race at window end | Keep contract unchanged; not a season API |
| `services/codex_plan_generator.py::generate_plan_with_codex` | Schema-constrained response for current importer, no DB save | Optional AI composition deferred from deterministic Y2 |
| `services/ai_plan_import.py::parse_chatgpt_plan` | ImportedTrainingPlan, replacement range, request ID/hash and exact metrics | Legacy single-event adapter only |
| `services/plan_persistence.py::save_imported_plan` | Required stale hash; duplicate no-op; atomic broad range delete, rows, merged cache and plan_import_history | Reuse transaction concepts, not destructive SQL or history as season authority |
| `exports/forever/build_daily_plan.py::build_and_store_plan`, local `save_generated_plan` | Secondary older writer; settings/range helpers commit separately | Inventory and guard this entry too |
| `db/queries.py::delete_planned_workouts_in_range`, `insert_planned_workout` | Broad base-table mutation, per-call commit, catches errors | Ineligible for season transactions; ownership rejection must propagate |
| `plan_methodology/proposal_validation.py::build_candidate_from_ai_proposal` | Canonical V2 composition creates CANDIDATE; parser is running-only with bounded segments/templates | Reuse DTOs/layered validation; season assembly is application-owned |
| `plan_methodology/revision_repository.py::approve_revision`, `_approve_revision_on_connection` | VALIDATED candidate, content hash, parent ID/hash; atomic graph/projection/pointer | Extend connection-level boundary for season checks, lineage/state/audit; never nest path-owned transactions |
| `plan_methodology/revision_repository.py::_replace_projection` | Rebuilds entire projection for source plan; leaves unrelated plans/legacy rows | Complete candidate must contain history/outside-window sessions; projection row IDs are not stable identities |
| `plan_methodology/legacy.py::resolve_legacy_plan`, `convert` | One unconverted flat rowset across ALL dates; explicit named conversion; source retained/mapped/hidden, no historical conformance claim | Explicit adoption only; not a date-subset conversion or automatic migration |
| `plan_methodology/activity_workout_match.py::create_match`, `load_confirmed_match` | Revision/workout/activity identity; confirmed rows immutable, globally one confirmed match per activity | Resolve completion through origin lineage; never duplicate/retarget confirmed matches |
| `plan_methodology/runtime_compliance.py::evaluate_confirmed_match` | Uses original matched workout and revision parameters | Continue evaluating original match; resolve origin first |
| `db/schema.sql::active_planned_workout` | Active canonical projections plus unmapped legacy rows; hides converted source/inactive projection | Retain view; it neither chooses a global active plan nor prevents date collisions |
| `ui_nicegui/data.py::plan_rows`, dashboard/calendar readers | Active view; plan_rows extracts legacy sport/phase/intensity from structure JSON | Add canonical/auxiliary-aware presentation adapter |
| `ui_nicegui/data.py::nutrition_rows` | Reads last_generated_plan nutrition independently of revisions | Invalidate stale affected nutrition on season apply; macro generation deferred |
| `db/migrate.py::apply_schema` | v11; v7+ migration/version savepoints, additive repair and baseline replay | Add next migration, baseline DDL, repair and rollback tests together |

## 3. Canonical revision compatibility

One season references one existing training_plan. The sole active pointer remains
training_plan.current_revision_id, exposed by join rather than duplicated in the
season. Multiple non-overlapping seasons may coexist. Overlap with another active
season is rejected. Overlapping unmanaged plans require explicit adoption or
removal; never silently delete them. Past overlap remains history and a warning.

Reuse PlanRevisionCandidate, PlannedWorkout, ordered WorkoutSegment, canonical
JSON and SHA-256. Store a versioned all-event snapshot/dates in goal_snapshot;
season input identity and availability in constraints; generator/policy versions
in provenance; exact diff/lineage/preservation in change_summary. Methodology
parameters stay in parameter_snapshot: Maffetone fields are allowlisted, so no
season/event fields there. Retain existing sanitized athlete facts and exclusions
of raw questionnaires, credentials and prompts.

Use existing revision reasons: INITIAL_GENERATION, GOAL_CHANGE for event edits,
ATHLETE_STATE_CHANGE, PRESCRIPTION_EDIT and REGENERATION. Event CRUD updates intent
and marks the schedule as needing regeneration; it does not change the active
revision until apply.

The new candidate contains the complete timeline. Unchanged occurrences keep
workout ID, date, segments, prescriptions, title, metadata and flags. Only the
contiguous ordinal may change when earlier sessions are added/removed; preservation
comparison excludes this position and the diff records old/new ordinals. Changed
or moved occurrences get new IDs with a replaces-workout reference. Allocate IDs
before preview, not solely from date. Reapplying the same approved preview is a
no-op through its application identity.

### Origin lineage and parameter interpretation

Add immutable plan_revision_workout_origin rows from each copied revision/workout
to its original revision/workout, both composite foreign keys. Enforce same plan
and exact prescribed content equality excluding ordinal inside apply. Flatten to
the first prescribing revision; no cycles/future origins. Include the same mapping
in hashed change_summary and verify relational rows against that document on load.
New/changed workouts are their own origin and require no mapping row.

Confirmed matches remain on the original revision: global activity uniqueness
prevents duplicating them for carried workouts. Completion and compliance resolve
the origin, including when a new match is recorded for a carried occurrence.
Season matching routes that write to the origin identity. Copied prescriptions likewise retain their original threshold/MAF
parameter references and use the origin revision's snapshot; newly generated
prescriptions use the candidate's snapshot. Never reinterpret old targets against
current athlete settings.

Y2 keeps the parent's methodology/parameter snapshot fixed on subsequent full
generation previews. Y2 apply is limited to an empty season with start >= today,
no adopted active revision and no overlapping unmanaged workouts. Existing/adopted
plans remain readable; regenerated previews cannot be applied until Y3 protection
and lineage are implemented. Y3 adds origin-aware parameter refresh before allowing prescribing
parameter changes. Current persistence enforces one methodology per candidate;
switching methodology while preserving incompatible history is rejected in v1,
with guidance to use a separately reviewed future plan. Mixed-method storage is
deferred. Validation evaluates carried workouts under origin snapshots and new
workouts under candidate parameters, including combined weekly accounting. A
VALIDATED flag alone is not a substitute for these checks.

### Auxiliary sessions

Sport currently has only RUNNING; the V2 parser rejects STRENGTH/MOBILITY/REST
families despite their presence in the broader vocabulary. Y2 adds explicit
auxiliary sport values and an application-owned adapter. Event/GoalSnapshot sport
remains RUNNING. Auxiliary segments have no endurance prescriptions and are
excluded from native running distribution/compliance. Rest uses an explicit
non-training OPEN segment, never fabricated duration. Add a NON_TRAINING segment
kind rather than labelling rest FREE_RUN. Validate auxiliary family/sport pairs
and prohibit endurance prescriptions on them. Update policy normalization
and projection together; verify they are not run days/endurance minutes. Keep
existing V2 AI parsing running-only; do not weaken that contract.

## 4. Protected-workout semantics

Add season_workout_state keyed by (plan_id, workout_id), not planned_workout_id:
locked, manually_edited, manually_created, explicitly_completed, version and
editor/reason/time. Operational state is outside immutable prescriptions. A
prescription edit creates a revision and marks its resulting occurrence manual.
Spelling-only display edits can use the existing metadata-overlay distinction.

| Condition | Decision / replacement control |
| --- | --- |
| Date before today in saved season timezone | Preserve; historical rewrites excluded from Y1-Y3 |
| Confirmed activity at origin or explicit completion | Preserve, including today/future dates; regeneration cannot replace |
| Future locked or manual edit/creation | Preserve; only explicit per-preview opt-in for identified future occurrences permits replacement |
| Candidate/unconfirmed activity match | Provisional protection and warning; resolve/reject evidence, then make a fresh preview |
| Unknown/partial provenance or unsupported preserved prescription | Block adoption/apply; never infer from title or same-day activity |
| Outside permitted replacement window | Preserve; custom range also respects history/completion |
| Ordinary generated incomplete future session inside range | Eligible; diff names exact occurrence |

Locked/manual rest reserves its date. Preserved active sessions consume
availability, hard-day and load capacity before generation. Do not retain an old
session and add its replacement. If fixed workouts prevent a valid plan, block
and explain alternatives. Cancelling a future event does not silently remove a
locked/manual event workout; require the specific future override.

Save an IANA timezone, default from the user's configured timezone, never the
execution host. Preview captures local today/cutover; a changed local date
invalidates apply. From-today includes only eligible sessions today. New completion
evidence invalidates preview even when the schedule hash is unchanged.

## 5. Event orchestration and conflicts

Implement a pure services/season_schedule.py pipeline:

1. Freeze events sorted by date/ID, availability/constraints, fitness evidence,
   methodology/parameters and operational state.
2. Build planned-event influence windows. Completed events provide recorded-date
   recovery without a new race. Skipped/cancelled events provide no new phases;
   their OLD windows still matter to change calculation. Reversing completion
   requires reviewed source correction.
3. Assign one controlling phase/load budget per date/week. Recovery constrains
   all priorities; A preparation/taper outranks B/C preparation. Intersecting
   taper/recovery uses the lower load. Events replace a session, not add a key day.
4. Populate continuous progression and cutbacks from templates, accounting for
   preserved load/availability first. Gaps use maintenance/transition. Zero planned
   events after cancellation is a valid maintenance season.
5. Validate the full combined timeline and reconnection boundaries, then build
   candidate/diff/warnings. No persistence in generation.

### Initial versioned window policy

These are proposed product scheduling parameters for deterministic tests, not
individualized readiness guarantees. Y2 stores effective values/policy version
and validates resulting load. Unsupported distance/terrain needs an explicit
supported configuration, never silent fallback to 50K.

| Distance class | A preparation P (days) | A taper T | Recovery R (all priorities) |
| --- | ---: | ---: | ---: |
| 5K | 42 | 7 | 2 |
| 10K / 10M | 56 | 7 | 3 |
| Half marathon | 84 | 10 | 7 |
| 20M / Marathon | 112 | 14 | 14 |
| 50K / 50M | 140 | 14 | 21 |
| 100K / 100M | 168 | 21 | 28 |

B uses ceil(P/2) and min(T,3); C has no separate preparation/taper. Recovery is
distance-based regardless of priority. For event D, inclusive intervals are
preparation [D-T-P,D-T-1], taper [D-T,D-1], event [D,D], recovery [D+1,D+R]. Clip
output to season bounds, keeping overflow warnings/boundary context. Override
inputs are bounded integers: taper 0-28 days; recovery default R through 42 days.
Shortening default recovery is rejected in v1; C overrides cannot defeat A windows.

Same-day active events are blocking ambiguity regardless of priority. Two A events
whose earlier recovery intersects later taper cannot claim separate peaks: block
until one is downgraded/moved/cancelled. Other A preparation overlaps produce an
explicit shared-build warning; earliest A controls until recovery ends, then next.
Tie order is date then ID. Never silently change stored priority or attendance.
B/C participation inside recovery blocks. A B/C event inside A taper may replace
a compatible easy session only if its distance/load fits that day's budget;
otherwise block and suggest moving/skipping. Insufficient preparation warns and
reduces the build; never compress missed training into remaining weeks. Hard load
violations still block apply.

### Workload validation

Reuse meanings of existing training_policy caps, not its single-event signature:
it demands a race at its end date and weeks are relative to plan start. Season
validation uses local Monday-Sunday weeks, labels partial weeks, checks EACH
planned event and includes adjacent days/weeks outside the replacement range.
Preserve maximum 3 sessions/day, one hard/race/day, configured run days,
age/method hard-session caps, strength cap, non-race 40-hour/1500-TSS weekly caps
and named-method acceptance. Coarse maxima do not establish individual suitability.

Check current 10% build-distance growth only with complete known distance; add
10% build-duration growth with complete known running duration. Missing values
are not zero; no invented TSS. Following cutback/taper/recovery compare with the
last complete normal build week and a conservative return envelope frozen from
recovery/fitness assumptions. Without a baseline, warn progression is unverified
and use explicitly recorded starter load supported by consumed history or an
explicit conservative starting configuration. Show event load separately; it
still counts for hard-day/recovery conflicts. Include protected and known completed
load at boundaries. Policy errors block; preparation/coverage warnings require
visible acknowledgement.

The initial return envelope is deterministic: the first full week after recovery
is capped at the lower of the last complete normal build week's known duration
and the frozen current starting-duration baseline. Later complete normal weeks
increase by at most 10%; cutbacks/partial weeks do not raise the envelope. Where
both duration and distance are fully known, apply both caps. The starting baseline
is the mean of the last four fully covered completed local weeks within the prior
eight weeks. Fewer than four marks reduced coverage and requires an explicit
starting-duration setting. No usable weeks requires that setting and an
unverified-readiness warning. Do not infer missing load or a zero baseline.

### Representative acceptance examples

| Case | Required result |
| --- | --- |
| One A half marathon 2027-05-16, season 2027-01-01..12-31 | One event, 10-day taper, 7-day recovery, then maintenance/build; no fabricated season-end race |
| A half marathon 2027-05-16 and A marathon 2027-10-17 | Continuous load, distinct phases; no concatenated progression reset |
| A marathons 2027-10-17 and 10-24 | Recovery 10-18..10-31 intersects later taper 10-10..10-23: block separate peaks |
| B half marathon 2027-10-10 before A marathon 10-17 | Exceeds compatible A taper budget: block; do not add another race/key workout |
| C 5K 2027-09-05 during A build | Replace suitable quality day, reserve 2-day recovery, recheck hard cap |
| C 10K 2027-10-20 after A marathon 10-17 | Participation blocked by recovery regardless of C priority |
| Marathon 2026-10-18 inserted with today 2026-10-02 | Preparation warning; conservative output only if load/recovery valid; no 112-day build compressed into 16 days |
| Event moves 2027-05-16 to 06-06 | Union old/new windows; old generated race removed only if eligible |
| Event cancelled after an earlier completed event | Completed history/recovery retained, maintenance fills allowed future gap |
| Event 2027-01-03 in season 2026-10-02..2027-10-01 | Taper crosses Dec/Jan; actual dates drive weeks/year/leap logic |
| Manual hard run in taper and locked rest today | Preserve and reserve capacity; block infeasible taper |
| Match/lock added after preview; exception during apply | Reject stale preview / roll back every component |

## 6. Affected ranges and atomic apply (Y3)

Envelope OLD and NEW event windows, add seven days either side for transition,
expand to Monday weeks, then repeatedly include every intersecting neighbor's
full influence/reconnection window to a fixed point bounded by season length.
Clamp eligible replacement to [today,season end], retaining earlier validation
context and reasons for clipping/expansion. Deletion/priority changes use old
inputs. Availability/prescribing input changes usually need full-from-today
evaluation. If local reconciliation fails, preview that alternative; never enlarge
the applied range without showing it.

Custom ranges narrower than recommended must pass full merged event/boundary/load
validation or block with an expanded-range suggestion. Ending inside recovery
shows unrepresented tail and rejects known following-season conflicts.

Preview envelope captures candidate hash, parent ID/hash, season input version/hash,
ALL active-plan schedule hash, relevant protection/match-state hash, policy/generator
versions, local today/timezone, range/mode and explicit override IDs. Existing
active_plan_sha256 alone is insufficient. Runtime state stays outside revision
content; reviewed preservation decisions enter the immutable audit. Preview is
read-only.

Apply owns ONE connection-level BEGIN IMMEDIATE transaction:

1. Recheck every identity, local today, ownership and override under lock; verify
   parent immutable content on load.
2. Recheck merge/protection/validation against current operational state.
3. Use extended connection-level revision approval for graph and full projection;
   server validates lineage/preserved content, not a client VALIDATED flag alone.
4. Insert immutable origins and season_revision_application: exact input snapshot,
   old/new revision hashes, range/mode, event delta, added/removed/changed/preserved
   IDs/reasons, overrides, warnings/rationale, versions, approver and timestamp.
   Prior/resulting full state remains in referenced immutable revisions.
5. Update new/manual operational state, invalidate affected stale nutrition and
   activate the sole revision pointer; commit once.

Failures roll back graph, projection, origin, state, audit, cache and pointer.
Already applied preview identity gives a no-op; different current parent requires
new preview. Do not use committing query helpers or path-owned approve_revision
inside the outer transaction. Full projection rebuild is permitted only with
unchanged preserved prescriptions/logical IDs/evidence and unchanged other plans.

## 7. Additive migration and adoption

Current schema is v11; allocate next available versions at implementation (v12
currently next). Y0 does not apply new DDL.

| Planned table / stage | Fields and constraints |
| --- | --- |
| season_plan, Y1/v12 | season_id PK; nullable UNIQUE plan_id FK training_plan, reserved/adopted once; name/start/end/timezone; draft/active/archived; sanitized versioned input JSON; input_version and timestamps. SQL checks order/status/version; service validates real ISO dates, timezone and <=366 inclusive days |
| season_event, Y1/v12 | event_id PK; season FK RESTRICT; name/date/RUNNING type/distance metres and supported code; priority CHECK A/B/C; status CHECK planned/completed/skipped/cancelled; goal; exact target seconds OR speed string; terrain/course; nullable taper/recovery overrides; timestamps. Service checks in-season date, units/exclusivity, code/distance and duplicate active dates transactionally |
| season_revision_application, Y2 initial apply, Y3 enrichment | application_id PK; season/resulting revision FK; previous revision nullable FK; immutable input/event snapshot/hash/version; range/mode; versions, diff/preservation/override/warning/rationale documents, reviewer/time; UNIQUE applied preview identity per season; validate same-plan ownership |
| season_workout_state, Y3 | plan/workout PK; season owner, origin revision/workout FK; explicit protection booleans, monotonic version, editor/reason/time; retain removed historical state |
| plan_revision_workout_origin, Y3 | copied revision/workout PK/FK and origin revision/workout FK; copied content hash; immutable; enforce same-plan, acyclic exact carry-forward |

Event CRUD increments parent input_version in the same transaction with expected
version concurrency checks. Applied-event removal means cancellation, preserving
prior snapshots. Hard deletion only for never-applied drafts. Do not shrink season
bounds past applied history or silently move completed events. Archive applied
seasons; no cascading deletion of history.

New/upgrade DBs use shared packaged additive DDL, existing v7+ savepoint/version
pattern, statement execution without executescript inside savepoint, replay and
repair. Inject failure to verify both DDL/version rollback. Do not rewrite old
revision documents/hashes. Default migration creates empty season tables without
guessing events/priorities from cache, converting plans or changing workouts.

Native adoption explicitly links a reviewed existing plan without regeneration.
Legacy adoption can use existing explicit conversion only when the ENTIRE resolved
rowset is intended; partial-rowset adoption is unsupported in v1. Unsupported
legacy/mixed prescriptions block conversion rather than invent conformance.
Keeping the old plan and starting a non-overlapping season is supported. Mapped
legacy source rows remain immutable/hidden after subsequent revisions.

Before Y2 exposes apply, guard ALL old writers in section 2 against owned-season
overlap within their transaction; insert guards cover season dates even without
current projection rows. Rejections propagate. Old single-event imports outside
season coverage retain behavior.

Canonical approve_revision must also reject direct writes to a season-owned plan
unless invoked through the season transaction service. Otherwise generic approval
could bypass protection/audit despite passing parent hash checks. Season manual
prescription edits use that same transaction service; direct DB helper callers do
not become an alternate approval route.

Required migration snapshots for Y1-Y3:

- Fresh DB with upstream activity table; migrate twice.
- Pre-v9 legacy rows/settings/import history, including double-encoded cache.
- v9/v10 canonical revisions/confirmed matches without hash drift.
- v11 converted source rows/mappings/triggers plus unrelated native/legacy plans;
  no duplicate active rows or source loss.
- Empty settings, partial provenance (explicit corruption), initialization without
  upstream tables, current-version missing additive objects and midway failure.

Use temporary copies and SQLite backup API for actual snapshots; do not simply
copy a live WAL file. Check foreign keys, source counts, original hashes and active
view equivalence. Actual user-snapshot migration remains a Y1 gate.

## 8. Verification and handoff

Executed 2026-10-02 with repository .venv Python:

```text
python -m pytest tests/test_baseline_plan_builder.py tests/test_calendar_builder.py
  tests/test_plan_persistence.py tests/test_schema_migrations.py
  tests/test_plan_revision_persistence.py tests/test_activity_workout_match.py
  tests/test_legacy_plan_conversion.py tests/test_plan_ai_v2_boundary.py
  tests/test_training_policy.py -q -p no:cacheprovider --basetemp <temporary directory>
264 passed in 16.22s
python reports/yearly_y0/discovery_probe.py
5 discovery probes passed
```

[Raw output and reproducible probe](../reports/yearly_y0/) characterize completion
hash exclusion, carried-workout match resolution, complete projection rebuild,
legacy deletion bypass and missing protection flags/idempotent v11 replay. Probe
mutations used an automatically removed synthetic DB, never user schedule data.
These checks verify existing behavior, not unimplemented season functionality.

Y0 has no new UI, so visual verification is not applicable. Live apply, actual
user-snapshot migration, full-year performance and season UI interactions remain
unverified Y1-Y3 gates. Account dashboard and phase-attributable tokens/credits
unavailable; prior 82-credit snapshot is historical. Client settings unchanged.
No commit, push or deployment.

| Phase | Revised scope/dependency | Planning allowance |
| --- | --- | ---: |
| Y1 | Intent schema/repository/UI, version concurrency, adoption compatibility, representative migration snapshots | 160k retained |
| Y2 | Pure phase/load orchestration; deterministic canonical adapter and auxiliary typing; methodology validation; full preview/apply audit; guard all legacy writers; calendar/nutrition compatibility | 240k, previously 210k |
| Y3 | Fixed-point windows; merged revisions/origins; completion/parameter resolution; protection/overrides; identity diff, composite stale checks and atomic failure/idempotence tests | 240k, previously 200k |

Y0-Y3 totals approximately 680k (40k + 160k + 240k + 240k) before contingency.
These are planning estimates, not measured consumption or credit guarantees.
Y1 requires separate authorization. Y4 mixed-method history, advanced peak tuning,
rollback and optional AI season composition remain separate decisions.

Y0 gate resolved: exact entry points, canonical/origin compatibility, orchestration,
protection semantics, migration/adoption, workload and conflict examples. Future
implementation tests and real-snapshot verification remain required at their own
gates; no architecture prerequisite for Y1 is left undecided.

## 9. Y1 implementation notes — 2026-10-02

Y1 is implemented in `services/season_plans.py` and `ui_nicegui/seasons.py`, with
additive schema version 12. The versioned input snapshot retains age, methodology,
run days, long-run day, available weekdays, starting weekly duration and optional
HRmax/LTHR. Decimal target speeds are canonical strings; the UI accepts pace per
kilometre and preserves the stored speed when a rounded display value is unchanged.
Supported distances use rounded whole metres, matching canonical workout measures.

All writes use BEGIN IMMEDIATE and check the captured season input version before
mutating. Linking also checks the reviewed current revision/hash, all prior workout
date coverage, running methodology and complete projection content/totals. It leaves
the existing active pointer, prescriptions, hashes and confirmed matches unchanged.
Native and already converted plans can link; the UI does not implicitly invoke the
existing whole-rowset legacy converter. Completed event history is immutable here.

Two operational decisions refine the initial design: overlapping unarchived season
intent is rejected before generation, and archiving a linked season does not release
ownership. Old writer and canonical approval guards moved forward into Y1 because
explicit linking establishes that ownership. Direct approval of any plan with dates
inside owned season coverage is rejected, even when its plan ID differs. Y2/Y3 must
introduce the intended owner-aware transaction path for season apply; no bypass is
exposed in Y1. Unlinked season intent does not block existing single-event planning.

408 focused/shared/browser tests passed; desktop and 390px views and scrolling
editor controls were inspected. A private backup of the installed v11 database
upgraded to v12 and replayed without any checked plan/match/settings row, active schedule or foreign-key
change. Backup reads used immutable read-only mode only after checking that no WAL
existed, and rejected a source that changed during backup. No live source migration
or sidecar write occurred; the private backup was removed. Evidence is in
[reports/yearly_y1](../reports/yearly_y1/). The Y0 verification/version statements
above remain historical. Y2 generation/apply and Y3 preservation remain pending.


## 10. Y2 implementation notes — 2026-10-02

Y2 implements services/season_schedule.py (pure orchestration/composition) and
services/season_generation.py (read-only preview and audited initial apply), additive
v13 DDL and the Seasons review workflow. Old writer guards remain; private connection
approval only accepts an initial full revision owned by the season transaction.
Generic approval has no public bypass.

Auxiliary sports/non-training leaves validate structurally and are excluded from
running prescription/runtime accounting. Goals and V2 AI parsing stay running-only.
Confirmed LTHR/ordinary MAF parameters produce native prescriptions. Linked previews
keep parent parameters but cannot apply. Optional explicitly covered service history
sets baseline; the editor currently uses an explicit starter load. Full normal-week
growth is 5%, validated against 10%, with conservative recovery/maintenance return.
Distance/TSS gaps are visible; event targets are estimates, not readiness evidence.

Initial apply checks snapshot/version, active/raw schedule rows, match evidence,
local date/timezone, versions and recomputed content under lock. Revision graph,
projection/pointer, ownership, nutrition invalidation and immutable review audit
commit once. Retry identity is idempotent. Known previous/following recovery
boundaries block apply. Protection state, origins and regeneration remain Y3.

611 distinct combined checks, three browser workflows, 1440px/390px inspection,
ten injected apply failures, year/leap cases and a private real v11→v13 backup
preservation check passed. Full-year timing was measured. Source database was not
migrated or applied to. [Evidence](../reports/yearly_y2/README.md). Changes remain
uncommitted; prior C2/Y0/Y1 work was retained.
