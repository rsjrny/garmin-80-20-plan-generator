# Y4 verification and handoff

Date: 2026-10-02 (America/New_York). User requested “do Y4 now”.
Scope follows the roadmap's Yearly 4 refinement/event tuning. Optional rollback
remains separately scoped. Implementation is local and uncommitted.

## Delivered behavior

- Versioned deterministic A-build precedence over a supporting taper on shared
  dates. Event day, recovery precedence and distance recovery minimums remain
  reserved; standalone B taper and A taper protections remain effective.
- Effective event windows and controlled/shared/constrained/outside-season
  preparation counts. These describe calendar opportunity, not readiness or a
  physiological peak prediction. Shared and constrained categories can overlap.
- Specific recovery/peak calendar clearance dates, workload guidance, and preview
  links to event tuning. Edits save intent; a fresh reviewed preview is required
  before the active calendar changes. Current editor versions reject stale edits.
- Explicit optional completion participation duration, separate from performance
  time/pace targets. Duration stays estimated; missing duration remains unknown.
  Short completion events in an A taper must fit the actual easy-session budget,
  remain at most 5 km, and retain an easy native prescription. Events still consume
  configured run slots and hard-event limits.
- Identical taper participation checks for generated and preserved event sessions,
  including protected cancelled events. An immutable performance prescription
  cannot become easy merely by changing saved intent or retaining protection.
- Generator/policy v2 recommend full future evaluation when a linked revision
  comes from older rules. Custom ranges still undergo complete merged validation;
  preservation, origins, parameters and completion evidence retain Y3 semantics.
- Hashed candidate preparation assessments retained in immutable application
  history and shown under Recorded event preparation.
- Additive schema v15 adds nullable participation_seconds, including integer/range/
  completion-goal CHECK constraints. Existing events get NULL; prior immutable
  revision documents, hashes and audit rows are retained.

## Verification

The combined suite verifies **666 non-browser checks**. **Six browser workflows**
(four existing Seasons workflows and two new Y4 workflows) verify desktop/390px
conflict review, tuning, effective windows, completion duration, protected
regeneration, stale rejection, reviewed apply, history and prior season/linking
behavior. Total: **672 distinct checks**; reruns are not added to this count.

Use repository .venv/Scripts/python.exe, -q -p no:cacheprovider, and distinct
--basetemp directories under .tools. The combined pytest paths are:

- tests/test_season_refinement.py, test_season_regeneration.py,
  test_season_generation.py, test_season_plans.py, test_season_migrations.py,
  test_season_browser_logs.py
- tests/test_plan_revision_persistence.py, test_activity_workout_match.py,
  test_legacy_plan_conversion.py, test_baseline_plan_builder.py,
  test_plan_persistence.py, test_schema_migrations.py
- tests/test_calendar_builder.py, test_training_policy.py,
  test_plan_methodology_policy.py, test_plan_ai_v2_boundary.py,
  test_runtime_methodology_compliance.py, test_nicegui_data.py,
  test_nicegui_workspace.py, tests/plan_methodology_contracts

[Combined output](verified-suite.txt): 666 passed in 162.04s. A final combined
confirmation passed 666 checks in 162.41s in [final output](final-suite.txt), with the final
advisory wording checked by 27 passing checks in 20.59s in [focused confirmation](refinement-final.txt).
Initial existing season regression: [155 passed](initial-focused.txt). Initial
Y4 acceptance: [26 passed](refinement.txt), before the additional protected
cancelled-event case. The final focused scope has 27 checks.

New browser command:
python -m pytest tests/test_season_refinement_browser.py -q -p no:cacheprovider
--basetemp .tools/y4-browsers-complete

[Final settled visual/browser output](browser-complete.txt): two passed in 17.28s.
The earlier [settled capture run](browser-final.txt) also passed both workflows in 17.07s.
Existing browser command uses tests/test_seasons_browser.py,
tests/test_season_generation_browser.py and tests/test_season_regeneration_browser.py.
[Compatibility output](compatibility-browser.txt): four passed in 80.60s.
Compatibility captures live in compat_y3; prior Y3 evidence was restored byte for
byte. Raw server logs retain any existing Windows Proactor socket-close noise;
the existing narrow application_logs classifier still rejects app tracebacks.
No application error suppression was introduced.

Earlier Y4 browser attempts are retained: [initial output](browser.txt) failed
at post-apply history navigation behind the responsive drawer; the standard
reload/drawer-close flow then passed [both workflows](browser-verified.txt).
A screenshot-helper test refactor missed its second workflow's scope;
[visual attempt](browser-visual.txt) records one passed/one NameError. The final
shared helper and both settled capture workflows pass. Neither failure required
a product change. Screenshots wait for dialog transitions before capture.

Acceptance includes parameterized invalid estimates, DB-level CHECK enforcement,
unknown/oversized/performance/distance taper rejection, both running methods,
locked and cancelled preserved events, read-only preview, stale estimate edits,
exact event delta, immutable parent hashes/audits, v14 upgrade/replay, and injected
v15 migration/repair failures. Existing apply atomicity and history/match/origin
protections remain covered by the combined regression scope.

git diff --check passed. No phase commit, push, deployment or packaging rebuild.

## Installed database and timing

python reports/yearly_y4/verify_real_snapshot.py <installed database path>

A private read-only backup of the installed v14 database upgraded/replayed v15.
All prior app-owned fields/rows, immutable schedule documents, active schedule
and foreign-key state were unchanged. [Aggregate evidence](real_snapshot.txt).
The source was never migrated and the private full backup was removed.
Representative fresh/legacy/native/converted migration coverage is in the suite.

python reports/yearly_y4/benchmark.py

For synthetic 366-day/three-event seasons, initial preview took 0.023–0.034s,
initial apply 0.676–0.696s, regeneration preview 0.408–0.459s and atomic
regeneration apply 1.297–1.491s. Both methods preserved 259 occurrences.
[Timing output](performance.txt). Synthetic databases were removed. These are
local timings, not a general performance guarantee.

## Visual inspection

Settled desktop/390px dialogs were inspected. Text wraps, the event form scrolls,
modal tables remain within their scroll area, and tuning/apply controls are
reachable. The editor shows changing effective dates and C-event taper behavior.

[Desktop conflicts](conflicts_desktop.png), [phone conflicts](conflicts_narrow.png),
[phone tuning](tuning_narrow.png), [effective window and save](tuning_window_narrow.png),
[phone preparation](preparation_narrow.png),
[phone apply](apply_narrow.png), [history](history_narrow.png),
[completion editor](completion_editor_desktop.png), [editor controls](completion_controls_desktop.png),
[easy event](easy_taper_desktop.png).

## Handoff and remaining scope

Y4 refinement/event tuning is implemented. Optional rollback requires a separate
design and authorization; no direct active-pointer rollback was added. Physiological
peak optimization/prediction, AI season composition, automatic covered-history
collection, mixed methods/sports, manual creation UI, packaging and a live field
trial remain separate work. Existing deterministic duration rules and readiness
warnings remain authoritative; this phase does not introduce new physiological
thresholds or finish-time guarantees.

New tests: tests/test_season_refinement.py and test_season_refinement_browser.py.
Source: season_schedule.py, season_plans.py, season_regeneration.py,
season_generation.py, Seasons UI and v15 schema/migration. README, source plan,
architecture and roadmap describe the workflow. Client settings were unchanged;
attributable token/account-credit usage is unavailable.
