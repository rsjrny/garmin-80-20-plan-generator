# Yearly Multi-Event Scheduling Plan

Status: Proposed / not scheduled

## Objective

Allow an athlete to generate a year-long training schedule around multiple events, then add, change, or remove events later and regenerate only the affected portion of the schedule without unnecessarily replacing completed or intentionally preserved workouts.

## Guiding Principle

Events, athlete settings, availability, and season goals are source data. Generated workouts are replaceable output derived from that source data.

Keeping those concepts separate makes later regeneration safe, explainable, and reproducible.

## Required Capabilities

### 1. Event calendar

Store multiple events with fields such as:

- Event name and date
- Event type and distance
- Priority: A, B, or C
- Goal and optional target time or pace
- Terrain or course characteristics
- Optional taper and recovery preferences
- Status: planned, completed, skipped, or cancelled

An A event is a primary peak target. B events receive meaningful preparation but should not compromise an A event. C events generally replace suitable training sessions or act as supported training races.

### 2. Persistent season plan

Introduce a season-level record that retains both the inputs and generation metadata:

- Planning start and end dates
- Associated events and priorities
- Athlete settings and current fitness assumptions
- Weekly availability and preferred workout days
- Training constraints
- Generation version
- Generation timestamp and rationale
- Current revision or version identifier

The saved planning intent must be sufficient to reproduce or revise the schedule later.

### 3. Multi-event schedule generation

Generate the schedule by considering all events together rather than generating an independent plan for each event.

The generator should divide the season into appropriate phases, including:

- Base
- Build
- Event-specific preparation
- Taper
- Event
- Recovery
- Maintenance or transition

It must reconcile phases when events are close together and use event priority to decide which event controls the surrounding training.

### 4. Incremental regeneration

When an event is added, edited, cancelled, or reprioritized, calculate an affected date range instead of replacing the entire year by default.

The general shape should be:

```text
preserved history -> transition -> revised build -> taper -> event -> recovery -> reconnect to existing plan
```

Provide these regeneration modes:

- Regenerate the full schedule from today
- Regenerate only the calculated affected range
- Allow the user to select a custom replacement range

Completed workouts and dates before today should be locked by default. User-edited or manually locked workouts should also be preserved unless the user explicitly elects to replace them.

### 5. Preview and apply workflow

Regeneration should be previewable before it changes the active schedule. The preview should show:

- Added workouts
- Removed workouts
- Changed workouts
- Preserved workouts
- Conflicts and warnings
- The replacement date range
- A concise explanation of why the plan changed

Applying a preview should be an explicit operation.

### 6. Conflict and safety rules

Define explicit behavior for:

- Events with insufficient preparation time
- Overlapping taper or recovery periods
- Multiple A events that are too close for separate peaks
- B or C events that conflict with key A-event workouts
- An event inserted inside an existing training block
- Existing user-edited workouts
- Excessive increases in volume or intensity
- Recovery requirements after long or high-priority events

When the schedule cannot meet all goals coherently, return clear warnings and recommended compromises instead of silently producing a poor plan.

### 7. Revision history

Retain each applied regeneration with:

- Previous and resulting plan state
- Events added, changed, or removed
- Replacement date range
- Preserved and replaced workouts
- Generator inputs and version
- Warnings and rationale
- Application timestamp

The existing plan-import history and date-range replacement concepts may be reusable, but this must be confirmed during implementation discovery.

## Suggested Data Model

Exact names should follow existing repository conventions.

### `season_plan`

- Identifier
- Name
- Start and end dates
- Status
- Athlete/configuration snapshot
- Availability and constraint data
- Current revision identifier
- Created and updated timestamps

### `season_event`

- Identifier
- Season-plan identifier
- Name, date, type, and distance
- Priority
- Goal and target fields
- Terrain/course fields
- Taper and recovery overrides
- Status

### Planned-workout additions

Generated workouts should be traceable to:

- Season plan and revision
- Influencing event, when applicable
- Training phase
- Generation source
- Locked/preserved state
- Manual-edit state

## Regeneration Policy

An initial policy can use deterministic windows around the changed event, adjusted for event distance, priority, and neighboring events.

1. Start no earlier than today unless explicitly requested.
2. Preserve completed, locked, and manually edited workouts by default.
3. Look backward far enough to rebuild event-specific preparation.
4. Look forward through taper, event, recovery, and reconnection.
5. Expand the window when it intersects another event's preparation block.
6. Re-evaluate the full season if event priorities make local reconciliation impossible.
7. Validate workload progression before presenting the preview.

The affected-range calculation and its reasons should be visible in the preview.

## User Experience

### Season screen

- Year or season timeline
- Event list with A/B/C priority
- Add, edit, cancel, and reorder events
- Generate or regenerate action
- Visible schedule revision and warnings

### Regeneration dialog

- Summary of the triggering event change
- Recommended affected range
- Full-from-today, affected-range, and custom-range choices
- Preservation controls for locked/manual workouts
- Diff preview before applying

### Calendar

- Events visibly distinct from workouts
- Training phases visible at a glance
- Indicators for generated, manually edited, locked, and preserved workouts

## Delivery Sequence

### Phase 1: Multiple-event foundation

- Add season and event persistence
- Add event management UI
- Add priority and validation rules
- Preserve backward compatibility with existing plans

### Phase 2: Full-year generation

- Pass all season events into generation
- Generate phase boundaries and recovery periods
- Implement basic A/B/C precedence
- Add workload and overlap warnings

### Phase 3: Incremental regeneration

- Calculate affected ranges
- Preserve past, completed, locked, and manual workouts
- Produce a plan diff
- Preview and explicitly apply replacements
- Record revisions

### Phase 4: Refinement

- Improve conflict resolution and peak timing
- Add plan comparison and rollback if desired
- Tune rules for different event types and distances
- Expand test coverage using realistic multi-event seasons

## Testing Strategy

Include scenarios for:

- One event in a year
- Several well-spaced events
- A primary event with several tune-up races
- Two closely spaced A events
- An event added before an existing event
- An event added after an existing event
- An event added inside taper or recovery
- Event cancellation and date changes
- Regeneration with completed workouts
- Regeneration with locked and manually edited workouts
- Year-boundary schedules
- Failed validation leaving the active plan unchanged
- Applying a preview recording complete revision history

## Discovery Tasks Before Implementation

- Map the current plan-generation entry points and inputs.
- Confirm how active plans and planned workouts are persisted.
- Review current date-range replacement and plan-import history behavior.
- Identify how completed and manually edited workouts can be distinguished.
- Decide whether the existing generator can accept multiple event anchors or needs a season-level orchestration layer.
- Establish migration and backward-compatibility requirements.
- Define workload and event-conflict rules with representative examples.

## Rough Effort

This is a medium-to-large feature.

- A functional first version may take approximately 1-2 weeks if the current generator already supports date ranges and parameterized target events.
- A polished version with conflict handling, previews, revision history, preservation rules, and comprehensive tests may take approximately 3-6 weeks.

These estimates should be revised after completing the discovery tasks above.

## Completion Criteria

The feature is complete when a user can:

1. Create a season covering up to a year.
2. Add multiple prioritized events.
3. Generate one coherent schedule around all events.
4. Later add or modify an event.
5. Preview the resulting schedule diff and warnings.
6. Apply only the necessary future changes while preserving protected workouts.
7. Review how and why each schedule revision was produced.
