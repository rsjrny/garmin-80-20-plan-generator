# Activity Track Visuals Plan

Status: Proposed / ready for implementation discovery

## Objective

Make the Activities page's Track tab explain how an activity unfolded, rather than only showing where it occurred. The initial release should color the route by pace, provide a readable legend and useful point details, and establish a reusable foundation for heart rate, elevation, and cadence overlays.

## Product Principles

- The route remains the primary visual.
- Colors must communicate measured values, not decoration.
- The map, legend, and charts should use the same metric boundaries.
- GPS errors, pauses, and missing sensor data must not look like meaningful performance changes.
- Color must not be the only way information is communicated.
- The initial implementation should be useful on desktop and narrow layouts.

## First Shippable Slice: Pace-Colored Route

### Scope

- Replace or overlay the single-color route with short route segments colored by smoothed pace.
- Add a pace legend displaying the exact pace ranges and current distance unit.
- Show start and finish markers.
- Show a hover/click tooltip for a route segment.
- Distinguish stopped, paused, invalid, and missing-data segments in neutral gray.
- Retain the existing uncolored route as a fallback when timestamped GPS data is insufficient.

### Segment tooltip

Where data is available, show:

- Elapsed or local activity time
- Cumulative distance
- Smoothed pace
- Heart rate
- Elevation
- Cadence

The tooltip should omit missing values rather than showing misleading zeros.

### Pace palette

Use a color-blind-conscious sequential scale where the ordering remains understandable without relying on red versus green.

Recommended direction, fastest to slowest:

```text
deep blue -> cyan -> green -> amber -> muted orange
```

Use neutral gray for stopped, paused, invalid, or missing data. Verify foreground and background contrast in both light and dark themes if both are supported.

### Pace boundaries

For the initial version, derive five bands from valid smoothed moving-pace samples within the activity. Use a robust distribution method so isolated GPS spikes do not control the scale.

Recommended approach:

1. Exclude invalid, paused, and stopped samples.
2. Winsorize or clamp samples to reasonable lower and upper percentiles.
3. Derive five bands from the remaining pace distribution.
4. Round displayed boundaries to readable pace values.
5. Ensure the same boundaries drive both route colors and the legend.

Later, allow the user to choose activity-relative bands, configured pace zones, or fixed custom bands.

## Track Processing Pipeline

The visual layer should consume processed segments rather than calculate metrics inside map-rendering code.

```text
raw track points
    -> validate timestamps and coordinates
    -> calculate point-to-point time and distance
    -> identify pauses, stops, and GPS jumps
    -> calculate raw pace
    -> smooth pace
    -> calculate cumulative distance
    -> classify into display bands
    -> emit renderable track segments
```

### Suggested processed segment shape

Adapt names and types to repository conventions.

```text
start coordinate
end coordinate
start/end timestamp
cumulative distance
duration
raw pace
smoothed pace
heart rate (optional)
elevation (optional)
cadence (optional)
metric band
quality/status flag
```

### Data-quality rules

- Reject points with invalid coordinates or non-increasing timestamps.
- Do not calculate pace across explicit pauses.
- Detect implausible GPS jumps using speed and/or distance thresholds appropriate to the activity type.
- Treat near-zero movement as stopped rather than infinitely slow pace.
- Do not bridge long sampling gaps as ordinary route segments.
- Preserve enough diagnostic status to explain why a segment is gray or absent.

### Smoothing

Avoid coloring directly from raw point-to-point pace because GPS noise produces distracting speckling.

Start with one configurable method:

- A centered rolling time window of approximately 15-30 seconds; or
- A centered rolling distance window of approximately 50-100 metres.

Select the method that best matches the current track representation and available helpers. Handle the beginning and end of the activity without dropping visible route portions.

## Follow-Up Slice: Metric Selector

Add a compact selector above the map:

```text
Pace | Heart rate | Elevation | Cadence
```

- Hide or disable metrics unavailable for the selected activity.
- Preserve the selected metric while inspecting the same activity.
- Each metric owns its palette, units, band calculation, and tooltip formatting.
- Changing metrics must recolor existing processed segments without reloading the activity.

Suggested metric treatment:

- Pace: athlete zones when available, otherwise activity-relative bands.
- Heart rate: configured HR zones when available, otherwise activity-relative bands.
- Elevation: continuous low-to-high scale; consider grade as a separate future metric.
- Cadence: configured or activity-relative cadence bands.

## Follow-Up Slice: Laps and Splits

- Add mile or kilometre markers based on the user's distance-unit preference.
- Display manual laps distinctly from automatic distance splits.
- Highlight the fastest split without overpowering the map.
- Selecting a lap zooms or emphasizes its route section.
- Show lap distance, duration, pace, average/max heart rate, elevation change, and cadence where available.

## Follow-Up Slice: Linked Charts

Add aligned charts below the map for available metrics:

- Pace
- Heart rate
- Elevation
- Cadence

Interactions should be linked:

- Hovering a chart highlights the corresponding map location.
- Hovering the route places the same cursor on every visible chart.
- Selecting a lap or distance range emphasizes that interval everywhere.
- The x-axis can switch between elapsed time and distance if the existing chart system supports it cleanly.

## Layout Direction

### Summary strip

Keep a compact summary near the top of the tab:

- Distance
- Moving time
- Average pace
- Elevation gain
- Average heart rate, when available

Avoid duplicating a large activity summary already visible elsewhere on the Activities page.

### Map controls

- Metric selector
- Legend
- Map/satellite choice if supported by the existing map provider
- Fit-route action
- Full-screen action if supported without major custom work

### Responsive behavior

- Desktop: legend may be overlaid in a map corner if it does not obscure the route.
- Narrow layouts: place the selector and legend above or below the map.
- Tooltips must remain readable without leaving the viewport.

## Accessibility

- Use a color-blind-conscious palette.
- Do not use hue alone: provide numeric legend labels, tooltip values, and a strong selected-segment outline.
- Ensure controls are keyboard reachable.
- Give map controls accessible names.
- Do not announce every route segment to screen readers; provide a concise textual summary and accessible lap/split table instead.
- Respect reduced-motion preferences for transitions and map animation.

## Performance

Long activities may contain thousands of points. During discovery, determine the rendering limit of the current map library.

Potential safeguards:

- Merge adjacent segments in the same display band when detail would not be lost.
- Simplify geometry according to zoom level while retaining original metrics for tooltips.
- Cache processed tracks by activity and processing-version key.
- Avoid creating one heavyweight UI component per raw GPS point.
- Recolor existing geometry rather than reprocessing raw data when only the selected metric changes.

## Empty, Loading, and Error States

- Loading: retain the map frame and show a restrained loading state.
- No GPS track: explain that map visualization is unavailable for the activity.
- GPS track without timestamps: show the route but explain why pace coloring is unavailable.
- Partial sensor data: render available metrics and mark unavailable choices clearly.
- Processing error: fall back to the existing route and surface a non-blocking message.

## Implementation Discovery

Before modifying code:

1. Locate the Activities page and Track tab components.
2. Identify the map library and how the current route polyline is constructed.
3. Trace the track-point model from persistence/API through the UI.
4. Inventory per-point fields: coordinates, timestamp, distance, speed, heart rate, elevation, cadence, and pause status.
5. Identify current unit-formatting and user-preference helpers.
6. Identify existing chart, palette, tooltip, and theme utilities.
7. Check whether laps and pauses already have domain models.
8. Measure representative track sizes and current map rendering behavior.
9. Confirm whether processing belongs in the backend, view model, or client-side presentation layer.

Record the findings in this file before implementation, including exact files and architectural decisions.

## Proposed Delivery Order

### Phase 1: Pace foundation

- Complete implementation discovery.
- Add pure track validation, pace calculation, smoothing, and classification helpers.
- Add unit tests for the processing pipeline.
- Render the pace-colored route and legend.
- Add start/finish markers and segment details.
- Add fallback and empty states.

### Phase 2: Metric framework

- Extract reusable metric configuration.
- Add the metric selector.
- Add heart-rate, elevation, and cadence coloring.
- Add metric-specific legends and formatting.

### Phase 3: Splits and interaction

- Add distance and manual-lap markers.
- Add lap highlighting and details.
- Improve selection and keyboard behavior.

### Phase 4: Linked analytics

- Add aligned charts.
- Synchronize hover and selection with the map.
- Add range or lap comparison if it remains visually clear.

## Phase 1 Acceptance Criteria

- A timestamped GPS activity displays a route colored by smoothed pace.
- The legend displays exact ranges in the user's pace and distance units.
- Faster and slower sections are visually distinguishable and correctly ordered.
- Paused, stopped, missing, and invalid sections do not receive misleading pace colors.
- Hovering or selecting a rendered section exposes its pace and other available measurements.
- Start and finish are clearly identified.
- Activities without sufficient data retain a usable route or explanatory empty state.
- The feature remains usable with a representative long activity.
- Processing logic has deterministic unit tests for normal, paused, noisy, sparse, and invalid tracks.
- Existing Activities-page behavior remains covered by regression tests.

## Test Scenarios

- Steady-paced run with frequent samples
- Progressive or interval workout with clearly different pace bands
- Explicit pause and resume
- Stationary time without an explicit pause marker
- GPS jump or implausible instantaneous speed
- Long sampling gap
- Repeated or out-of-order timestamps
- Missing coordinates or timestamps
- Very short activity
- Long activity with thousands of points
- Out-and-back route with overlapping geometry
- Metric and imperial preferences
- Light and dark themes, if supported
- Track with partial HR, elevation, or cadence data
- Activity with no GPS track

## Decisions to Make During Discovery

- Time-based versus distance-based pace smoothing
- Activity-relative bands versus configured athlete pace zones for the default
- The exact palette and number of bands
- Where processed segments should be calculated and cached
- Whether the existing map library supports efficient per-segment styling and hit testing
- Whether overlapping out-and-back segments require directional arrows or selection emphasis

## Out of Scope for the First Slice

- Grade-adjusted pace
- Race-performance predictions
- Automatic coaching conclusions
- Route heatmaps aggregated across activities
- Comparing multiple activities on one map
- Editing GPS tracks
- Replacing the current map provider

## Definition of Done

The Track tab should let a user answer, at a glance:

1. Where did I speed up or slow down?
2. Were slow sections associated with stops or terrain?
3. What were my measurements at a specific point on the route?
4. Can I trust the visualization despite ordinary GPS noise?

The first slice is complete when those questions are answered clearly for pace without degrading activities that have incomplete track data.
