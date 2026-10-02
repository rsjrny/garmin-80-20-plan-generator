# Track T3 verification — 2026-10-02

T3 (laps/splits and route selection) is locally verified on baseline 1fca677.

## Reproduction

Run from the repository root with the existing virtual environment and bundled Playwright Chromium:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_track_splits.py tests/test_track_visuals.py tests/test_track_visuals_browser.py::test_t3_split_lap_selection_markers_keyboard_and_narrow_layout tests/test_nicegui_data.py tests/test_activity_grid_browser.py::test_activity_grid_click_listeners_request_only_plain_row_data -q -p no:cacheprovider --basetemp=.tools/t3_final_check
.\.venv\Scripts\python.exe -m pytest tests/test_track_visuals_browser.py::test_pace_track_desktop_narrow_and_popup tests/test_track_visuals_browser.py::test_metric_switch_reuses_sections_and_preserves_selection -q -p no:cacheprovider --basetemp=.tools/t3_prior_browsers
```

Final focused run: 51 passed in 8.27s; prior Track browser workflows: 2 passed in 13.54s. These are 53 distinct checks, including three browser workflows. See [pytest.txt](pytest.txt). An additional broader run including unrelated sync characterization did not return a completion result and is excluded from these counts.

Processing covers exact/interpolated metric and imperial boundaries, partial final splits, fastest eligibility, stops/pauses, weighted partial sensors, elevation, missing timestamps, sampling gaps, GPS jumps, invalid coordinates, malformed/old-schema lap metadata, source totals versus elapsed timing, explicit manual provenance and activity-isolated queries.

The T3 browser workflow verifies selection outlines without changing base layer IDs/colors, clearing, keyboard activation, clickable boundary markers through the map event bridge, selection across tabs and metric changes, unaligned-lap explanations and a named measurements table. Previous workflows retain the 20,000-point route and all T2 metric switches/partial sensors.

Desktop (1440px) and narrow (390px) full-page evidence: [desktop.png](desktop.png), [narrow.png](narrow.png). Inspected map emphasis, wrapping controls, selected measurements, and the horizontally scrollable table; no page-width overflow. External tile requests are blocked in browser tests. No browser errors or server tracebacks in the passing workflows.

A synthetic 20,000-point route produced 60 kilometre splits in 0.133s locally, excluding track preprocessing. This is a local observation, not an unlimited-track guarantee. Boundary markers are sampled above 200; every interval remains selectable in the table and controls. Earlier T1/T2 evidence images were restored after regression runs.

## Limitations

- Source lap trigger/timing availability varies. Only explicit manual trigger metadata is labeled Manual lap. Unknown provenance remains Stored lap. Timer duration alone is not used to invent an elapsed endpoint; last laps without an endpoint may have details but no route highlight.
- Source lap distance/duration can differ from calculated GPS splits. GPS distance excludes jumps, gaps and pauses. Sensor averages weight available edge-end readings by time; coverage excludes gaps and pauses. Net elevation is omitted without complete endpoint readings.
- Live tiles, real imported-lap field trial, dark theme and packaged executable verification were not performed. No dark-theme setting is present in the current shell.
- No database migration, live schedule write, commit, push or deployment. Phase token/credit attribution is unavailable; client model settings were unchanged.
