# Track T4 verification — 2026-10-02

T4 linked analytics is locally verified on clean starting baseline a4edfc7 (T3 track updates).

## Reproduction

From the repository root, with the existing virtual environment and bundled Playwright Chromium:

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests/test_track_charts.py tests/test_track_charts_browser.py tests/test_track_splits.py tests/test_track_visuals.py tests/test_track_visuals_browser.py tests/test_nicegui_data.py tests/test_activity_grid_browser.py::test_activity_grid_click_listeners_request_only_plain_row_data -q -p no:cacheprovider --basetemp=.tools/t4_gate_check
~~~

Final gate: **62 passed in 44.98s**, including five Track browser workflows. See [pytest.txt](pytest.txt). After final numerical review, the seven pure chart checks passed again, including rejection of a range starting beyond the activity while allowing three-decimal rounding of its final endpoint. These checks overlap the 62-check gate. Prior development failures are excluded: one test launcher timeout, Plotly's pointer-intercepting drag surface, an axis-controller disposal ordering bug (fixed), virtualized lap menu scrolling, an asynchronous null-bound assertion, NOT NULL fixture timestamp correction, narrow shell drawer setup, and the intentionally duplicated screen-reader readout locator. Final run has no warnings, browser page errors or server tracebacks. git diff --check passed. Both modified PowerShell packaging scripts parsed with zero errors; a packaged executable was not built.

## Verified behavior

- Available pace, heart rate, elevation and cadence share aligned panels and unit-aware elapsed-minute/GPS-distance axes. Pace ticks use m:ss, with faster values toward the top; steady pace has a meaningful minimum scale. Metric/imperial conversion, cycling cadence units inherited from T2, missing sensors, pauses, missing timestamps and disconnected GPS edges retain their established semantics.
- Actual Plotly pointer hover/click, client-local Leaflet edge events, pin/clear and keyboard position inspection share the map marker, a vertical cursor across every panel, and a textual reading. Keyboard inspection announces once; hover does not flood a live region. Taps/keyboard inspection reveal off-screen pinned positions without animation.
- T3 split selection shades exact km/mi boundaries; source laps use explicit elapsed endpoints independently of source timer totals. Unaligned source laps explain the missing chart highlight. Numeric ranges and actual drag extents (upper and lower panels tested during development) clip T3 route geometry, show quality-aware measurements and can be repeatedly replaced or cleared. Invalid bounds retain the previous selection.
- Axis, route metric, tab and activity changes preserve interval selection within the existing page session. Custom range state is stored as bounds, separately by activity, and reconstructed through the same validated analytics. Removed chart controllers dispose listeners and markers; the browser checks only one live controller after rebuilding the activity.
- 20,000 original points retain 19,999 original edge identities for map inspection. Each metric draws at most 2,400 sampled values plus gap separators (at most 4,800 entries); extrema remain, and sampling never connects disjoint runs. Pathologically sparse sensor data is also bounded. Pure long-chart preparation stayed within the five-second test bound.
- T2 base route layer IDs and styles remain intact when adding interval emphasis and chart cursors; recoloring keeps chart interaction and selection. Existing T1/T2/T3 workflows and activity/data wiring pass.

## Visual evidence

[Desktop](desktop.png) (1440px) and [narrow](narrow.png) (390px) full-page screenshots were inspected, together with readable chart detail crops: [desktop charts](desktop_charts.png), [narrow charts](narrow_charts.png). Controls wrap, chart axes are readable, selection spans all panels, and there is no document-width overflow. T3's wide measurements table remains horizontally scrollable. Earlier T1/T2/T3 screenshot bytes were restored after regression runs.

The synthetic long track intentionally exceeds its fixture activity summary/source totals. Charts and calculated splits use validated GPS distance; imported summary/lap totals remain distinct. External tile requests are blocked for deterministic tests.

## Files and limitations

New analytics/track_charts.py prepares charts and maps exact ranges. New ui_nicegui/track_charts.py and track_charts.js own presentation and client-local cursors. Track interval controls share range selection; stored laps retain explicit timing bounds; route features add raw edge identities. Activities integrates these controllers. pyproject.toml and both GUI packaging scripts include the client JavaScript asset. Tests, README, roadmap and source plan are updated.

- Cursor readings snap to original edge-end samples, rather than inventing interpolated sensor readings. Drawing samples local extrema; exceptionally short disjoint runs can be omitted by sampling, with surviving runs still disconnected. Original route data is retained for cursors. No unlimited-track guarantee.
- GPS distance excludes rejected edges, gaps and pauses. Distance axes can contain duplicate stop/pause positions; elapsed time separates them when available. Missing timestamps prevent elapsed selection and trustworthy pace, but distance sensor charts remain usable. Zero-distance timed sensor tracks use elapsed time only.
- Live tiles, real imported-activity field trial, dark theme and packaged executable remain unverified. No dark-theme control exists in the current shell. Screen-reader software itself was not tested; keyboard behavior and ARIA/readout wiring were checked.
- Changes remain uncommitted. No migration, live database/schedule write, push, deployment or package rebuild. Client settings unchanged; phase token/credit attribution unavailable. This completes T4; other deferred phases require their own authorization.
