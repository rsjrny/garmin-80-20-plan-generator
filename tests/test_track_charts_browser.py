from pathlib import Path
import json

import pytest
from browser_test_support import wait_for_layout
from playwright.sync_api import sync_playwright

from test_activity_grid_browser import _test_database, _free_loopback_port, _start_server, _stop_server, _click_activity
from test_track_visuals import points
from garmin_data_hub.db.sqlite import connect_sqlite


def _database(tmp_path, n=20000, missing_time=False):
    db = _test_database(tmp_path)
    raw = points(n)
    conn = connect_sqlite(db)
    conn.executemany(
        "INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,latitude,longitude,heart_rate_bpm,altitude_m,cadence) VALUES(202,?,?,?,?,?,?,?)",
        [(i, "" if missing_time else p["timestamp_utc"], p["lat_deg"], p["lon_deg"],
          None if i < 300 else 145, 20+i/1000, 175) for i,p in enumerate(raw)],
    )
    conn.execute("""CREATE TABLE activity_splits(activity_id INTEGER, split_number INTEGER,
        distance_meters REAL, duration_seconds REAL, average_speed REAL, average_hr REAL, max_hr REAL,
        elevation_gain REAL, elevation_loss REAL, avg_cadence REAL, start_time_gmt TEXT, raw_json TEXT)""")
    conn.executemany("INSERT INTO activity_splits VALUES(202,?,?,?,?,?,?,?,?,?,?,?)", [
        (1,800,280,2.86,145,170,15,5,175,raw[0]["timestamp_utc"],json.dumps({"lapTrigger":"manual","elapsedDuration":300})),
        (2,900,300,3,148,172,20,10,177,raw[300]["timestamp_utc"],'{}'),
    ])
    conn.commit()
    conn.close()
    return db


PLOT = "document.querySelector('[data-track-linked=\"true\"]')"
SNAPSHOT = """() => {
    const plot = document.querySelector('[data-track-linked="true"]');
    const map = getElement(Number(document.querySelector('.leaflet-container').id.slice(1))).map;
    const base = [], cursors = [], selected = [];
    map.eachLayer(l => {
      if(l.feature?.properties.overlays) base.push({id:l._leaflet_id,color:l.options.color});
      if(l._gdhTrackCursor) cursors.push([l.getLatLng().lat,l.getLatLng().lng]);
      if(l.feature && !l.feature.properties.overlays) selected.push(l.getLatLngs().length);
    });
    return {base,cursors,selected,axis:plot.dataset.trackAxis,cursor:plot.dataset.trackCursor,
      bounds:JSON.parse(plot.dataset.trackBounds||'null'),shapes:plot.layout.shapes,
      traces:plot.data.map(t=>({size:t.x.length,name:t.name}))};
}"""


def _select(page, label, option):
    field = page.get_by_label(label, exact=True)
    field.click()
    target = page.get_by_role("option", name=option, exact=True)
    if target.count() == 0:
        page.get_by_role("listbox").evaluate("e => {e.scrollTop = e.scrollHeight;}")
    target.click()
    return field


@pytest.mark.browser
def test_t4_long_linked_hover_keyboard_ranges_and_responsive_layout(tmp_path, browser_artifacts):
    db = _database(tmp_path)
    server = _start_server(db, port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1100})
            page.set_default_timeout(8000)
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.route("https://**/*", lambda r:r.abort())
            page.goto(f"http://127.0.0.1:{port}/activities", wait_until="networkidle")
            assert _click_activity(page,202,errors)
            page.get_by_role("tab",name="Track",exact=True).click()
            page.wait_for_selector('[data-track-linked="true"]')
            before = page.evaluate(SNAPSHOT)
            assert before["axis"] == "time"
            assert len(before["traces"]) == 4
            assert all(t["size"] <= 4800 for t in before["traces"])
            # Real pointer hover over a Plotly point updates the map and shared cursor.
            point = page.locator('[data-track-linked="true"] .scatterlayer .points path.point').nth(40)
            point.hover(force=True)
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackCursor !== ''")
            hover = page.evaluate(SNAPSHOT)
            assert len(hover["cursors"]) == 1
            assert any(s["type"] == "line" and s["yref"] == "paper" and s["y1"] == 1 for s in hover["shapes"])
            page.get_by_text("Sampled route position",exact=False).wait_for()
            point.click(force=True)
            pinned = page.evaluate(SNAPSHOT)["cursor"]
            page.get_by_text("Route measurements",exact=True).hover()
            assert page.evaluate(SNAPSHOT)["cursor"] == pinned
            # Keyboard position inspection keeps a cursor in missing sensor data.
            _select(page,"Chart x-axis","GPS distance (mi)")
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackAxis === 'distance'")
            page.get_by_label("Cursor position",exact=True).fill("0.1")
            page.get_by_role("button",name="Inspect position",exact=True).focus()
            page.keyboard.press("Enter")
            page.get_by_text("Heart rate: unavailable",exact=False).first.wait_for()
            page.get_by_role("button",name="Clear cursor",exact=True).click()
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackCursor === ''")
            # Route edge event uses original identities, beyond chart sampling.
            page.evaluate("""() => {
                const map=getElement(Number(document.querySelector('.leaflet-container').id.slice(1))).map;
                let target;map.eachLayer(l=>{if(!target && l.feature?.properties.overlays)target=l;});
                target.fire('mousemove',{latlng:target.getLatLngs().at(-1)});
            }""")
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackCursor !== ''")
            assert page.evaluate(SNAPSHOT)["cursors"]
            page.get_by_role("button",name="Clear cursor",exact=True).click()
            # A T3 split shades an exact whole mile on every chart, preserving base geometry.
            _select(page,"Split or lap","GPS split 1")
            page.wait_for_function("JSON.parse(document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds)?.[1] === 1")
            after = page.evaluate(SNAPSHOT)
            assert after["base"] == before["base"] and after["selected"]
            assert after["bounds"] == [0,1]
            _select(page,"Split or lap","Manual lap 1")
            _select(page,"Chart x-axis","Elapsed time (min)")
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackAxis === 'time'")
            page.wait_for_function("JSON.parse(document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds)?.[1] === 5")
            assert page.evaluate(SNAPSHOT)["bounds"] == [0,5]  # Timer total is 280, elapsed endpoint is 300.
            _select(page,"Split or lap","Stored lap 2")
            page.get_by_text("Selected interval has no reliable chart alignment.",exact=True).wait_for()
            assert page.evaluate(SNAPSHOT)["bounds"] is None
            # Explicit ranges share T3 route outline/details, including repeated replacement.
            page.get_by_label("Range start",exact=True).fill("1")
            page.get_by_label("Range end",exact=True).fill("3")
            page.get_by_role("button",name="Select chart range",exact=True).click()
            page.wait_for_function("JSON.parse(document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds)?.[0] === 1")
            assert page.get_by_label("Split or lap",exact=True).input_value() == "Selected chart range"
            assert page.evaluate(SNAPSHOT)["selected"]
            page.get_by_label("Range end",exact=True).fill("4")
            page.get_by_role("button",name="Select chart range",exact=True).click()
            page.wait_for_function("JSON.parse(document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds)?.[1] === 4")
            page.get_by_label("Range end",exact=True).fill("0")
            page.get_by_role("button",name="Select chart range",exact=True).click()
            page.get_by_text("Choose a range within the activity, with end after start.",exact=True).wait_for()
            assert page.evaluate(SNAPSHOT)["bounds"] == [1,4]
            # Actual horizontal drag extent selects a chart range.
            page.locator('[data-track-linked="true"]').scroll_into_view_if_needed()
            drag = page.evaluate("""() => {
              const p=document.querySelector('[data-track-linked="true"]'),r=p.getBoundingClientRect(),l=p._fullLayout;
              return {x0:r.left+l.xaxis._offset+l.xaxis._length*.1,x1:r.left+l.xaxis._offset+l.xaxis._length*.15,y:r.top+l.yaxis4._offset+l.yaxis4._length*.5};
            }""")
            page.mouse.move(drag["x0"],drag["y"])
            page.mouse.down()
            page.mouse.move(drag["x1"],drag["y"],steps=10)
            page.mouse.up()
            page.wait_for_function("JSON.parse(document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds)?.[0] > 30")
            _select(page,"Chart x-axis","GPS distance (mi)")
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackAxis === 'distance'")
            assert page.evaluate(SNAPSHOT)["bounds"][0] > 3
            page.get_by_role("tab",name="Overview",exact=True).click()
            page.get_by_role("tab",name="Track",exact=True).click()
            assert page.get_by_label("Chart x-axis",exact=True).input_value() == "GPS distance (mi)"
            assert page.get_by_label("Split or lap",exact=True).input_value() == "Selected chart range"
            _select(page,"Route metric","Heart rate")
            page.wait_for_function("("+SNAPSHOT+")().base.some((section, i) => section.color !== "+json.dumps(before["base"])+"[i].color)")
            recolored = page.evaluate(SNAPSHOT)
            assert [s["id"] for s in recolored["base"]] == [s["id"] for s in before["base"]]
            assert recolored["selected"] and recolored["bounds"]
            page.evaluate("""() => {
              const map=getElement(Number(document.querySelector('.leaflet-container').id.slice(1))).map;
              let target;map.eachLayer(l=>{if(!target && l.feature?.properties.overlays)target=l;});
              target.fire('mousemove',{latlng:target.getLatLngs().at(-1)});
            }""")
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackCursor !== ''")
            assert page.evaluate(SNAPSHOT)["cursors"]
            page.get_by_role("button",name="Clear cursor",exact=True).click()
            output = browser_artifacts
            output.mkdir(parents=True,exist_ok=True)
            page.get_by_role("button",name="Fit route",exact=True).click()
            page.evaluate("window.scrollTo(0,0)")
            page.screenshot(path=str(output/"desktop.png"),full_page=True)
            page.set_viewport_size({"width":390,"height":844})
            page.get_by_role("button",name="Fit route",exact=True).click()
            wait_for_layout(page)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.evaluate("window.scrollTo(0,0)")
            page.screenshot(path=str(output/"narrow.png"),full_page=True)
            assert page.locator('[data-track-linked="true"]').evaluate("e=>e.getBoundingClientRect().width") <= 390
            # Resize while hidden, then reveal: no deferred Plotly resize may reject.
            page.get_by_role("tab",name="Overview",exact=True).click()
            page.set_viewport_size({"width":1440,"height":1100})
            page.get_by_role("tab",name="Track",exact=True).click()
            page.wait_for_function("""() => {
                const plot = document.querySelector('[data-track-linked="true"]');
                return plot && plot._fullLayout.width <= plot.clientWidth + 1;
            }""")
            # Rebuilding another activity disposes old controllers and range state is activity-isolated.
            page.get_by_role("tab",name="Overview",exact=True).click()
            assert _click_activity(page,101,errors)
            page.get_by_role("tab",name="Track",exact=True).click()
            page.get_by_text("No valid GPS trackpoints are stored",exact=False).wait_for()
            assert _click_activity(page,202,errors)
            page.get_by_role("tab",name="Track",exact=True).click()
            page.wait_for_selector('[data-track-linked="true"]')
            assert page.get_by_label("Split or lap",exact=True).input_value() == "Selected chart range"
            page.wait_for_function("Object.keys(window.gdhTrackCharts).length === 1")
            page.get_by_role("button",name="Clear interval",exact=True).click()
            page.wait_for_function("document.querySelector('[data-track-linked=\"true\"]').dataset.trackBounds === 'null'")
            assert not page.evaluate(SNAPSHOT)["selected"]
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        log = _stop_server(server)
    assert not errors, errors
    assert "Traceback" not in log, log


@pytest.mark.browser
def test_t4_no_timestamps_sensor_fallback(tmp_path, browser_artifacts):
    db = _database(tmp_path, n=401, missing_time=True)
    server = _start_server(db, port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.set_default_timeout(8000)
            page.on("pageerror", lambda e:errors.append(str(e)))
            page.route("https://**/*",lambda r:r.abort())
            page.goto(f"http://127.0.0.1:{port}/activities",wait_until="networkidle")
            assert _click_activity(page,202,errors)
            page.get_by_role("tab",name="Track",exact=True).click()
            page.wait_for_selector('[data-track-linked="true"]')
            page.set_viewport_size({"width":390,"height":844})
            page.get_by_text("Elapsed-time charts unavailable",exact=False).wait_for()
            assert page.evaluate(SNAPSHOT)["axis"] == "distance"
            assert [t["name"] for t in page.evaluate(SNAPSHOT)["traces"]] == ["Heart rate","Elevation","Cadence"]
            page.get_by_label("Cursor position",exact=True).fill("0.6")
            page.get_by_role("button",name="Inspect position",exact=True).click()
            page.get_by_text("Elapsed time unavailable",exact=False).first.wait_for()
            page.get_by_label("Range start",exact=True).fill("0.1")
            page.get_by_label("Range end",exact=True).fill("0.6")
            page.get_by_role("button",name="Select chart range",exact=True).click()
            page.get_by_text("Incomplete timing / GPS quality break; pace unavailable",exact=False).first.wait_for()
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        log = _stop_server(server)
    assert not errors, errors
    assert "Traceback" not in log, log
