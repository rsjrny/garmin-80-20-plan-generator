from pathlib import Path
from playwright.sync_api import sync_playwright
from test_activity_grid_browser import (_test_database, _free_loopback_port, _start_server, _stop_server, _click_activity)
from test_track_visuals import points
from garmin_data_hub.db.sqlite import connect_sqlite


def test_pace_track_desktop_narrow_and_popup(tmp_path):
    db = _test_database(tmp_path)
    conn = connect_sqlite(db)
    conn.executemany(
        "INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,latitude,longitude,heart_rate_bpm,altitude_m) VALUES(202,?,?,?,?,?,?)",
        [(i,p["timestamp_utc"],p["lat_deg"],p["lon_deg"],140,20) for i,p in enumerate(points(20000))],
    )
    conn.commit()
    conn.close()
    port = _free_loopback_port()
    process = _start_server(db, port)
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.route("https://**/*", lambda route: route.abort())
            page.goto(f"http://127.0.0.1:{port}/activities", wait_until="networkidle")
            assert _click_activity(page,202,errors)
            page.get_by_role("tab", name="Track", exact=True).click()
            page.get_by_text("Smoothed pace (min/mi)", exact=False).wait_for()
            page.get_by_text("Unavailable: Cadence.",exact=False).wait_for()
            page.wait_for_function("document.querySelector('.leaflet-container canvas') !== null")
            page.locator(".leaflet-tooltip", has_text="Start").wait_for()
            page.locator(".leaflet-tooltip", has_text="Finish").wait_for()
            page.wait_for_timeout(500)
            output = Path(__file__).resolve().parents[1] / "reports" / "track_t1"
            output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/"desktop.png"),full_page=True)
            # NiceGUI's map instance owns a single GeoJSON group with interactive sections.
            result = page.evaluate("""() => {
                const mapEl = document.querySelector('.leaflet-container');
                const map = getElement(Number(mapEl.id.slice(1))).map;
                let sections=0;
                map.eachLayer(layer => {if(layer.feature) sections++;});
                let group;
                map.eachLayer(layer => {if(layer.getLayers && layer.getLayers()[0]?.feature) group=layer;});
                const layers = group.getLayers();
                layers[0].openPopup();
                return {count:layers.length, popup:!!layers[0].getPopup(), color:layers[0].options.color};
            }""")
            assert result["count"] < 200
            assert result["popup"], result
            page.locator('.leaflet-popup-content').wait_for(state="visible")
            assert "Smoothed pace" in page.locator('.leaflet-popup-content').inner_text()
            page.set_viewport_size({"width":390,"height":844})
            page.get_by_role("button",name="Fit route",exact=True).click()
            page.wait_for_timeout(500)
            page.screenshot(path=str(output/"narrow.png"),full_page=True)
            assert page.locator('.leaflet-container').evaluate("e => e.getBoundingClientRect().width") <= 390
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors, errors
    assert "Traceback" not in server, server


def test_metric_switch_reuses_sections_and_preserves_selection(tmp_path):
    db = _test_database(tmp_path)
    conn = connect_sqlite(db)
    raw = points(120)
    conn.executemany(
        "INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,latitude,longitude,heart_rate_bpm,altitude_m,cadence) VALUES(202,?,?,?,?,?,?,?)",
        [(i,p["timestamp_utc"],p["lat_deg"],p["lon_deg"],None if i<30 else 120+i//3,20+i/3,None if i<60 else 170+i//10) for i,p in enumerate(raw)],
    )
    conn.commit()
    conn.close()
    process = _start_server(db, port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror",lambda e: errors.append(str(e)))
            page.route("https://**/*",lambda route: route.abort())
            page.goto(f"http://127.0.0.1:{port}/activities",wait_until="networkidle")
            assert _click_activity(page,202,errors)
            page.get_by_role("tab",name="Track",exact=True).click()
            page.locator(".leaflet-tooltip",has_text="Finish").wait_for()
            snapshot = """() => {
                const map = getElement(Number(document.querySelector('.leaflet-container').id.slice(1))).map;
                const layers=[];
                map.eachLayer(l => {if(l.feature) layers.push({id:l._leaflet_id,color:l.options.color});});
                return layers;
            }"""
            before = page.evaluate(snapshot)
            selector = page.get_by_label("Route metric",exact=True)
            for label, heading, key in [("Heart rate","Heart rate (bpm)","heart_rate"),("Elevation","Elevation (ft)","elevation"),("Cadence","Cadence (spm)","cadence")]:
                selector.click()
                page.get_by_role("option",name=label,exact=True).click()
                page.get_by_text(heading,exact=False).wait_for()
                page.wait_for_timeout(150)
                after = page.evaluate(snapshot)
                assert [s["id"] for s in after] == [s["id"] for s in before]
                assert any(s["color"] != old["color"] for s,old in zip(after,before))
            assert any(s["color"] == "#808892" for s in after)
            page.get_by_role("tab",name="Overview",exact=True).click()
            page.get_by_role("tab",name="Track",exact=True).click()
            assert selector.input_value() == "Cadence"
            page.wait_for_timeout(400)
            # Missing selected readings explain the gray section in the popup.
            page.evaluate("""() => {
                const map = getElement(Number(document.querySelector('.leaflet-container').id.slice(1))).map;
                let target;
                map.eachLayer(l => {if(!target && l.feature?.properties.overlays.cadence.missing) target=l;});
                target.openPopup();
            }""")
            page.locator('.leaflet-popup-content').wait_for()
            assert "unavailable for this section" in page.locator('.leaflet-popup-content').inner_text()
            page.wait_for_timeout(300)
            output = Path(__file__).resolve().parents[1]/"reports"/"track_t2"
            output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/"desktop.png"),full_page=True)
            page.set_viewport_size({"width":390,"height":844})
            page.get_by_role("button",name="Fit route",exact=True).click()
            page.wait_for_timeout(300)
            page.screenshot(path=str(output/"narrow.png"),full_page=True)
            assert page.locator('.leaflet-container').evaluate("e => e.getBoundingClientRect().width") <= 390
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors, errors
    assert "Traceback" not in server, server
