import pytest
from browser_test_support import wait_for_layout
from datetime import date, timedelta
from pathlib import Path
from playwright.sync_api import expect, sync_playwright
from test_activity_calendar_days import _database, _insert_activity
from test_activity_grid_browser import _start_server, _stop_server, _free_loopback_port
from garmin_data_hub.db.sqlite import connect_sqlite


@pytest.mark.browser
def test_chart_overview_filters_toggles_and_responsive_layout(tmp_path, browser_artifacts):
    db = _database(tmp_path)
    today = date.today()
    for i, offset in enumerate([0,7,14,35,91,110],1):
        day = (today-timedelta(days=offset)).isoformat()
        _insert_activity(db,i,local=day+"T12:00:00",gmt=day+"T12:00:00")
    conn = connect_sqlite(db)
    for i in range(1,7):
        conn.execute("INSERT INTO activity_metrics(activity_id,moving_time_s,tss,trimp,zone_1_s,zone_2_s,zone_3_s,zone_4_s,zone_5_s) VALUES(?,3000,40,60,0,2000,1000,0,0)",(i,))
    conn.execute("UPDATE activity SET activity_type='strength_training',training_stress_score=NULL WHERE activity_id=4")
    conn.execute("DELETE FROM activity_metrics WHERE activity_id=4")
    conn.commit()
    conn.close()
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror",lambda e: errors.append(str(e)))
            page.goto(f"http://127.0.0.1:{port}/charts",wait_until="networkidle")
            page.get_by_text("Weekly training volume",exact=True).wait_for()
            expect(page.locator('.js-plotly-plot')).to_have_count(3)
            page.get_by_text("Select one sport for comparable performance analysis.",exact=True).wait_for()
            def select(label,value):
                page.get_by_label(label,exact=True).click()
                page.get_by_role("option",name=value,exact=True).click()
                expect(page.get_by_label(label,exact=True)).to_have_value(value)
            select("Date range","4 weeks")
            assert page.get_by_label("Date range",exact=True).input_value() == "4 weeks"
            expect(page.get_by_label("Start date",exact=True)).to_have_value((today-timedelta(days=27)).isoformat())
            select("Sport","running")
            expect(page.locator('.js-plotly-plot')).to_have_count(4)
            select("Load source","TRIMP")
            page.get_by_text("Training load (TRIMP)",exact=True).wait_for()
            select("Intensity display","Percent")
            select("Volume metric","Distance")
            page.get_by_label("Start date",exact=True).fill((today-timedelta(days=60)).isoformat())
            page.get_by_label("Start date",exact=True).press("Tab")
            expect(page.get_by_label("Date range",exact=True)).to_have_value("Custom")
            page.get_by_label("End date",exact=True).fill((today-timedelta(days=1)).isoformat())
            page.get_by_label("End date",exact=True).press("Tab")
            wait_for_layout(page)
            output = browser_artifacts
            output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/"desktop.png"),full_page=True)
            page.set_viewport_size({"width":390,"height":844})
            wait_for_layout(page)
            page.screenshot(path=str(output/"narrow.png"),full_page=True)
            widths = page.locator('.js-plotly-plot').evaluate_all("els => els.map(e=>e.getBoundingClientRect().width)")
            assert widths and max(widths) <= 390
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.get_by_label("Start date",exact=True).fill(today.isoformat())
            page.get_by_label("Start date",exact=True).press("Tab")
            page.get_by_text("Start date must be on or before end date.",exact=True).wait_for()
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors, errors
    assert "Traceback" not in server, server
