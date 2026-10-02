from datetime import date, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright
from test_activity_calendar_days import _database, _insert_activity
from test_activity_grid_browser import _start_server, _stop_server, _free_loopback_port
from garmin_data_hub.db.sqlite import connect_sqlite


def test_chart_drilldown_explorer_and_tab_state(tmp_path):
    db = _database(tmp_path)
    today = date.today()
    for aid, offset in [(101,1),(202,1),(303,10),(404,110)]:
        day = (today-timedelta(days=offset)).isoformat()
        _insert_activity(db,aid,local=day+"T12:00:00",gmt=day+"T12:00:00")
    conn = connect_sqlite(db)
    conn.execute("ALTER TABLE activity ADD COLUMN activity_name TEXT")
    conn.execute("UPDATE activity SET activity_name='Morning run' WHERE activity_id=101")
    conn.execute("UPDATE activity SET activity_name='Evening run' WHERE activity_id=202")
    conn.execute("UPDATE activity SET average_speed=4, average_hr=155 WHERE activity_id=202")
    conn.execute("UPDATE activity SET training_stress_score=NULL WHERE activity_id=303")
    conn.executemany("INSERT INTO activity_metrics(activity_id,moving_time_s,tss,trimp,zone_1_s,zone_2_s,zone_3_s,zone_4_s,zone_5_s) VALUES(?,3000,40,60,0,2000,1000,0,0)",[(101,),(202,)])
    conn.commit()
    conn.close()
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror",lambda e: errors.append(str(e)))
            page.on("console",lambda e: errors.append(e.text) if e.type == "error" else None)
            base = f"http://127.0.0.1:{port}"
            def select(label,value):
                page.get_by_label(label,exact=True).click()
                page.get_by_role("option",name=value,exact=True).click()
                page.wait_for_timeout(250)
            def charts_ready():
                page.get_by_text("Weekly training volume",exact=True).wait_for()
                page.locator('.js-plotly-plot').last.locator('.scatterlayer .trace').first.locator('.point').first.wait_for()
            page.goto(base+"/charts",wait_until="networkidle")
            select("Date range","4 weeks")
            select("Sport","running")
            select("Load source","TRIMP")
            select("Intensity display","Percent")
            select("Volume metric","Distance")
            charts_ready()
            # Actual rendered point click: same day, different ID and speed.
            page.locator('.js-plotly-plot').last.locator('.scatterlayer .trace').first.locator('.point').nth(2).click(force=True)
            page.get_by_text("Activity 202",exact=True).wait_for()
            assert "activity_id=202" in page.url
            page.get_by_role("link",name="Return to Charts",exact=True).click()
            charts_ready()
            for label,value in [("Date range","4 weeks"),("Sport","running"),("Load source","TRIMP"),("Intensity display","Percent"),("Volume metric","Distance")]:
                assert page.get_by_label(label,exact=True).input_value() == value
            # A real bar click resolves the selected Monday week.
            page.locator('.js-plotly-plot').first.locator('.barlayer .trace').first.locator('.point path').last.click(force=True)
            page.get_by_role("button",name="Close contributors",exact=True).wait_for()
            dialog = page.get_by_role("dialog")
            assert dialog.get_by_role("link",name="Open activity 101",exact=True).count() == 1
            assert dialog.get_by_role("link",name="Open activity 202",exact=True).count() == 1
            assert dialog.get_by_role("link",name="Open activity 404",exact=True).count() == 0
            page.get_by_role("button",name="Close contributors",exact=True).click()
            # Keyboard-accessible source link and custom dates survive detail navigation.
            page.get_by_label("Start date",exact=True).fill((today-timedelta(days=60)).isoformat())
            page.get_by_label("Start date",exact=True).press("Tab")
            page.get_by_label("End date",exact=True).fill((today-timedelta(days=1)).isoformat())
            page.get_by_label("End date",exact=True).press("Tab")
            page.wait_for_timeout(300)
            page.get_by_text("Activity source table",exact=True).click()
            link = page.get_by_role("link",name="Open activity 101",exact=True)
            link.focus()
            link.press("Enter")
            page.get_by_text("Activity 101",exact=True).wait_for()
            page.get_by_role("link",name="Return to Charts",exact=True).click()
            charts_ready()
            assert page.get_by_label("Date range",exact=True).input_value() == "Custom"
            assert page.get_by_label("End date",exact=True).input_value() == (today-timedelta(days=1)).isoformat()
            select("Chart section","Explorer")
            assert page.locator('.js-plotly-plot').count() == 1
            page.get_by_label("Explorer charts",exact=True).click()
            page.get_by_role("option",name="Weekly training stress",exact=True).click()
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
            assert page.locator('.js-plotly-plot').count() == 2
            page.reload(wait_until="networkidle")
            page.get_by_text("Weekly training stress",exact=True).last.wait_for()
            assert page.get_by_label("Chart section",exact=True).input_value() == "Explorer"
            assert page.locator('.js-plotly-plot').count() == 2
            # Explorer filtered activity points retain identities.
            page.locator('.js-plotly-plot').first.locator('.scatterlayer .trace').first.locator('.point').nth(1).click(force=True)
            page.get_by_text("Activity 101",exact=True).wait_for()
            page.get_by_role("link",name="Return to Charts",exact=True).click()
            page.get_by_text("Weekly training stress",exact=True).last.wait_for()
            # Fresh tab receives defaults, not another tab's filters.
            other = browser.new_page()
            other.goto(base+"/charts",wait_until="networkidle")
            assert other.get_by_label("Chart section",exact=True).input_value() == "Overview"
            assert other.get_by_label("Sport",exact=True).input_value() == "All sports"
            other.goto(base+"/activities?activity_id=999999",wait_until="networkidle")
            other.get_by_text("That activity no longer exists.",exact=True).wait_for()
            other.close()
            page.get_by_role("link",name="Dashboard",exact=True).click()
            page.get_by_role("link",name="Charts",exact=True).click()
            page.get_by_text("Weekly training stress",exact=True).last.wait_for()
            assert page.get_by_label("Chart section",exact=True).input_value() == "Explorer"
            output = Path(__file__).resolve().parents[1]/"reports"/"charts_c2"
            output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/"desktop.png"),full_page=True)
            page.set_viewport_size({"width":390,"height":844})
            page.wait_for_timeout(300)
            page.screenshot(path=str(output/"narrow.png"),full_page=True)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            # Accessible weekly inspection includes absent metrics and empty weeks.
            missing_day = today-timedelta(days=10)
            monday = missing_day-timedelta(days=missing_day.weekday())
            select("Inspect week",f"{monday.isoformat()} · 1 activities")
            page.get_by_role("button",name="Show weekly contributors",exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_role("link",name="Open activity 303",exact=True).wait_for()
            assert dialog.get_by_role("link",name="Open activity 101",exact=True).count() == 0
            page.wait_for_timeout(300)
            page.screenshot(path=str(output/"contributors_narrow.png"),full_page=True)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.set_viewport_size({"width":1440,"height":1000})
            page.wait_for_timeout(200)
            page.screenshot(path=str(output/"contributors_desktop.png"),full_page=True)
            page.get_by_role("button",name="Close contributors",exact=True).click()
            first_day = today-timedelta(days=60)
            monday = first_day-timedelta(days=first_day.weekday())
            select("Inspect week",f"{monday.isoformat()} · 0 activities")
            page.get_by_role("button",name="Show weekly contributors",exact=True).click()
            page.get_by_text("No activities contributed in this selection.",exact=True).wait_for()
            page.get_by_role("button",name="Close contributors",exact=True).click()
            page.get_by_role("button",name="Reset zoom",exact=True).first.click()
            page.wait_for_timeout(250)
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors, errors
    assert "Traceback" not in server, server
