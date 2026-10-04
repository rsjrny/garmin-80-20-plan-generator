from dataclasses import replace
from datetime import date, timedelta
import sqlite3

import pytest
from playwright.sync_api import expect, sync_playwright
from browser_test_support import wait_for_layout
from test_activity_calendar_days import _database, _insert_activity
from test_activity_grid_browser import _start_server, _stop_server, _free_loopback_port
from revision_fixtures import _approve, _fitzgerald_candidate
from garmin_data_hub.plan_methodology.activity_workout_match import create_match, review_match
from garmin_data_hub.plan_methodology.domain import Sport, SegmentKind, LoadMode
from garmin_data_hub.plan_methodology.segments import WorkoutSegment


def comparison_database(tmp_path):
    db = _database(tmp_path)
    today = date.today()
    start = today-timedelta(days=today.weekday()+14)
    anchor = start+timedelta(days=2)
    candidate = _fitzgerald_candidate()
    template = candidate.workouts[0]
    workouts = []
    for i in range((today-anchor).days+1):
        row = replace(template,scheduled_date=anchor+timedelta(days=i),workout_id=f"w{i}",ordinal=i,title=f"Scheduled run {i}")
        if i == 1:
            row = replace(row,sport=Sport.REST,family="REST",purpose="RECOVERY",title="Scheduled rest",
                          segments=(WorkoutSegment(SegmentKind.NON_TRAINING,LoadMode.OPEN),))
        workouts.append(row)
    _approve(db,replace(candidate,workouts=tuple(workouts)))
    for aid,offset in [(101,1),(102,2),(103,3)]:
        day = anchor+timedelta(days=offset)
        _insert_activity(db,aid,local=day.isoformat()+"T12:00:00",gmt=day.isoformat()+"T12:00:00")
    conn = sqlite3.connect(db)
    conn.execute("UPDATE activity SET training_stress_score=NULL WHERE activity_id=103")
    create_match(conn,revision_id="rev-a",workout_id="w0",activity_id=101,status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="Moved run")
    pending = create_match(conn,revision_id="rev-a",workout_id="w2",activity_id=102,status="CANDIDATE",source="RECONCILIATION",confidence="LOW")
    conn.commit(); conn.close()
    return db,start,anchor,pending.activity_workout_match_id


@pytest.mark.browser
def test_plan_comparison_drilldown_filters_refresh_and_responsive(tmp_path,browser_artifacts):
    db,start,anchor,pending = comparison_database(tmp_path)
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport=dict(width=1440,height=1000))
            page.on("pageerror",lambda error:errors.append(str(error)))
            page.on("console",lambda message:errors.append(message.text) if message.type == "error" else None)
            def close_drawer():
                backdrop = page.locator(".q-drawer__backdrop")
                if page.viewport_size["width"] == 390 and backdrop.is_visible():
                    backdrop.click(position={"x":350,"y":120})
            def select(label,value):
                close_drawer()
                page.get_by_label(label,exact=True).locator("xpath=ancestor::label[1]").click()
                page.get_by_role("option",name=value,exact=True).click()
                expect(page.get_by_label(label,exact=True)).to_have_value(value)
            page.goto(f"http://127.0.0.1:{port}/charts",wait_until="networkidle")
            select("Date range","4 weeks")
            select("Chart section","Plan comparison")
            page.get_by_text("Weekly planned versus completed",exact=True).wait_for()
            expect(page.locator(".js-plotly-plot")).to_have_count(3)
            select("Compare plan","plan-1")
            select("Week alignment","Plan weeks")
            # Week clicks use plan boundaries, not the calendar-week contributor path.
            page.wait_for_function("document.querySelector('.js-plotly-plot')?.data?.[0]?.x?.includes('Plan week 1')")
            bars = page.locator(".js-plotly-plot").first.locator(".barlayer .trace").first.locator(".point path")
            visible_index = bars.evaluate_all("nodes => nodes.findIndex(node => node.getBoundingClientRect().height > 2)")
            bars.nth(visible_index).click(force=True)
            dialog = page.get_by_role("dialog")
            dialog.get_by_text("Confirmed substitution",exact=True).wait_for()
            dialog.get_by_text("Scheduled rest",exact=True).wait_for()
            dialog.get_by_text("Candidate awaiting review",exact=True).wait_for()
            dialog.get_by_role("link",name="Open activity 101",exact=True).first.wait_for()
            browser_artifacts.mkdir(parents=True,exist_ok=True)
            wait_for_layout(page)
            page.screenshot(path=str(browser_artifacts/"evidence_desktop.png"))
            page.get_by_role("button",name="Close plan week",exact=True).click()
            page.set_viewport_size(dict(width=390,height=844))
            page.reload(wait_until="networkidle")
            wait_for_layout(page)
            select("Inspect plan week","Plan week 1")
            page.get_by_role("button",name="Show plan week evidence",exact=True).click()
            page.get_by_role("dialog").get_by_text("Confirmed substitution",exact=True).wait_for()
            wait_for_layout(page)
            page.screenshot(path=str(browser_artifacts/"evidence_narrow.png"))
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.get_by_role("button",name="Close plan week",exact=True).click()
            page.reload(wait_until="networkidle")
            expect(page.get_by_label("Chart section",exact=True)).to_have_value("Plan comparison")
            expect(page.get_by_label("Compare plan",exact=True)).to_have_value("plan-1")
            expect(page.get_by_label("Week alignment",exact=True)).to_have_value("Plan weeks")
            select("Inspect plan week","Plan week 1")
            page.get_by_role("button",name="Show plan week evidence",exact=True).click()
            # Accessible activity navigation and Charts return retain selection.
            link = page.get_by_role("dialog").get_by_role("link",name="Open activity 101",exact=True).first
            link.focus(); link.press("Enter")
            page.get_by_text("Activity 101",exact=True).wait_for()
            close_drawer()
            page.get_by_role("link",name="Return to Charts",exact=True).click()
            page.get_by_text("Weekly planned versus completed",exact=True).wait_for()
            close_drawer()
            expect(page.get_by_label("Week alignment",exact=True)).to_have_value("Plan weeks")
            # Explicit refresh reads new confirmed-match evidence.
            conn = sqlite3.connect(db)
            review_match(conn,activity_workout_match_id=pending,status="CONFIRMED",reviewer="r",reason="Reviewed")
            conn.commit(); conn.close()
            old_week_id = page.get_by_label("Inspect plan week",exact=True).get_attribute("id")
            page.get_by_role("button",name="Refresh data",exact=True).click()
            page.locator(f'[id="{old_week_id}"]').wait_for(state="detached")
            select("Inspect plan week","Plan week 1")
            page.get_by_role("button",name="Show plan week evidence",exact=True).click()
            page.get_by_role("dialog").get_by_text("Confirmed activity",exact=True).wait_for()
            assert page.get_by_role("dialog").get_by_text("Candidate awaiting review",exact=True).count() == 0
            page.get_by_role("button",name="Close plan week",exact=True).click()
            select("Week alignment","Calendar weeks")
            select("Sport","running")
            select("Volume metric","Distance")
            select("Load source","TRIMP")
            page.get_by_text("Canonical plans do not currently prescribe TRIMP",exact=False).wait_for()
            page.get_by_text("Plan variance and coverage table",exact=True).click()
            expect(page.get_by_role("button",name="Download plan comparison CSV",exact=True)).to_be_visible()
            with page.expect_download() as download:
                page.get_by_role("button",name="Download plan comparison CSV",exact=True).click()
            assert download.value.suggested_filename == "plan-comparison.csv"
            select("Volume metric","Time")
            page.wait_for_function("document.querySelector('.js-plotly-plot')?._fullLayout?.yaxis?.title?.text === 'hours'")
            assert page.locator(".js-plotly-plot").first.locator(".barlayer .trace").first.locator(".point path").evaluate_all("nodes => nodes.some(node => node.getBoundingClientRect().height > 2)")
            page.get_by_text("Plan variance and coverage table",exact=True).click()
            page.evaluate("window.scrollTo(0,0)")
            wait_for_layout(page)
            page.screenshot(path=str(browser_artifacts/"narrow.png"),full_page=True)
            page.set_viewport_size(dict(width=1440,height=1000))
            page.evaluate("window.scrollTo(0,0)")
            wait_for_layout(page)
            page.screenshot(path=str(browser_artifacts/"desktop.png"),full_page=True)
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors,errors
    assert "Traceback" not in server,server


@pytest.mark.browser
def test_empty_legacy_and_corrupt_schedule_explain_unavailable_comparison(tmp_path):
    db = _database(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name,planned_duration_s) VALUES(?,?,1800)",(date.today().isoformat(),"Legacy run"))
    conn.commit(); conn.close()
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport=dict(width=1440,height=1000))
            page.on("pageerror",lambda error:errors.append(str(error)))
            def select(label,value):
                page.get_by_label(label,exact=True).click()
                page.get_by_role("option",name=value,exact=True).click()
            page.goto(f"http://127.0.0.1:{port}/charts",wait_until="networkidle")
            select("Chart section","Plan comparison")
            page.get_by_text("No active plan is available for comparison.",exact=False).wait_for()
            page.get_by_text("1 legacy/unmanaged schedule rows excluded",exact=False).wait_for()
            select("Week alignment","Plan weeks")
            page.get_by_text("Plan comparison unavailable: Select one active plan to use plan weeks.",exact=True).wait_for()
            select("Week alignment","Calendar weeks")
            page.get_by_text("Weekly planned versus completed",exact=True).wait_for()
            candidate = _fitzgerald_candidate()
            candidate = replace(candidate,workouts=(replace(candidate.workouts[0],scheduled_date=date.today()),))
            _approve(db,candidate)
            old_week_id = page.get_by_label("Inspect plan week",exact=True).get_attribute("id")
            page.get_by_role("button",name="Refresh data",exact=True).click()
            page.locator(f'[id="{old_week_id}"]').wait_for(state="detached")
            old_week_id = page.get_by_label("Inspect plan week",exact=True).get_attribute("id")
            select("Compare plan","plan-1")
            page.locator(f'[id="{old_week_id}"]').wait_for(state="detached")
            page.get_by_text("Scheduled workout evidence",exact=True).click()
            page.get_by_text("Foundation Run rev-a",exact=True).wait_for()
            conn = sqlite3.connect(db)
            conn.execute("UPDATE planned_workout SET scheduled_date=? WHERE source_revision_id='rev-a'",((date.today()-timedelta(days=1)).isoformat(),))
            conn.commit(); conn.close()
            page.get_by_role("button",name="Refresh data",exact=True).click()
            page.get_by_text("Plan comparison unavailable: Active schedule date/identity differs",exact=False).wait_for()
            page.evaluate("window.socket?.disconnect()")
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors,errors
    assert "Traceback" not in server,server
