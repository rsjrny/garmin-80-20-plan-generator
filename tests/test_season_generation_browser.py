from pathlib import Path
from season_browser_logs import application_logs

from playwright.sync_api import expect, sync_playwright

from garmin_data_hub.services import season_plans as intent, season_generation as generation
from garmin_data_hub.services.plan_persistence import get_active_plan_sha256
from test_activity_grid_browser import _test_database, _start_server, _stop_server, _free_loopback_port


def test_yearly_preview_warning_review_stale_and_initial_apply(tmp_path):
    db=_test_database(tmp_path)
    season=intent.create_season(db,intent.SeasonDraft("2027 Running Season","2027-01-01","2027-12-31","America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800,lthr=170)))
    intent.save_event(db,season.season_id,intent.EventDraft("Spring Half","2027-05-16"),expected_version=1)
    original=get_active_plan_sha256(db)
    process=_start_server(db,port:=_free_loopback_port())
    errors=[]
    output=Path(__file__).resolve().parents[1]/"reports/yearly_y3/compat_y2"
    output.mkdir(parents=True,exist_ok=True)
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True)
            page=browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror",lambda e:errors.append(str(e)))
            page.on("console",lambda e:errors.append(e.text) if e.type=="error" else None)
            page.set_default_timeout(10000)
            base=f"http://127.0.0.1:{port}"
            page.goto(base+f"/seasons?season_id={season.season_id}",wait_until="networkidle")

            def generate(linked=False):
                page.get_by_role("button",name="Preview yearly schedule",exact=True).click()
                dialog=page.get_by_role("dialog")
                dialog.get_by_role("button",name="Generate preview",exact=True).click()
                if not linked:
                    dialog.get_by_text("80/20 generation requires LTHR and explicit confirmation that it is a measured running threshold.",exact=True).wait_for()
                    dialog.get_by_label("I confirm this LTHR is a measured running threshold.",exact=True).click()
                    dialog.get_by_role("button",name="Generate preview",exact=True).click()
                page.get_by_text("Season schedule preview",exact=True).wait_for()
                return page.get_by_role("dialog")

            dialog=generate()
            assert get_active_plan_sha256(db)==original
            expect(dialog.get_by_role("button",name="Apply reviewed season",exact=True)).to_be_enabled()
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            dialog.get_by_text("Review and acknowledge the schedule warnings before applying.",exact=True).wait_for()
            assert get_active_plan_sha256(db)==original
            # Reviewed inputs change while the modal remains open.
            current=intent.get_season(db,season.season_id)
            intent.save_event(db,season.season_id,intent.EventDraft("Fall Marathon","2027-10-17","MAR"),expected_version=current.input_version)
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).click()
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            dialog.get_by_text("Season inputs, schedule, completion evidence, policy or local date changed. Generate a fresh preview.",exact=True).wait_for()
            assert get_active_plan_sha256(db)==original
            dialog.get_by_role("button",name="Close preview",exact=True).click()
            page.get_by_role("button",name="Refresh seasons",exact=True).click()
            dialog=generate()
            expect(page.locator(".q-notification")).to_have_count(0,timeout=15000)
            page.screenshot(path=str(output/"preview_desktop.png"))
            page.set_viewport_size({"width":390,"height":844})
            page.wait_for_timeout(250)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(output/"preview_narrow.png"))
            dialog.get_by_text("Local Monday–Sunday workload",exact=True).scroll_into_view_if_needed()
            page.screenshot(path=str(output/"workload_narrow.png"))
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).scroll_into_view_if_needed()
            page.screenshot(path=str(output/"apply_narrow.png"))
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).click()
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            page.get_by_text("Season schedule applied.",exact=True).wait_for()
            active=intent.get_season(db,season.season_id)
            assert active.current_revision_id
            assert len(generation.list_applications(db,season.season_id))==1
            page.reload(wait_until="networkidle")
            page.locator(".q-drawer__backdrop").click(position={"x":350,"y":120})
            page.get_by_text("Applied season revisions",exact=True).click()
            page.get_by_text("INITIAL_FULL",exact=False).wait_for()
            page.evaluate("window.scrollTo(0,0)")
            page.wait_for_timeout(250)
            page.screenshot(path=str(output/"applied_narrow.png"),full_page=True)
            # An existing season now supports a reviewed protected regeneration.
            dialog=generate(linked=True)
            expect(dialog.get_by_role("button",name="Apply reviewed season",exact=True)).to_be_enabled()
            dialog.get_by_text("Workout changes and preservation",exact=True).wait_for()
            dialog.get_by_role("button",name="Close preview",exact=True).click()
            # The active calendar reads canonical rest/auxiliary/native intensity rows.
            page.goto(base+"/plan",wait_until="networkidle")
            page.locator(".q-drawer__backdrop").click(position={"x":350,"y":120})
            page.get_by_text("Easy aerobic run",exact=False).first.wait_for()
            page.screenshot(path=str(output/"calendar_narrow.png"))
            assert errors==[]
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server_output=_stop_server(process)
        (output/"browser-server.txt").write_text(server_output,encoding="utf-8")
    assert "Traceback" not in application_logs(server_output) and "ERROR:" not in application_logs(server_output)


def test_incompatible_a_peaks_have_visible_conflicts_and_disabled_apply(tmp_path):
    db=_test_database(tmp_path)
    season=intent.create_season(db,intent.SeasonDraft("Conflicting peaks","2027-01-01","2027-12-31","America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800,lthr=170)))
    intent.save_event(db,season.season_id,intent.EventDraft("First marathon","2027-10-17","MAR"),expected_version=1)
    intent.save_event(db,season.season_id,intent.EventDraft("Second marathon","2027-10-24","MAR"),expected_version=2)
    original=get_active_plan_sha256(db)
    process=_start_server(db,port:=_free_loopback_port())
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True)
            page=browser.new_page(viewport={"width":390,"height":844})
            page.set_default_timeout(10000)
            page.goto(f"http://127.0.0.1:{port}/seasons",wait_until="networkidle")
            page.locator(".q-drawer__backdrop").click(position={"x":350,"y":120})
            page.get_by_role("button",name="Preview yearly schedule",exact=True).click()
            dialog=page.get_by_role("dialog")
            dialog.get_by_label("I confirm this LTHR is a measured running threshold.",exact=True).click()
            dialog.get_by_role("button",name="Generate preview",exact=True).click()
            dialog=page.get_by_role("dialog")
            dialog.get_by_text("First marathon recovery overlaps Second marathon taper. Separate peaks are infeasible; downgrade, move or cancel one.",exact=False).wait_for()
            expect(dialog.get_by_role("button",name="Apply reviewed season",exact=True)).to_be_disabled()
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(Path(__file__).resolve().parents[1]/"reports/yearly_y3/compat_y2/conflicts_narrow.png"))
            assert get_active_plan_sha256(db)==original
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server_output=_stop_server(process)
    assert "Traceback" not in application_logs(server_output)
