import pytest
from browser_test_support import wait_for_layout
from dataclasses import replace
from pathlib import Path
from season_browser_logs import application_logs

from playwright.sync_api import expect, sync_playwright

from garmin_data_hub.services import season_plans as intent, season_generation as generation
from garmin_data_hub.services.season_schedule import GenerationSettings
from garmin_data_hub.plan_methodology.revision_repository import load_revision
from test_activity_grid_browser import _test_database, _start_server, _stop_server, _free_loopback_port


@pytest.mark.browser
def test_protection_manual_preview_affected_diff_stale_rejection_and_apply(tmp_path, browser_artifacts):
    db=_test_database(tmp_path)
    season=intent.create_season(db,intent.SeasonDraft("Safe regeneration","2027-01-01","2027-12-31","America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800,lthr=170)))
    season_event=intent.save_event(db,season.season_id,intent.EventDraft("Spring Half","2027-05-16"),expected_version=1)
    initial=generation.preview_season(db,season.season_id,GenerationSettings(lthr_confirmed=True))
    generation.apply_season_preview(db,initial,approved_by="TEST",acknowledge_warnings=True)
    season=intent.get_season(db,season.season_id)
    selected=next(w for w in initial.schedule.candidate.workouts if w.sport.value=="RUNNING" and not w.event_flag)
    process=_start_server(db,port:=_free_loopback_port())
    output = browser_artifacts
    output.mkdir(parents=True,exist_ok=True)
    errors=[]
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True)
            page=browser.new_page(viewport={"width":1440,"height":1000})
            page.on("pageerror",lambda e:errors.append(str(e)))
            page.set_default_timeout(15000)
            page.goto(f"http://127.0.0.1:{port}/seasons?season_id={season.season_id}",wait_until="networkidle")
            page.get_by_role("button",name="Workout protection",exact=True).click()
            dialog=page.get_by_role("dialog")
            dialog.get_by_label("Season workout",exact=True).fill(selected.scheduled_date.isoformat())
            page.get_by_role("option",name=f"{selected.scheduled_date.isoformat()} · {selected.title}",exact=True).click()
            dialog.get_by_text("Keep this workout locked",exact=True).click()
            dialog.get_by_label("Protection reason",exact=True).fill("Keep my preferred session")
            dialog.get_by_role("button",name="Save workout protection",exact=True).click()
            page.get_by_text("Workout protection saved.",exact=True).wait_for()
            page.get_by_role("button",name="Workout protection",exact=True).click()
            dialog=page.get_by_role("dialog")
            dialog.get_by_label("Season workout",exact=True).fill(selected.scheduled_date.isoformat())
            page.get_by_role("option",name=f"{selected.scheduled_date.isoformat()} · {selected.title}",exact=True).click()
            dialog.get_by_label("Edited session title",exact=True).fill("My preserved easy run")
            dialog.get_by_text("I authorize editing this locked or manual future session",exact=True).click()
            dialog.get_by_role("button",name="Preview prescription edit",exact=True).click()
            page.get_by_text("Season schedule preview",exact=True).wait_for()
            dialog=page.get_by_role("dialog")
            expect(dialog.get_by_role("button",name="Apply reviewed season",exact=True)).to_be_enabled()
            expect(page.locator(".q-notification")).to_have_count(0,timeout=15000)
            wait_for_layout(page)
            page.screenshot(path=str(output/"manual_preview_desktop.png"))
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).click()
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            page.get_by_text("Season schedule applied.",exact=True).wait_for()
            season=intent.get_season(db,season.season_id)
            e=intent.list_events(db,season.season_id)[0]
            intent.save_event(db,season.season_id,replace(e.draft,event_date="2027-06-06"),event_id=e.event_id,expected_version=season.input_version)
            refreshed=intent.get_season(db,season.season_id)
            page.get_by_role("button",name="Refresh seasons",exact=True).click()
            expect(page.get_by_text(f"Planning version {refreshed.input_version} · Linked revision {refreshed.current_revision_id}",exact=True)).to_be_visible()
            page.get_by_role("button",name="Preview yearly schedule",exact=True).click()
            dialog=page.get_by_role("dialog")
            page.get_by_text("Generate season preview",exact=True).wait_for()
            expect(page.locator(".q-notification")).to_have_count(0,timeout=15000)
            wait_for_layout(page)
            page.screenshot(path=str(output/"regeneration_settings_desktop.png"))
            dialog.get_by_role("button",name="Generate preview",exact=True).click()
            page.get_by_text("Season schedule preview",exact=True).wait_for()
            dialog=page.get_by_role("dialog")
            dialog.get_by_text("Workout changes and preservation",exact=True).wait_for()
            expect(dialog.get_by_role("button",name="Apply reviewed season",exact=True)).to_be_enabled()
            wait_for_layout(page)
            page.screenshot(path=str(output/"affected_preview_desktop.png"))
            page.set_viewport_size({"width":390,"height":844})
            wait_for_layout(page)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(output/"affected_preview_narrow.png"))
            # A changed operational lock is invisible to immutable plan hashes but must stale the preview.
            from garmin_data_hub.services.season_workouts import set_workout_protection
            current=intent.get_season(db,season.season_id)
            active=load_revision(db,current.current_revision_id).candidate
            target=active.workouts[10]
            set_workout_protection(db,season.season_id,target.workout_id,expected_version=0,editor="r",reason="new lock",locked=True)
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).click()
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            dialog.get_by_text("Season inputs, schedule, completion evidence, policy or local date changed. Generate a fresh preview.",exact=True).wait_for()
            dialog.get_by_role("button",name="Close preview",exact=True).click()
            # Close the phone shell drawer if it is open.
            backdrop=page.locator(".q-drawer__backdrop")
            if backdrop.is_visible():
                backdrop.click(position={"x":350,"y":120})
            page.get_by_role("button",name="Preview yearly schedule",exact=True).click()
            page.get_by_role("dialog").get_by_role("button",name="Generate preview",exact=True).click()
            page.get_by_text("Season schedule preview",exact=True).wait_for()
            dialog=page.get_by_role("dialog")
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.",exact=True).click()
            page.screenshot(path=str(output/"review_apply_narrow.png"))
            dialog.get_by_role("button",name="Apply reviewed season",exact=True).click()
            page.get_by_text("Season schedule applied.",exact=True).wait_for()
            assert len(generation.list_applications(db,season.season_id))==3
            current=intent.get_season(db,season.season_id)
            final=load_revision(db,current.current_revision_id).candidate
            assert any(w.title=="My preserved easy run" for w in final.workouts)
            assert [w.scheduled_date.isoformat() for w in final.workouts if w.event_flag]==["2027-06-06"]
            page.reload(wait_until="networkidle")
            backdrop=page.locator(".q-drawer__backdrop")
            if backdrop.is_visible():
                backdrop.click(position={"x":350,"y":120})
            page.get_by_text("Applied season revisions",exact=True).click()
            page.get_by_text("Recorded preservation and overrides",exact=True).first.wait_for()
            page.get_by_text("Recorded preservation and overrides",exact=True).last.click()
            wait_for_layout(page)
            page.get_by_text("My preserved easy run",exact=False).last.scroll_into_view_if_needed()
            wait_for_layout(page)
            page.screenshot(path=str(output/"history_narrow.png"))
            page.evaluate("window.socket?.disconnect()")
            wait_for_layout(page)
            page.set_viewport_size({"width":1440,"height":1000})
            page.goto(f"http://127.0.0.1:{port}/plan",wait_until="networkidle")
            page.get_by_text("My preserved easy run",exact=True).first.wait_for()
            page.locator(".ag-body-horizontal-scroll-viewport").first.evaluate("e => e.scrollLeft=e.scrollWidth")
            page.get_by_text("Manual · Preserved",exact=True).first.wait_for()
            page.get_by_text("Manual · Preserved",exact=True).first.scroll_into_view_if_needed()
            wait_for_layout(page)
            page.screenshot(path=str(output/"calendar_protection_desktop.png"))
            assert errors==[]
            page.evaluate("window.socket?.disconnect()")
            wait_for_layout(page)
            browser.close()
    finally:
        logs=_stop_server(process)
        (output/"browser-server.txt").write_text(logs,encoding="utf-8")
    assert "Traceback" not in application_logs(logs) and "ERROR:" not in application_logs(logs)
