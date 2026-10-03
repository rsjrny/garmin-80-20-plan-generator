from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from season_browser_logs import application_logs
from garmin_data_hub.services import season_plans as intent, season_generation as generation
from garmin_data_hub.services.plan_persistence import get_active_plan_sha256
from test_activity_grid_browser import _test_database, _start_server, _stop_server, _free_loopback_port


OUTPUT = Path(__file__).resolve().parents[1] / "reports/yearly_y4"


def capture(page, name):
    page.wait_for_timeout(450)
    page.screenshot(path=str(OUTPUT / name))


def test_conflict_tuning_effective_windows_and_reviewed_apply(tmp_path):
    db = _test_database(tmp_path)
    season = intent.create_season(db, intent.SeasonDraft("Y4 season", "2027-01-01", "2027-12-31", "America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800, lthr=170)))
    intent.save_event(db, season.season_id, intent.EventDraft("Primary", "2027-10-17", "MAR"), expected_version=1)
    later = intent.save_event(db, season.season_id, intent.EventDraft("Second peak", "2027-10-24", "MAR"), expected_version=2)
    original = get_active_plan_sha256(db)
    process = _start_server(db, port := _free_loopback_port())
    errors = []
    OUTPUT.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("console", lambda e: errors.append(e.text) if e.type == "error" else None)
            page.set_default_timeout(15000)
            page.goto(f"http://127.0.0.1:{port}/seasons?season_id={season.season_id}", wait_until="networkidle")

            def generate():
                page.get_by_role("button", name="Preview yearly schedule", exact=True).click()
                dialog = page.get_by_role("dialog")
                dialog.get_by_label("I confirm this LTHR is a measured running threshold.", exact=True).click()
                dialog.get_by_role("button", name="Generate preview", exact=True).click()
                page.get_by_text("Season schedule preview", exact=True).wait_for()
                return page.get_by_role("dialog")

            dialog = generate()
            expect(dialog.get_by_role("button", name="Apply reviewed season", exact=True)).to_be_disabled()
            dialog.get_by_text("Changing priority does not shorten required recovery.", exact=True).wait_for()
            capture(page, "conflicts_desktop.png")
            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(250)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            capture(page, "conflicts_narrow.png")
            dialog.get_by_role("button", name=f"Tune event {later.event_id}", exact=True).click()
            page.get_by_text("Edit event", exact=True).wait_for()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("Event date", exact=True).fill("2027-11-21")
            dialog.get_by_text("Effective window:", exact=False).filter(has_text="2027-12-05").wait_for()
            capture(page, "tuning_narrow.png")
            dialog.get_by_text("Effective window:", exact=False).scroll_into_view_if_needed()
            capture(page, "tuning_window_narrow.png")
            dialog.get_by_role("button", name="Save event", exact=True).click()
            page.get_by_text("Event saved.", exact=True).wait_for()
            assert get_active_plan_sha256(db) == original
            assert intent.list_events(db, season.season_id)[1].draft.event_date == "2027-11-21"
            # Close the responsive shell drawer if it covers the page.
            backdrop = page.locator(".q-drawer__backdrop")
            if backdrop.is_visible():
                backdrop.click(position={"x": 350, "y": 120})
            dialog = generate()
            expect(dialog.get_by_role("button", name="Apply reviewed season", exact=True)).to_be_enabled()
            dialog.get_by_text("Event preparation and tuning", exact=True).scroll_into_view_if_needed()
            capture(page, "preparation_narrow.png")
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.", exact=True).scroll_into_view_if_needed()
            capture(page, "apply_narrow.png")
            dialog.get_by_label("I reviewed the schedule and acknowledge all warnings.", exact=True).click()
            dialog.get_by_role("button", name="Apply reviewed season", exact=True).click()
            page.get_by_text("Season schedule applied.", exact=True).wait_for()
            applications = generation.list_applications(db, season.season_id)
            assert len(applications) == 1
            page.reload(wait_until="networkidle")
            backdrop = page.locator(".q-drawer__backdrop")
            if backdrop.is_visible():
                backdrop.click(position={"x": 350, "y": 120})
            page.get_by_text("Applied season revisions", exact=True).click()
            page.get_by_text("Recorded event preparation", exact=True).click()
            page.get_by_text("Second peak ·", exact=False).wait_for()
            capture(page, "history_narrow.png")
            assert errors == []
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server_output = _stop_server(process)
        (OUTPUT / "tuning-browser-server.txt").write_text(server_output, encoding="utf-8")
    assert "Traceback" not in application_logs(server_output)
    assert "ERROR:" not in application_logs(server_output)


def test_completion_duration_editor_enables_easy_taper_participation(tmp_path):
    db = _test_database(tmp_path)
    season = intent.create_season(db, intent.SeasonDraft("Easy taper participation", "2027-01-01", "2027-12-31", "America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800, lthr=170)))
    intent.save_event(db, season.season_id, intent.EventDraft("Primary", "2027-10-17", "MAR"), expected_version=1)
    easy = intent.save_event(db, season.season_id, intent.EventDraft("Easy 5K", "2027-10-10", "5K", "C"), expected_version=2)
    process = _start_server(db, port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(15000)
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(f"http://127.0.0.1:{port}/seasons?season_id={season.season_id}", wait_until="networkidle")
            page.get_by_role("button", name=f"Edit event {easy.event_id}", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("Planned completion minutes (optional)", exact=True).fill("10")
            dialog.get_by_text("C events replace training;", exact=False).wait_for()
            capture(page, "completion_editor_desktop.png")
            dialog.get_by_role("button", name="Save event", exact=True).scroll_into_view_if_needed()
            capture(page, "completion_controls_desktop.png")
            dialog.get_by_role("button", name="Save event", exact=True).click()
            page.get_by_text("Event saved.", exact=True).wait_for()
            assert intent.list_events(db, season.season_id)[0].draft.participation_seconds == 600
            page.get_by_role("button", name="Preview yearly schedule", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("I confirm this LTHR is a measured running threshold.", exact=True).click()
            dialog.get_by_role("button", name="Generate preview", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_text("Easy 5K replaces an easy taper session", exact=False).wait_for()
            expect(dialog.get_by_role("button", name="Apply reviewed season", exact=True)).to_be_enabled()
            capture(page, "easy_taper_desktop.png")
            assert errors == []
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server_output = _stop_server(process)
        (OUTPUT / "completion-browser-server.txt").write_text(server_output, encoding="utf-8")
    assert "Traceback" not in application_logs(server_output)
    assert "ERROR:" not in application_logs(server_output)
