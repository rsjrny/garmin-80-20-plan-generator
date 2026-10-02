from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from garmin_data_hub.services.plan_persistence import get_active_plan_sha256
from garmin_data_hub.services.season_plans import list_seasons, list_events
from test_activity_grid_browser import _test_database, _start_server, _stop_server, _free_loopback_port
from test_plan_revision_persistence import _fitzgerald_candidate, _approve


def test_season_calendar_edit_link_and_stale_editor(tmp_path):
    db = _test_database(tmp_path)
    candidate = _fitzgerald_candidate()
    _approve(db, candidate)
    original_hash = get_active_plan_sha256(db)
    process = _start_server(db, port := _free_loopback_port())
    errors = []
    output = Path(__file__).resolve().parents[1] / "reports/yearly_y1"
    output.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("console", lambda e: errors.append(e.text) if e.type == "error" else None)
            base = f"http://127.0.0.1:{port}"
            page.goto(base + "/seasons", wait_until="networkidle")
            page.get_by_role("button", name="New season", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("Season name", exact=True).fill("2026 Race Calendar")
            dialog.get_by_label("Season start", exact=True).fill("2026-01-01")
            dialog.get_by_label("Season end", exact=True).fill("2026-12-31")
            dialog.get_by_role("button", name="Save season", exact=True).click()
            page.get_by_text("No events yet.", exact=True).wait_for()

            def select(dialog, label, option):
                dialog.get_by_label(label, exact=True).click()
                page.get_by_role("option", name=option, exact=True).click()

            page.get_by_role("button", name="Add event", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("Event name", exact=True).fill("Spring Half")
            dialog.get_by_label("Event date", exact=True).fill("2026-05-16")
            select(dialog, "Event priority", "B · Supporting event")
            select(dialog, "Event goal", "Performance")
            dialog.get_by_label("Target finish time (HH:MM:SS)", exact=True).fill("01:45:00")
            dialog.get_by_role("button", name="Save event", exact=True).click()
            page.get_by_text("Target finish: 01:45:00", exact=True).wait_for()

            page.get_by_role("button", name="Add event", exact=True).click()
            dialog = page.get_by_role("dialog")
            dialog.get_by_label("Event name", exact=True).fill("Fall Marathon")
            dialog.get_by_label("Event date", exact=True).fill("2026-05-16")
            dialog.get_by_role("button", name="Save event", exact=True).click()
            dialog.get_by_text("Another active event is scheduled on that date. Move or cancel it first.", exact=True).wait_for()
            dialog.get_by_label("Event date", exact=True).fill("2026-10-17")
            select(dialog, "Event distance", "Marathon")
            select(dialog, "Event type", "Trail")
            dialog.get_by_label("Terrain", exact=True).fill("Rolling hills")
            dialog.get_by_label("Course notes", exact=True).fill("A steady climb through the first half followed by a rolling descent.")
            dialog.get_by_role("button", name="Save event", exact=True).click()
            page.get_by_text("Fall Marathon", exact=True).wait_for()
            page.reload(wait_until="networkidle")
            page.get_by_text("Spring Half", exact=True).wait_for()
            season = list_seasons(db)[0]
            events = list_events(db, season.season_id)
            assert [e.draft.priority for e in events] == ["B", "A"]
            assert events[0].draft.target_seconds == 6300

            # Concurrent editor keeps its captured version and cannot overwrite the second tab.
            page.get_by_role("button", name=f"Edit event {events[0].event_id}", exact=True).click()
            first_dialog = page.get_by_role("dialog")
            first_dialog.get_by_label("Event name", exact=True).fill("Stale name")
            other = browser.new_page()
            other.goto(base + f"/seasons?season_id={season.season_id}", wait_until="networkidle")
            other.get_by_role("button", name=f"Edit event {events[0].event_id}", exact=True).click()
            second_dialog = other.get_by_role("dialog")
            second_dialog.get_by_label("Event name", exact=True).fill("Spring Tune-up")
            second_dialog.get_by_role("button", name="Save event", exact=True).click()
            other.get_by_text("Spring Tune-up", exact=True).wait_for()
            first_dialog.get_by_role("button", name="Save event", exact=True).click()
            first_dialog.get_by_text("Season changed in another editor. Refresh before saving.", exact=True).wait_for()
            first_dialog.get_by_role("button", name="Close event editor", exact=True).click()
            page.get_by_role("button", name="Refresh seasons", exact=True).click()
            page.get_by_text("Spring Tune-up", exact=True).wait_for()
            other.evaluate("window.socket?.disconnect()")
            other.wait_for_timeout(250)
            other.close()
            assert list_events(db, season.season_id)[0].draft.name == "Spring Tune-up"

            # Explicit plan linking retains all workout content.
            page.get_by_text("Link an existing plan", exact=True).click()
            page.get_by_label("Existing plan", exact=True).click()
            page.get_by_role("option", name="plan-1 · 2026-10-01–2026-10-01 · 1 session", exact=True).click()
            acknowledgement = page.get_by_label("I reviewed this plan and want the season to own its date range.", exact=True)
            acknowledgement.click()
            expect(acknowledgement).to_have_attribute("aria-checked", "true")
            page.get_by_role("button", name="Link reviewed plan", exact=True).click()
            page.get_by_text("Event changes are saved as planning intent. The linked schedule stays in place until regeneration is available.", exact=True).wait_for()
            assert get_active_plan_sha256(db) == original_hash
            expect(page.locator(".q-notification")).to_have_count(0, timeout=15000)
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(250)
            page.screenshot(path=str(output / "desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(250)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(output / "narrow.png"), full_page=True)
            page.get_by_role("button", name="Add event", exact=True).click()
            page.wait_for_timeout(250)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(output / "event_editor_narrow.png"))
            save_button = page.get_by_role("dialog").get_by_role("button", name="Save event", exact=True)
            save_button.scroll_into_view_if_needed()
            expect(save_button).to_be_in_viewport()
            page.screenshot(path=str(output / "event_editor_narrow_bottom.png"))
            page.get_by_role("dialog").get_by_role("button", name="Close event editor", exact=True).click()
            page.get_by_role("button", name=f"Remove event {events[1].event_id}", exact=True).click()
            page.get_by_role("dialog").get_by_role("button", name="Confirm removal", exact=True).click()
            page.get_by_text("2026-10-17 · Marathon · Trail · Cancelled", exact=True).wait_for()
            assert get_active_plan_sha256(db) == original_hash
            page.get_by_role("button", name="Archive season", exact=True).click()
            page.get_by_role("button", name="Restore season", exact=True).wait_for()
            assert page.get_by_role("button", name="Add event", exact=True).count() == 0
            page.get_by_role("button", name="Restore season", exact=True).click()
            page.get_by_role("button", name="Add event", exact=True).wait_for()
            page.goto(base + "/plan", wait_until="networkidle")
            page.get_by_role("link", name="Manage seasons and multiple events", exact=True).wait_for()
            assert get_active_plan_sha256(db) == original_hash
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server = _stop_server(process)
    assert not errors, errors
    assert "Traceback" not in server, server
