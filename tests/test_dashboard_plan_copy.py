"""Dashboard upcoming-plan clipboard behavior."""

from __future__ import annotations

import pytest

from nicegui import ui

from ui_test_support import run_ui, wait_until

from garmin_data_hub.ui_nicegui import pages
from garmin_data_hub.ui_nicegui.pages import _format_upcoming_plan_for_sharing


def test_shareable_upcoming_plan_is_plain_text_in_display_units():
    text = _format_upcoming_plan_for_sharing(
        [
            {
                "date": "2026-09-01",
                "workout": " Easy\nrun ",
                "duration_min": 45.0,
                "distance_mi": 5.0,
                "tss": 32.5,
            },
            {
                "date": "legacy-date",
                "workout": "Strength",
                "duration_min": 30,
                "distance_mi": None,
                "tss": 0,
            },
        ],
        "mi",
    )

    assert text == (
        "Upcoming training plan (2 sessions shown)\n"
        "- Tue 2026-09-01 — Easy run · 45 min · 5 mi · TSS 32.5\n"
        "- legacy-date — Strength · 30 min · TSS 0"
    )
    assert "None" not in text


def test_shareable_upcoming_plan_handles_one_or_no_sessions():
    assert _format_upcoming_plan_for_sharing([], "km") == (
        "Upcoming training plan\nNo upcoming sessions scheduled."
    )
    assert _format_upcoming_plan_for_sharing(
        [
            {
                "date": "2026-09-02",
                "workout": "Rest",
                "duration_min": None,
                "distance_km": float("nan"),
                "tss": None,
            }
        ],
        "km",
    ) == (
        "Upcoming training plan (1 session shown)\n"
        "- Wed 2026-09-02 — Rest"
    )


@pytest.mark.parametrize("has_plan", [True, False], ids=["populated", "empty"])
def test_dashboard_copy_uses_visible_rows_and_empty_plan_is_disabled(
    monkeypatch, ui_database, has_plan
):
    upcoming = [{"date": "2026-09-01", "workout": "Easy run", "duration_min": 45,
                 "distance_km": 8.04672, "tss": 32.5}] if has_plan else []
    monkeypatch.setattr(pages, "dashboard_data", lambda *_args, **_kwargs: {
        "stats": {}, "plan": {}, "athlete": {}, "diagnostics": {},
        "upcoming": upcoming, "recent": [],
    })
    copied = []
    monkeypatch.setattr(ui.clipboard, "write", copied.append)

    async def scenario(user):
        await user.open("/")
        button = next(iter(user.find("Copy plan").elements))
        assert button.enabled is has_plan
        if has_plan:
            user.find("Copy plan").click()
            await wait_until(lambda: bool(copied))
            assert copied == [
                "Upcoming training plan (1 session shown)\n"
                "- Tue 2026-09-01 — Easy run · 45 min · 5 mi · TSS 32.5"
            ]
            assert user.notify.contains("Copied 1 upcoming session to the clipboard.")
        else:
            assert copied == []

    run_ui(ui_database, scenario)
