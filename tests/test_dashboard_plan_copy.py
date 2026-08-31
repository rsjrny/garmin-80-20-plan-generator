"""Dashboard upcoming-plan clipboard behavior."""

from __future__ import annotations

from pathlib import Path

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


def test_dashboard_wires_copy_button_to_visible_upcoming_rows():
    source = Path(pages.__file__).read_text(encoding="utf-8")
    dashboard = source.split('    @ui.page("/")', maxsplit=1)[1].split(
        '    @ui.page("/activities")', maxsplit=1
    )[0]

    assert '"Copy plan"' in dashboard
    assert 'icon="content_copy"' in dashboard
    assert "on_click=copy_upcoming_plan" in dashboard
    assert "_format_upcoming_plan_for_sharing(" in dashboard
    assert "display_upcoming," in dashboard
    assert "ui.clipboard.write(shareable_upcoming)" in dashboard
    assert "copy_plan_button.set_enabled(bool(display_upcoming))" in dashboard
    assert "Copied {session_count} upcoming {session_label}" in dashboard
    assert '"to the clipboard."' in dashboard
