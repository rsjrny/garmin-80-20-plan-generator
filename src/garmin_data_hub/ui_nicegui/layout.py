"""Shared native application shell for NiceGUI pages."""

from __future__ import annotations

from pathlib import Path


NAVIGATION = (
    ("Dashboard", "/", "dashboard"),
    ("Garmin Sync", "/sync", "sync"),
    ("Activities", "/activities", "directions_run"),
    ("Charts", "/charts", "monitoring"),
    ("Plan", "/plan", "event_note"),
    ("Codex Coach", "/coach", "auto_awesome"),
    ("Compliance", "/compliance", "fact_check"),
    ("Data Query", "/query", "database"),
    ("Settings", "/settings", "settings"),
    ("Guide", "/guide", "help_outline"),
)


def render_shell(title: str, db_path: Path, *, sandboxed: bool) -> None:
    from nicegui import ui

    ui.add_css(
        """
        body { background: #f4f6f8; }
        .gdh-card { border: 1px solid #dce3e9; border-radius: 14px; box-shadow: none; }
        .gdh-metric { min-width: 155px; flex: 1 1 155px; }
        .gdh-page { width: 100%; max-width: 1550px; margin: 0 auto; padding: 1.25rem; gap: 1rem; }
        .gdh-nav { width: 100%; justify-content: flex-start; color: #334155; }
        """
    )
    def toggle_navigation() -> None:
        navigation_drawer.toggle()

    with ui.header().classes("items-center justify-between bg-slate-900 px-5"):
        with ui.row().classes("items-center gap-3"):
            ui.button(icon="menu", on_click=toggle_navigation).props(
                'flat round color=white aria-label="Navigation menu"'
            ).tooltip("Open or close navigation")
            ui.icon("directions_run").classes("text-2xl")
            ui.label("Garmin Data Hub").classes("text-xl font-semibold")
            if sandboxed:
                ui.badge("SANDBOX", color="amber")
        ui.label(title).classes("text-sm text-slate-200")

    with ui.left_drawer(value=True).props(
        "show-if-above breakpoint=1024"
    ).classes("bg-slate-50") as navigation_drawer:
        ui.label("NAVIGATION").classes("text-xs font-bold text-grey-6 px-2 pt-3")
        for label, route, icon in NAVIGATION:
            with ui.link(target=route).classes("no-underline w-full"):
                with ui.row().classes("gdh-nav items-center gap-3 px-3 py-2 rounded-lg"):
                    ui.icon(icon)
                    ui.label(label)
        ui.separator().classes("my-3")
        ui.label("Database").classes("text-xs font-bold text-grey-6 px-2")
        ui.label(str(db_path)).classes("text-xs text-grey-7 px-2 break-all")
        if sandboxed:
            ui.label("Changes affect only the snapshot.").classes(
                "text-xs text-amber-9 px-2 mt-2"
            )


def page_heading(title: str, description: str) -> None:
    from nicegui import ui

    ui.label(title).classes("text-3xl font-bold text-slate-900")
    ui.label(description).classes("text-slate-600")


def metric_card(label: str, value: object, *, icon: str | None = None) -> None:
    from nicegui import ui

    with ui.card().classes("gdh-card gdh-metric p-3"):
        with ui.row().classes("items-center gap-2"):
            if icon:
                ui.icon(icon).classes("text-blue-7")
            ui.label(label).classes("text-xs uppercase text-grey-7")
        ui.label(str(value)).classes("text-2xl font-semibold")


def data_grid(rows: list[dict], *, height: str = "28rem"):
    from nicegui import ui

    if not rows:
        ui.label("No data available.").classes("text-grey-7")
        return None
    columns = [
        {
            "headerName": key.replace("_", " ").title(),
            "field": key,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "wrapText": key in {"notes", "description", "Current", "Proposed"},
            "autoHeight": key in {"notes", "description", "Current", "Proposed"},
        }
        for key in rows[0]
    ]
    return ui.aggrid(
        {
            "columnDefs": columns,
            "rowData": rows,
            "pagination": len(rows) > 25,
            "paginationPageSize": 25,
            "rowSelection": {"mode": "singleRow"},
            "defaultColDef": {"minWidth": 105},
        }
    ).classes("w-full").style(f"height: {height}")
