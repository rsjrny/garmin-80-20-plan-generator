"""NiceGUI vertical-slice prototype for the Codex Plan Workspace."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import default_db_path, schema_sql_path
from garmin_data_hub.services.ai_plan_import import PlanImportError
from garmin_data_hub.services.codex_prerequisites import (
    NODE_DOWNLOAD_URL,
    OPENAI_CODEX_AUTH_URL,
    OPENAI_CODEX_INSTALL_URL,
    CodexPrerequisiteError,
    CodexPrerequisiteStatus,
    ToolStatus,
    detect_codex_prerequisites,
    install_missing_codex_prerequisites,
    launch_codex_login,
)
from garmin_data_hub.services.plan_persistence import (
    PlanPersistenceError,
    StalePlanWriteError,
)
from garmin_data_hub.ui_nicegui.data import cancel_all_sync_jobs
from garmin_data_hub.ui_nicegui.layout import render_shell
from garmin_data_hub.ui_nicegui.pages import register_core_pages
from garmin_data_hub.ui_nicegui.ask_coach import register_ask_coach_page
from garmin_data_hub.ui_nicegui.workspace import (
    GenerationJob,
    ProposalReview,
    WorkspaceContext,
    build_workspace_packet,
    create_database_snapshot,
    load_workspace_context,
    load_workspace_prompt,
    persist_workspace_preferences,
    prompt_as_template,
    prompt_for_packet,
    review_proposal,
    save_review_to_database,
    save_workspace_prompt,
    validate_workspace_context,
)


class _LocalSecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Prevent browser embedding and common content-type confusion attacks."""

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response


def _configure_local_web_security(app: Any, core: Any, *, port: int) -> None:
    """Restrict the local desktop server's hosts, origins, and framing."""
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost"],
    )
    app.add_middleware(_LocalSecurityHeadersMiddleware)
    core.sio.eio.cors_allowed_origins = [
        f"http://127.0.0.1:{port}",
        f"http://localhost:{port}",
    ]


def _workout_rows(review: ProposalReview) -> list[dict[str, Any]]:
    return [
        {
            "date": item.iso_date,
            "sport": item.sport.replace("_", " ").title(),
            "phase": item.phase,
            "workout": item.workout,
            "intensity": item.intensity.title(),
            "duration_min": item.duration_minutes,
            "distance_km": item.distance_km,
            "tss": item.tss,
            "flags": ", ".join(item.flags),
            "notes": item.notes,
        }
        for item in review.plan.workouts
    ]


def _macro_rows(review: ProposalReview) -> list[dict[str, Any]]:
    return [target.to_dict() for target in review.plan.nutrition_targets]


def _grid(rows: list[dict[str, Any]], *, height: str = "24rem") -> None:
    from nicegui import ui

    if not rows:
        ui.label("No rows to display.").classes("text-grey-7")
        return
    columns = [
        {
            "headerName": key.replace("_", " ").title(),
            "field": key,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "wrapText": key in {"notes", "Current", "Proposed"},
            "autoHeight": key in {"notes", "Current", "Proposed"},
        }
        for key in rows[0]
    ]
    ui.aggrid(
        {
            "columnDefs": columns,
            "rowData": rows,
            "pagination": len(rows) > 25,
            "paginationPageSize": 25,
            "defaultColDef": {"minWidth": 110},
        }
    ).classes("w-full").style(f"height: {height}")


def _tool_status_text(tool: ToolStatus) -> str:
    if tool.available:
        return f"{tool.name}: {tool.version} ({tool.path})"
    if tool.path:
        return f"{tool.name}: found but unavailable - {tool.error}"
    return f"{tool.name}: not installed"


def _auth_status_text(status: CodexPrerequisiteStatus) -> str:
    if status.auth_state == "signed_in":
        return "Codex account: signed in"
    if status.auth_state == "signed_out":
        return "Codex account: sign-in required"
    if status.auth_state == "not_installed":
        return "Codex account: install Codex before signing in"
    return f"Codex account: status unavailable - {status.auth_detail or 'try again'}"


def create_ui(db_path: Path, *, sandboxed: bool) -> None:
    """Register the primary NiceGUI application against the selected database."""
    from nicegui import run, ui

    register_core_pages(db_path, sandboxed=sandboxed)
    register_ask_coach_page(db_path, sandboxed=sandboxed)

    @ui.page("/coach")
    def codex_workspace() -> None:
        context_holder: dict[str, WorkspaceContext] = {
            "value": load_workspace_context(db_path, sandboxed=sandboxed)
        }
        job = GenerationJob()
        state: dict[str, Any] = {
            "packet": None,
            "prompt_packet": None,
            "review": None,
            "handled_generation_state": "idle",
            "prerequisites": None,
            "prerequisites_busy": False,
            "codex_login_launched": False,
        }

        render_shell("Codex Coach", db_path, sandboxed=sandboxed)

        with ui.column().classes("gdh-page"):
            if sandboxed:
                with ui.card().classes("gdh-card w-full bg-amber-1 text-amber-10"):
                    with ui.row().classes("items-center gap-3"):
                        ui.icon("science")
                        ui.label(
                            "Sandbox mode: this interface uses a SQLite snapshot. "
                            "Applying a plan changes only the prototype database, "
                            "never the live app."
                        )
            else:
                with ui.card().classes("gdh-card w-full bg-red-1 text-red-10"):
                    with ui.row().classes("items-center gap-3"):
                        ui.icon("warning")
                        ui.label(
                            "LIVE DATABASE MODE: approved plans will update the "
                            "active database."
                        )

            ui.label("Codex Plan Workspace").classes("text-3xl font-bold text-slate-900")
            ui.label(
                "Generate through your signed-in Codex CLI, validate locally, "
                "review exact changes, and explicitly approve before saving."
            ).classes("text-slate-600")

            context = context_holder["value"]
            with ui.row().classes("w-full gap-3 flex-wrap"):
                for label, value, icon in (
                    ("Plan start", context.plan_start.isoformat(), "calendar_today"),
                    ("Event date", context.event_date.isoformat(), "flag"),
                    ("Distance", context.distance, "straighten"),
                    ("Run days/week", str(context.run_days_per_week), "repeat"),
                    ("History", f"{context.lookback_weeks} weeks", "history"),
                ):
                    with ui.card().classes("gdh-card gdh-metric p-3"):
                        with ui.row().classes("items-center gap-2"):
                            ui.icon(icon).classes("text-blue-7")
                            ui.label(label).classes("text-xs uppercase text-grey-7")
                        ui.label(value).classes("text-xl font-semibold")

            validation_errors = validate_workspace_context(context)
            if validation_errors:
                with ui.card().classes("gdh-card w-full bg-red-1"):
                    ui.label("Resolve these Build Plan settings first:").classes(
                        "font-semibold text-red-9"
                    )
                    for message in validation_errors:
                        ui.label(f"• {message}").classes("text-red-9")

            tabs = ui.tabs().classes("w-full")
            with tabs:
                configure_tab = ui.tab("Configure & Generate", icon="tune")
                review_tab = ui.tab("Review & Apply", icon="fact_check")

            with ui.tab_panels(tabs, value=configure_tab).classes(
                "gdh-card w-full bg-white"
            ):
                with ui.tab_panel(configure_tab):
                    ui.label("Coaching preferences").classes("text-xl font-semibold")
                    ui.label(
                        "These fields are saved to the prototype database when "
                        "generation starts. Leave unknown information blank."
                    ).classes("text-sm text-grey-7")
                    preference_inputs: dict[str, Any] = {}
                    labels = {
                        "injuries_or_limitations": "Injuries or limitations",
                        "scheduling_notes": "Scheduling notes",
                        "strength_equipment": "Strength equipment",
                        "strength_experience": "Strength experience",
                        "dietary_preferences": "Dietary preferences",
                        "allergies_or_intolerances": "Allergies or intolerances",
                        "gi_considerations": "GI considerations",
                    }
                    with ui.grid(columns=2).classes("w-full gap-3"):
                        for key, label in labels.items():
                            preference_inputs[key] = ui.textarea(
                                label,
                                value=context.preferences.get(key, ""),
                            ).props("outlined autogrow").classes("w-full")
                    lookback = ui.select(
                        [4, 8, 12, 16, 24, 52],
                        value=context.lookback_weeks,
                        label="Training-history window (weeks)",
                    ).props("outlined").classes("w-72")

                    with ui.card().classes("gdh-card w-full bg-slate-50"):
                        ui.label("Codex generation").classes("text-lg font-semibold")
                        ui.label(
                            "Node.js, npm, and Codex are optional and used only "
                            "for this page. Existing working installations are kept."
                        ).classes("text-sm text-grey-7")
                        prerequisite_summary = ui.label(
                            "Checking local prerequisites..."
                        ).classes("font-medium")
                        node_status_label = ui.label("node: checking...").classes(
                            "text-sm text-grey-7 break-all"
                        )
                        npm_status_label = ui.label("npm: checking...").classes(
                            "text-sm text-grey-7 break-all"
                        )
                        codex_status_label = ui.label("codex: checking...").classes(
                            "text-sm text-grey-7 break-all"
                        )
                        auth_status_label = ui.label(
                            "Codex account: checking..."
                        ).classes("text-sm text-grey-7")
                        setup_detail_label = ui.label(
                            "Detection runs version and login-status checks; it "
                            "does not invoke installers."
                        ).classes("text-sm text-grey-7 whitespace-pre-wrap")
                        setup_progress = ui.linear_progress(value=0).props(
                            "indeterminate"
                        )
                        setup_progress.set_visibility(False)
                        with ui.row().classes("gap-2 flex-wrap"):
                            check_prerequisites_button = ui.button(
                                "Check again", icon="refresh"
                            ).props("outline")
                            install_prerequisites_button = ui.button(
                                "Install missing tools", icon="download"
                            ).props("color=primary")
                            sign_in_button = ui.button(
                                "Sign in to Codex", icon="login"
                            ).props("outline")
                            install_prerequisites_button.set_visibility(False)
                            sign_in_button.set_visibility(False)
                        with ui.row().classes("gap-4 flex-wrap"):
                            ui.link(
                                "Node.js manual download",
                                NODE_DOWNLOAD_URL,
                                new_tab=True,
                            )
                            ui.link(
                                "Codex install help",
                                OPENAI_CODEX_INSTALL_URL,
                                new_tab=True,
                            )
                            ui.link(
                                "Codex sign-in help",
                                OPENAI_CODEX_AUTH_URL,
                                new_tab=True,
                            )

                        with ui.dialog() as prerequisite_dialog, ui.card().classes(
                            "w-full max-w-2xl"
                        ):
                            ui.label("Install missing Codex tools?").classes(
                                "text-xl font-semibold"
                            )
                            ui.label(
                                "Garmin Data Hub will leave every working command "
                                "unchanged. If Node.js or npm is missing, Windows "
                                "Package Manager will install the Node.js LTS package. "
                                "If Codex is missing, npm will run "
                                "'npm install --global @openai/codex'."
                            ).classes("max-w-xl")
                            ui.label(
                                "This downloads software from the internet. Windows "
                                "may request administrator approval for Node.js. "
                                "A failed setup does not remove Garmin Data Hub or "
                                "uninstall any shared tools."
                            ).classes("max-w-xl text-amber-9")
                            setup_consent = ui.checkbox(
                                "I approve downloading and installing only the missing tools."
                            )

                            def confirm_prerequisite_install() -> None:
                                if not setup_consent.value:
                                    ui.notify(
                                        "Check the consent box before installing.",
                                        type="warning",
                                    )
                                    return
                                prerequisite_dialog.submit(True)

                            with ui.row().classes("w-full justify-end gap-2"):
                                ui.button(
                                    "Cancel",
                                    on_click=lambda: prerequisite_dialog.submit(False),
                                ).props("flat")
                                ui.button(
                                    "Install missing tools",
                                    icon="download",
                                    on_click=confirm_prerequisite_install,
                                ).props("color=primary")

                        ui.separator()
                        status_label = ui.label(
                            "Generation is available after Codex sign-in."
                        ).classes("font-medium")
                        try:
                            prompt_packet = build_workspace_packet(context)
                            initial_prompt = load_workspace_prompt(
                                context.db_path, prompt_packet
                            )
                        except (OSError, TypeError, ValueError):
                            prompt_packet = None
                            initial_prompt = "Resolve the plan settings above before generating."
                        state["prompt_packet"] = prompt_packet
                        prompt_editor = ui.textarea(
                            "Generation prompt", value=initial_prompt
                        ).props("outlined autogrow").classes(
                            "w-full whitespace-pre-wrap break-words"
                        )
                        ui.label(
                            "Saved prompts use placeholders for request-specific hashes, "
                            "which are restored for each fresh packet."
                        ).classes("text-xs text-grey-7")
                        with ui.row().classes("gap-2"):
                            load_default_button = ui.button(
                                "Load default prompt", icon="restart_alt"
                            ).props("outline")
                            save_prompt_button = ui.button(
                                "Save prompt", icon="save"
                            ).props("outline")
                        elapsed_label = ui.label("Elapsed: 0:00").classes(
                            "text-sm text-grey-7"
                        )
                        progress = ui.linear_progress(value=0).props("indeterminate")
                        progress.set_visibility(False)
                        with ui.row().classes("gap-2"):
                            generate_button = ui.button(
                                "Generate proposal", icon="auto_awesome"
                            ).props("color=primary")
                            generate_button.disable()
                            cancel_button = ui.button(
                                "Cancel", icon="stop", color="negative"
                            )
                            cancel_button.set_visibility(False)

                        def load_default_prompt() -> None:
                            packet = state.get("prompt_packet")
                            if packet:
                                prompt_editor.value = str(
                                    packet["chatgpt"]["copyable_prompt"]
                                )

                        def save_current_prompt() -> None:
                            packet = state.get("prompt_packet")
                            if not packet:
                                ui.notify(
                                    "Build a valid packet before saving a prompt.",
                                    type="negative",
                                )
                                return
                            try:
                                save_workspace_prompt(
                                    context_holder["value"].db_path,
                                    str(prompt_editor.value or ""),
                                    packet,
                                )
                            except OSError as exc:
                                ui.notify(str(exc), type="negative")
                                return
                            ui.notify("Generation prompt saved locally.", type="positive")

                        load_default_button.on("click", load_default_prompt)
                        save_prompt_button.on("click", save_current_prompt)

                        def refresh_prerequisite_controls() -> None:
                            prerequisite_status: CodexPrerequisiteStatus | None = (
                                state.get("prerequisites")
                            )
                            busy = bool(state["prerequisites_busy"])
                            generation_running = job.snapshot().state in {
                                "running",
                                "cancelling",
                            }
                            controls_enabled = not busy and not generation_running
                            check_prerequisites_button.set_enabled(controls_enabled)
                            if prerequisite_status is None:
                                install_prerequisites_button.set_visibility(False)
                                sign_in_button.set_visibility(False)
                                generate_button.disable()
                                return
                            install_prerequisites_button.set_visibility(
                                not prerequisite_status.commands_ready
                            )
                            install_prerequisites_button.set_enabled(
                                controls_enabled
                            )
                            sign_in_button.set_visibility(
                                prerequisite_status.codex.available
                                and prerequisite_status.auth_state == "signed_out"
                            )
                            sign_in_button.set_enabled(
                                controls_enabled
                                and not bool(state["codex_login_launched"])
                            )
                            generate_button.set_enabled(
                                prerequisite_status.ready_for_generation
                                and not validation_errors
                                and not busy
                                and not generation_running
                            )

                        def show_prerequisite_status(
                            prerequisite_status: CodexPrerequisiteStatus,
                        ) -> None:
                            state["prerequisites"] = prerequisite_status
                            node_status_label.text = _tool_status_text(
                                prerequisite_status.node
                            )
                            npm_status_label.text = _tool_status_text(
                                prerequisite_status.npm
                            )
                            codex_status_label.text = _tool_status_text(
                                prerequisite_status.codex
                            )
                            auth_status_label.text = _auth_status_text(
                                prerequisite_status
                            )
                            if prerequisite_status.ready_for_generation:
                                prerequisite_summary.text = (
                                    "Codex is installed, signed in, and ready."
                                )
                            elif prerequisite_status.auth_state == "signed_out":
                                prerequisite_summary.text = (
                                    "Codex is installed; complete account sign-in."
                                )
                            elif prerequisite_status.codex.available:
                                prerequisite_summary.text = (
                                    "Codex is installed, but login status could not "
                                    "be verified."
                                )
                            else:
                                missing = ", ".join(
                                    prerequisite_status.missing_tools
                                )
                                prerequisite_summary.text = (
                                    f"Missing or unavailable: {missing}."
                                )
                            failures = [
                                tool.error
                                for tool in (
                                    prerequisite_status.node,
                                    prerequisite_status.npm,
                                    prerequisite_status.codex,
                                )
                                if tool.error
                            ]
                            if failures:
                                setup_detail_label.text = "\n".join(failures)
                            elif prerequisite_status.auth_state == "signed_out":
                                setup_detail_label.text = (
                                    "Use Sign in to Codex. The official CLI opens "
                                    "the browser flow; this app does not receive or "
                                    "store your Codex credentials."
                                )
                            elif prerequisite_status.auth_state == "unknown":
                                setup_detail_label.text = (
                                    prerequisite_status.auth_detail
                                    or "Could not verify the saved Codex login."
                                )
                            else:
                                setup_detail_label.text = (
                                    "All command and login checks passed."
                                )
                            refresh_prerequisite_controls()

                        def set_prerequisite_busy(busy: bool) -> None:
                            state["prerequisites_busy"] = busy
                            setup_progress.set_visibility(busy)
                            refresh_prerequisite_controls()

                        async def check_prerequisites() -> None:
                            if bool(state["prerequisites_busy"]):
                                return
                            state["codex_login_launched"] = False
                            set_prerequisite_busy(True)
                            prerequisite_summary.text = (
                                "Checking local prerequisites..."
                            )
                            try:
                                prerequisite_status = await run.io_bound(
                                    detect_codex_prerequisites
                                )
                            except Exception as exc:
                                state["prerequisites"] = None
                                prerequisite_summary.text = (
                                    "Prerequisite check failed."
                                )
                                setup_detail_label.text = str(exc)
                                ui.notify(
                                    f"Could not check Codex tools: {exc}",
                                    type="negative",
                                    multi_line=True,
                                )
                            else:
                                show_prerequisite_status(prerequisite_status)
                            finally:
                                set_prerequisite_busy(False)

                        async def install_prerequisites() -> None:
                            if not await prerequisite_dialog:
                                setup_consent.value = False
                                return
                            if bool(state["prerequisites_busy"]) or job.snapshot().state in {
                                "running",
                                "cancelling",
                            }:
                                setup_consent.value = False
                                ui.notify(
                                    "Wait for the current Codex operation to finish.",
                                    type="warning",
                                )
                                return
                            set_prerequisite_busy(True)
                            prerequisite_summary.text = (
                                "Installing and verifying missing tools..."
                            )
                            try:
                                result = await run.io_bound(
                                    install_missing_codex_prerequisites,
                                    consent=True,
                                )
                            except CodexPrerequisiteError as exc:
                                setup_detail_label.text = str(exc)
                                ui.notify(str(exc), type="negative", multi_line=True)
                            except Exception as exc:
                                setup_detail_label.text = str(exc)
                                ui.notify(
                                    f"Prerequisite setup failed: {exc}",
                                    type="negative",
                                    multi_line=True,
                                )
                            else:
                                show_prerequisite_status(result.after)
                                setup_detail_label.text = result.detail
                                if result.success:
                                    ui.notify(
                                        "Codex tools are installed. Sign in if prompted.",
                                        type="positive",
                                    )
                                else:
                                    ui.notify(
                                        "Setup is incomplete. Review the recovery "
                                        "details and manual-install links.",
                                        type="negative",
                                        multi_line=True,
                                    )
                            finally:
                                setup_consent.value = False
                                set_prerequisite_busy(False)

                        async def sign_in_to_codex() -> None:
                            if (
                                bool(state["prerequisites_busy"])
                                or bool(state["codex_login_launched"])
                                or job.snapshot().state in {"running", "cancelling"}
                            ):
                                return
                            set_prerequisite_busy(True)
                            try:
                                launch = await run.io_bound(launch_codex_login)
                            except CodexPrerequisiteError as exc:
                                ui.notify(str(exc), type="negative", multi_line=True)
                            except Exception as exc:
                                ui.notify(
                                    f"Could not open Codex sign-in: {exc}",
                                    type="negative",
                                    multi_line=True,
                                )
                            else:
                                state["codex_login_launched"] = True
                                setup_detail_label.text = (
                                    "Codex sign-in opened in a separate terminal and "
                                    "browser. Complete it there, close the terminal, "
                                    "then choose Check again."
                                )
                                ui.notify(
                                    f"Codex sign-in started (process {launch.process_id}).",
                                    type="info",
                                )
                            finally:
                                set_prerequisite_busy(False)

                        check_prerequisites_button.on(
                            "click", check_prerequisites
                        )
                        install_prerequisites_button.on(
                            "click", install_prerequisites
                        )
                        sign_in_button.on("click", sign_in_to_codex)
                        ui.timer(0.1, check_prerequisites, once=True)

                with ui.tab_panel(review_tab):
                    @ui.refreshable
                    def render_review() -> None:
                        review: ProposalReview | None = state.get("review")
                        if review is None:
                            ui.label(
                                "Generate a proposal to review its plan, policy "
                                "results, macros, and database changes."
                            ).classes("text-grey-7 p-4")
                            return

                        ui.label("Change summary").classes("text-xl font-semibold")
                        ui.label(review.plan.rationale or "No rationale returned.").classes(
                            "text-base"
                        )
                        with ui.row().classes("w-full gap-3 flex-wrap"):
                            for label, value in (
                                ("Plan days", len(review.plan.day_plans)),
                                ("Sessions", len(review.plan.workouts)),
                                ("Changed dates", len(review.changes)),
                                ("Policy errors", len(review.errors)),
                            ):
                                with ui.card().classes("gdh-card gdh-metric p-3"):
                                    ui.label(label).classes("text-xs uppercase text-grey-7")
                                    ui.label(str(value)).classes("text-2xl font-semibold")

                        if review.errors:
                            with ui.card().classes("gdh-card w-full bg-red-1"):
                                ui.label("Cannot apply").classes("font-bold text-red-9")
                                for message in review.errors:
                                    ui.label(f"• {message}").classes("text-red-9")
                        if review.warnings:
                            with ui.expansion("Warnings", icon="warning").classes(
                                "w-full bg-amber-1 rounded-lg"
                            ):
                                for message in review.warnings:
                                    ui.label(f"• {message}")

                        detail_tabs = ui.tabs().classes("w-full")
                        with detail_tabs:
                            changes = ui.tab("Database changes")
                            workouts = ui.tab("Daily plan")
                            macros = ui.tab("Macro schedule")
                            guidance = ui.tab("Guidance")
                        with ui.tab_panels(detail_tabs, value=changes).classes("w-full"):
                            with ui.tab_panel(changes):
                                _grid(list(review.changes))
                            with ui.tab_panel(workouts):
                                _grid(_workout_rows(review), height="30rem")
                            with ui.tab_panel(macros):
                                _grid(_macro_rows(review), height="30rem")
                            with ui.tab_panel(guidance):
                                ui.label("Strength").classes("font-semibold")
                                for item in review.plan.strength_guidance:
                                    ui.label(f"• {item}")
                                ui.label("Nutrition").classes("font-semibold mt-3")
                                for item in review.plan.nutrition_guidance:
                                    ui.label(f"• {item}")

                        acknowledgement = ui.checkbox(
                            "I reviewed the changes, warnings, and replacement range."
                        )
                        apply_button = ui.button(
                            (
                                "Apply to prototype database"
                                if context_holder["value"].sandboxed
                                else "Apply to live database"
                            ),
                            icon="save",
                        ).props("color=positive")
                        apply_button.set_enabled(review.can_apply)

                        def apply_plan() -> None:
                            if not acknowledgement.value:
                                ui.notify("Check the review acknowledgement first.", color="warning")
                                return
                            try:
                                result = save_review_to_database(
                                    context_holder["value"], review
                                )
                            except StalePlanWriteError as exc:
                                ui.notify(str(exc), color="negative", multi_line=True)
                            except (PlanPersistenceError, OSError, ValueError) as exc:
                                ui.notify(
                                    f"The plan was not saved: {exc}",
                                    color="negative",
                                    multi_line=True,
                                )
                            else:
                                ui.notify(
                                    f"Saved import #{result.plan_import_id}: "
                                    f"{result.workout_count} sessions.",
                                    color="positive",
                                )

                        apply_button.on("click", apply_plan)

                    render_review()

            async def start_generation() -> None:
                nonlocal context
                if bool(state["prerequisites_busy"]) or job.snapshot().state in {
                    "running",
                    "cancelling",
                }:
                    return
                set_prerequisite_busy(True)
                status_label.text = "Verifying Codex sign-in..."
                try:
                    prerequisite_status = await run.io_bound(
                        detect_codex_prerequisites
                    )
                except Exception as exc:
                    status_label.text = "Could not verify Codex prerequisites."
                    setup_detail_label.text = str(exc)
                    ui.notify(
                        f"Could not check Codex tools: {exc}",
                        color="negative",
                        multi_line=True,
                    )
                    set_prerequisite_busy(False)
                    return
                show_prerequisite_status(prerequisite_status)
                if not prerequisite_status.ready_for_generation:
                    if not prerequisite_status.codex.available:
                        message = "Install the missing Codex tools before generating."
                    elif prerequisite_status.auth_state == "signed_out":
                        message = "Sign in to Codex before generating."
                    else:
                        message = (
                            "The saved Codex login could not be verified. "
                            "Review the status details, then use Check again."
                        )
                    status_label.text = message
                    ui.notify(message, color="negative")
                    set_prerequisite_busy(False)
                    return
                cli_path = prerequisite_status.codex.path
                if not cli_path:
                    status_label.text = "A working Codex CLI was not found."
                    set_prerequisite_busy(False)
                    return
                preferences = {
                    key: str(element.value or "")
                    for key, element in preference_inputs.items()
                }
                try:
                    context = persist_workspace_preferences(
                        context_holder["value"],
                        preferences,
                        lookback_weeks=int(lookback.value),
                    )
                    context_holder["value"] = context
                    packet = build_workspace_packet(context)
                    prior_packet = state.get("prompt_packet")
                    if prior_packet:
                        template = prompt_as_template(
                            str(prompt_editor.value or ""),
                            str(prior_packet["request_id"]),
                            str(prior_packet["active_plan_sha256"]),
                        )
                        prompt = prompt_for_packet(template, packet)
                    else:
                        prompt = str(packet["chatgpt"]["copyable_prompt"])
                    prompt_editor.value = prompt
                    state["prompt_packet"] = packet
                    job.start(packet, prompt=prompt, executable=cli_path)
                except (OSError, TypeError, ValueError, RuntimeError) as exc:
                    ui.notify(str(exc), color="negative", multi_line=True)
                    set_prerequisite_busy(False)
                    return
                state["packet"] = packet
                state["review"] = None
                state["handled_generation_state"] = "running"
                render_review.refresh()
                status_label.text = "Codex is generating a proposal…"
                generate_button.disable()
                cancel_button.set_visibility(True)
                progress.set_visibility(True)
                set_prerequisite_busy(False)

            def cancel_generation() -> None:
                if job.cancel():
                    status_label.text = "Cancelling Codex…"
                    cancel_button.disable()

            generate_button.on("click", start_generation)
            cancel_button.on("click", cancel_generation)

            def poll_generation() -> None:
                snapshot = job.snapshot()
                minutes, seconds = divmod(int(snapshot.elapsed_seconds), 60)
                elapsed_label.text = f"Elapsed: {minutes}:{seconds:02d}"
                if snapshot.state in {"running", "cancelling"}:
                    return
                if state.get("handled_generation_state") == snapshot.state:
                    return
                state["handled_generation_state"] = snapshot.state
                progress.set_visibility(False)
                cancel_button.set_visibility(False)
                cancel_button.enable()
                if snapshot.state == "completed" and snapshot.response_json:
                    try:
                        state["review"] = review_proposal(
                            context_holder["value"],
                            state["packet"],
                            snapshot.response_json,
                        )
                    except (PlanImportError, KeyError, TypeError, ValueError) as exc:
                        status_label.text = "Codex returned an invalid proposal."
                        ui.notify(str(exc), color="negative", multi_line=True)
                    else:
                        status_label.text = "Proposal ready for review."
                        tabs.set_value(review_tab)
                        render_review.refresh()
                elif snapshot.state == "cancelled":
                    status_label.text = "Generation cancelled; nothing was saved."
                elif snapshot.state == "failed":
                    status_label.text = "Generation failed; nothing was saved."
                    ui.notify(snapshot.error or "Unknown Codex failure", color="negative")
                    state["prerequisites"] = None
                    ui.timer(0.1, check_prerequisites, once=True)
                refresh_prerequisite_controls()

            ui.timer(0.5, poll_generation)


def _prepare_database(args: argparse.Namespace) -> tuple[Path, bool]:
    source = Path(args.source_db or default_db_path())
    source.parent.mkdir(parents=True, exist_ok=True)
    use_snapshot = (bool(args.sandbox) or bool(args.preview_db)) and not args.live_db
    if use_snapshot:
        target = Path(
            args.preview_db
            or source.parent / "nicegui-preview" / "garmin-preview.db"
        )
        return create_database_snapshot(source, target), True
    conn = connect_sqlite(source)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return source, False


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run Garmin Data Hub")
    parser.add_argument("--source-db", help="SQLite database to use")
    parser.add_argument("--preview-db", help="Sandbox snapshot destination")
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Use a copied database rather than the live database",
    )
    parser.add_argument(
        "--live-db",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="Open in the default browser instead of a native window",
    )
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args(argv)

    try:
        from nicegui import app, core, ui
    except ImportError as exc:  # pragma: no cover - environment guidance
        raise SystemExit(
            "NiceGUI is not installed. Run: pip install -e .[nicegui]"
        ) from exc

    db_path, sandboxed = _prepare_database(args)
    _configure_local_web_security(app, core, port=args.port)
    create_ui(db_path, sandboxed=sandboxed)
    app.on_shutdown(cancel_all_sync_jobs)
    ui.run(
        title="Garmin Data Hub",
        host="127.0.0.1",
        native=not args.browser,
        reload=False,
        port=args.port,
        show=True,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
