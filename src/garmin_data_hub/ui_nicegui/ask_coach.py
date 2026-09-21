"""Session-local Ask Coach page."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from garmin_data_hub.services.coach_chat import build_chat_context, generate_chat_answer, add_mcp_context
from garmin_data_hub.services.codex_plan_generator import CodexCliCancelledError, CodexPlanGenerationError
from garmin_data_hub.ui_nicegui.layout import render_shell


def register_ask_coach_page(db_path: Path, *, sandboxed: bool) -> None:
    from nicegui import run, ui

    @ui.page("/ask-coach")
    def ask_coach() -> None:
        render_shell("Ask Coach", db_path, sandboxed=sandboxed)
        history: list[dict[str, str]] = []
        cancel_event = threading.Event()
        busy = False
        client = ui.context.client
        client.on_disconnect(cancel_event.set)

        with ui.column().classes("gdh-page").style("max-width: 1050px"):
            with ui.row().classes("w-full items-center justify-between"):
                ui.label("Ask Coach").classes("text-2xl font-semibold")
                ui.link("Codex Coach", "/coach")
            with ui.row().classes("w-full items-center justify-between"):
                recovery = ui.switch("Share sleep & recovery summaries", value=False)
                clear = ui.button(icon="delete_sweep").props('flat round aria-label="Clear conversation"').tooltip("Clear conversation")
            ui.label("Questions and selected training data are sent to Codex. Chat is not saved.").classes("text-sm text-grey-7")
            lookups = ui.switch("Look up additional Garmin data", value=False)
            ui.label("Lookup results are also shared with Codex when enabled.").classes("text-sm text-grey-7")
            with ui.row().classes("gap-2 flex-wrap") as starters:
                for text in ("How is my training progressing?", "What is coming up in my plan?", "How consistent have I been?"):
                    ui.button(text, on_click=lambda _, value=text: question.set_value(value)).props("flat dense")
            transcript = ui.column().classes("w-full gap-4").style("overflow-wrap: anywhere")
            with ui.expansion("Context sent with the last question", icon="data_object").classes("w-full"):
                preview = ui.code("{}", language="json").classes("w-full").style("max-height: 20rem; overflow: auto")
            question = ui.textarea("Your question").props("outlined autogrow maxlength=4000").classes("w-full")
            with ui.row().classes("w-full items-center gap-3"):
                send = ui.button("Ask", icon="send")
                cancel = ui.button(icon="stop", on_click=cancel_event.set).props('flat round aria-label="Cancel answer"').tooltip("Cancel answer")
                cancel.set_visibility(False)
                spinner = ui.spinner(size="sm")
                spinner.set_visibility(False)
                status = ui.label("").classes("text-sm text-grey-7")

            def clear_chat() -> None:
                history.clear()
                transcript.clear()
                preview.set_content("{}")
                status.set_text("")

            clear.on_click(clear_chat)
            # Clear prior health-bearing responses when the sharing scope changes.
            recovery.on_value_change(lambda _: clear_chat())
            lookups.on_value_change(lambda _: clear_chat())

            async def ask() -> None:
                nonlocal busy
                text = (question.value or "").strip()
                if busy or not text:
                    return
                busy = True
                cancel_event.clear()
                for control in (send, clear, recovery, lookups, question):
                    control.set_enabled(False)
                starters.set_visibility(False)
                cancel.set_visibility(True)
                spinner.set_visibility(True)
                status.set_text("Reviewing your training...")
                try:
                    context = await run.io_bound(build_chat_context, db_path, include_recovery=bool(recovery.value))
                    if lookups.value:
                        status.set_text("Looking up Garmin data...")
                        context["mcp"] = await run.io_bound(
                            add_mcp_context, db_path, context, text, list(history), cancel_event=cancel_event)
                    preview.set_content(json.dumps(context, indent=2, ensure_ascii=False))
                    status.set_text("Preparing your answer...")
                    answer = await run.io_bound(generate_chat_answer, context, text, list(history), cancel_event=cancel_event)
                    if cancel_event.is_set():
                        raise CodexCliCancelledError("Cancelled")
                    with transcript:
                        ui.label(text).classes("w-full font-semibold whitespace-pre-wrap")
                        ui.label(answer["answer"]).classes("w-full whitespace-pre-wrap")
                        for key, label in (("evidence", "Evidence"), ("missing_data", "Data gaps")):
                            if answer[key]:
                                with ui.expansion(label).classes("w-full"):
                                    for item in answer[key]:
                                        ui.label(item).classes("whitespace-pre-wrap")
                        with ui.row().classes("flex-wrap gap-2"):
                            for item in answer["followups"][:3]:
                                ui.button(item, on_click=lambda _, value=item: question.set_value(value)).props("flat dense").classes("max-w-full whitespace-normal")
                        ui.separator()
                    history.extend([{"role": "user", "content": text}, {"role": "assistant", "content": answer["answer"]}])
                    del history[:-12]
                    question.set_value("")
                    status.set_text("")
                except CodexCliCancelledError:
                    status.set_text("Answer cancelled. Your question is ready to retry.")
                except CodexPlanGenerationError as exc:
                    status.set_text(str(exc))
                except Exception:
                    status.set_text("Could not answer. Check Codex Coach sign-in and your database, then retry.")
                finally:
                    busy = False
                    for control in (send, clear, recovery, lookups, question):
                        control.set_enabled(True)
                    starters.set_visibility(True)
                    cancel.set_visibility(False)
                    spinner.set_visibility(False)

            send.on_click(ask)
