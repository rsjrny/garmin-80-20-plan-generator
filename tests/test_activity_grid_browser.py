from __future__ import annotations

import ast
import hashlib
import os
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.ui_nicegui import pages


SERVER_SCRIPT = """
import sys
from pathlib import Path

from garmin_data_hub.ui_nicegui.app import create_ui, _configure_local_web_security
from nicegui import app, core, ui

db_path = Path(sys.argv[1])
port = int(sys.argv[2])
_configure_local_web_security(app, core, port=port)
create_ui(db_path, sandboxed=True)
ui.run(
    title='Garmin Data Hub browser regression',
    host='127.0.0.1',
    native=False,
    reload=False,
    port=port,
    show=False,
)
"""


def _test_database(tmp_path: Path) -> Path:
    db_path = tmp_path / "browser-regression.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                activity_type TEXT,
                start_time_gmt TEXT,
                distance_meters REAL,
                elapsed_duration_seconds REAL,
                average_hr REAL,
                max_hr REAL,
                elevation_gain REAL,
                average_speed REAL,
                training_stress_score REAL,
                start_latitude REAL,
                start_longitude REAL
            )
            """
        )
        today = date.today().isoformat()
        conn.executemany(
            """
            INSERT INTO activity VALUES
                (?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                (
                    202,
                    f"{today}T12:00:00",
                    12000,
                    4200,
                    146,
                    174,
                    130,
                    2.86,
                    75,
                ),
                (
                    101,
                    f"{today}T10:00:00",
                    8000,
                    2700,
                    139,
                    168,
                    80,
                    2.96,
                    52,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_server(db_path: Path, port: int) -> subprocess.Popen[str]:
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    environment.pop("PYTEST_CURRENT_TEST", None)
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", SERVER_SCRIPT, str(db_path), str(port)],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    deadline = time.monotonic() + 20
    url = f"http://127.0.0.1:{port}/activities"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.communicate()[0]
            raise AssertionError(f"NiceGUI exited before startup:\n{output}")
        try:
            with urllib.request.urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return process
        except OSError:
            time.sleep(0.1)
    process.terminate()
    output = process.communicate(timeout=10)[0]
    raise AssertionError(f"NiceGUI did not start within 20 seconds:\n{output}")


def _stop_server(process: subprocess.Popen[str]) -> str:
    process.terminate()
    try:
        return process.communicate(timeout=10)[0]
    except subprocess.TimeoutExpired:
        process.kill()
        return process.communicate(timeout=10)[0]


def _chrome_executable() -> Path | None:
    candidates = (
        Path(os.environ.get("PROGRAMFILES", ""))
        / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", ""))
        / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", ""))
        / "Google/Chrome/Application/chrome.exe",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    # Use Playwright-managed Chromium when a local Windows Chrome is absent.
    return None


def _click_activity(page, activity_id: int, interaction_errors: list[str]) -> bool:
    row = page.locator(
        ".ag-center-cols-container .ag-row", has_text=str(activity_id)
    ).first
    try:
        row.locator('.ag-cell[col-id="id"]').click(timeout=10_000)
        page.get_by_text(f"Activity {activity_id}", exact=True).wait_for(
            state="visible", timeout=10_000
        )
    except PlaywrightError as exc:
        interaction_errors.append(f"activity {activity_id}: {exc}")
        return False
    return True


def test_activity_row_clicks_cross_browser_boundary_without_runtime_errors(
    tmp_path: Path,
) -> None:
    db_path = _test_database(tmp_path)
    original_db_hash = hashlib.sha256(db_path.read_bytes()).hexdigest()
    port = _free_loopback_port()
    process = _start_server(db_path, port)
    browser_errors: list[str] = []
    interaction_errors: list[str] = []
    detail_results: list[tuple[int, bool]] = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                executable_path=_chrome_executable(), headless=True
            )
            page = browser.new_page()
            page.on(
                "console",
                lambda message: browser_errors.append(message.text)
                if message.type == "error"
                else None,
            )
            page.on("pageerror", lambda error: browser_errors.append(str(error)))

            def allow_loopback_only(route) -> None:
                parsed = urlsplit(route.request.url)
                if parsed.scheme in {"data", "blob"} or parsed.hostname in {
                    "127.0.0.1",
                    "localhost",
                }:
                    route.continue_()
                else:
                    route.abort()

            page.route("**/*", allow_loopback_only)
            page.goto(
                f"http://127.0.0.1:{port}/activities",
                wait_until="networkidle",
            )
            page.locator(".ag-center-cols-container .ag-row").first.wait_for(
                state="visible", timeout=15_000
            )
            for activity_id in (202, 101, 202):
                detail_results.append(
                    (
                        activity_id,
                        _click_activity(page, activity_id, interaction_errors),
                    )
                )
            page.wait_for_timeout(250)
            page.evaluate("window.socket?.disconnect()")
            page.wait_for_timeout(250)
            browser.close()
    finally:
        server_output = _stop_server(process)

    circular_errors = [
        message
        for message in browser_errors
        if "converting circular structure to json" in message.lower()
    ]
    assert not circular_errors, (
        "activity row click produced the known circular-JSON browser failure; "
        f"details={detail_results}; errors={circular_errors}"
    )
    assert detail_results == [(202, True), (101, True), (202, True)], (
        f"interaction_errors={interaction_errors}; browser_errors={browser_errors}; "
        f"server_output={server_output}"
    )
    assert not browser_errors, f"unexpected browser errors: {browser_errors}"
    assert "Traceback (most recent call last)" not in server_output
    assert "Error while handling event" not in server_output
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == original_db_hash


def test_activity_grid_click_listeners_request_only_plain_row_data() -> None:
    source = Path(pages.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    registrations: dict[str, list[object] | None] = {}
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        if not isinstance(call.func, ast.Attribute) or call.func.attr != "on":
            continue
        if not call.args or not isinstance(call.args[0], ast.Constant):
            continue
        event_name = call.args[0].value
        if event_name not in {"cellClicked", "rowClicked"}:
            continue
        args_keyword = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "args"),
            None,
        )
        registrations[str(event_name)] = (
            ast.literal_eval(args_keyword) if args_keyword is not None else None
        )

    assert registrations == {
        "cellClicked": ["data"],
        "rowClicked": ["data"],
    }
