"""Framework-neutral credential and MFA behavior used by the Sync page."""

from __future__ import annotations

from pathlib import Path

import pytest

from garmin_data_hub.services.garmin_credentials import GarminCredentials
from garmin_data_hub.ui_nicegui import pages
from garmin_data_hub.ui_nicegui.pages import (
    _browser_login_state_exists,
    _credentials_for_sync,
    _mark_saved_login_unavailable,
    _resolve_sync_credentials,
    _sync_waiting_for_mfa,
)


def _sync_page_source() -> str:
    source = Path(pages.__file__).read_text(encoding="utf-8")
    return source.split('    @ui.page("/sync")', maxsplit=1)[1].split(
        '    @ui.page("/query")', maxsplit=1
    )[0]


def _sync_callback_source(name: str, next_name: str) -> str:
    source = _sync_page_source()
    return source.split(f"            {name}", maxsplit=1)[1].split(
        f"            {next_name}", maxsplit=1
    )[0]


def test_entered_sync_credentials_are_validated_and_normalized() -> None:
    credentials, entered = _resolve_sync_credentials(
        "  athlete@example.com  ",
        "current-password",
        None,
    )

    assert credentials == GarminCredentials(
        "athlete@example.com",
        "current-password",
    )
    assert entered is True


@pytest.mark.parametrize("email", ["", "ATHLETE@example.com"])
def test_blank_password_resolves_matching_saved_credential(email: str) -> None:
    saved = GarminCredentials("athlete@example.com", "saved-password")

    credentials, entered = _resolve_sync_credentials(email, "", saved)

    assert credentials is saved
    assert entered is False


@pytest.mark.parametrize(
    ("email", "saved", "message"),
    [
        ("", None, "email and password"),
        ("athlete@example.com", None, "password"),
        (
            "different@example.com",
            GarminCredentials("athlete@example.com", "saved-password"),
            "email shown",
        ),
    ],
)
def test_incomplete_or_mismatched_sync_login_is_rejected(
    email: str,
    saved: GarminCredentials | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _resolve_sync_credentials(email, "", saved)


def test_normal_sync_uses_saved_login_and_ignores_closed_editor_values() -> None:
    saved = GarminCredentials("athlete@example.com", "saved-password")

    credentials, entered = _credentials_for_sync(
        False,
        "different@example.com",
        "stale-editor-password",
        saved,
    )

    assert credentials is saved
    assert entered is False


def test_normal_sync_without_a_saved_login_requests_explicit_update() -> None:
    with pytest.raises(ValueError, match="Update login"):
        _credentials_for_sync(False, "", "", None)


def test_update_login_validates_and_returns_the_entered_credentials() -> None:
    saved = GarminCredentials("old@example.com", "old-password")

    credentials, entered = _credentials_for_sync(
        True,
        "  new@example.com  ",
        "new-password",
        saved,
    )

    assert credentials == GarminCredentials("new@example.com", "new-password")
    assert entered is True


def test_saved_credential_load_failure_opens_required_login_editor() -> None:
    state = {
        "saved_email": "stale@example.com",
        "credential_problem": None,
        "credential_may_exist": True,
        "editing_login": False,
        "login_reset_required": False,
    }

    _mark_saved_login_unavailable(
        state,
        "Credential Manager read failed",
        credential_may_exist=False,
    )

    assert state == {
        "saved_email": None,
        "credential_problem": "Credential Manager read failed",
        "credential_may_exist": False,
        "editing_login": True,
        "login_reset_required": True,
    }


def test_unreadable_saved_credential_can_remain_for_explicit_forget() -> None:
    state = {
        "saved_email": "stale@example.com",
        "credential_problem": None,
        "credential_may_exist": False,
        "editing_login": False,
        "login_reset_required": False,
    }

    _mark_saved_login_unavailable(
        state,
        RuntimeError("Credential Manager is locked"),
        credential_may_exist=True,
    )

    assert state["saved_email"] is None
    assert state["credential_problem"] == "Credential Manager is locked"
    assert state["credential_may_exist"] is True
    assert state["editing_login"] is True
    assert state["login_reset_required"] is True


def test_browser_login_state_detection_is_conservative(tmp_path: Path) -> None:
    assert _browser_login_state_exists(tmp_path) is False

    session_file = tmp_path / "garmin_session.json"
    session_file.write_text("{}", encoding="utf-8")
    assert _browser_login_state_exists(tmp_path) is True

    session_file.unlink()
    (tmp_path / "browser_profile").mkdir()
    assert _browser_login_state_exists(tmp_path) is True


def test_sync_page_hides_login_editor_until_update_is_requested() -> None:
    sync_page = _sync_page_source()

    assert '"Update login"' in sync_page
    assert '"editing_login": not bool(saved_email)' in sync_page
    assert "login_editor" in sync_page
    assert "login_editor.set_visibility" in sync_page
    assert 'editing and bool(state["saved_email"])' in sync_page
    assert 'update_login_button.on("click", edit_login)' in sync_page
    assert "_credentials_for_sync(" in sync_page
    assert "Garmin session ready" in sync_page


def test_login_replacement_clears_password_and_uses_atomic_update() -> None:
    callback = _sync_callback_source("async def save_login", "def forget_login")
    atomic_helper = _sync_callback_source(
        "async def _apply_browser_login_update",
        "async def reset_browser_login",
    )

    password_copy_index = callback.index('str(password.value or "")')
    password_clear_index = callback.index('password.value = ""')
    credential_validation_index = callback.index("GarminCredentials(")
    commit_block = callback.split(
        "def commit_login() -> GarminCredentials:", maxsplit=1
    )[1].split("\n\n                try:", maxsplit=1)[0]

    assert password_copy_index < password_clear_index < credential_validation_index
    assert "save_credentials(" in commit_block
    assert "await _apply_browser_login_update(" in callback
    assert "commit_login," in callback
    assert "job.apply_browser_login_update," in atomic_helper
    assert "reset_confirmed=reset_confirmed" in atomic_helper


def test_run_clears_password_and_resets_conflicting_session_before_use() -> None:
    callback = _sync_callback_source("async def start_sync", "def stop_sync")

    password_copy_index = callback.index("str(password.value or \"\")")
    password_clear_index = callback.index('password.value = ""')
    credential_resolution_index = callback.index("_credentials_for_sync(")
    launch_block = callback.split(
        "def launch_with_entered_login() -> GarminCredentials:", maxsplit=1
    )[1].split("\n\n                try:", maxsplit=1)[0]

    assert password_copy_index < password_clear_index
    assert password_clear_index < credential_resolution_index
    assert "save_credentials(" in launch_block
    assert "delete_credentials()" in launch_block
    assert "job.start(" in launch_block
    assert "await _apply_browser_login_update(" in callback
    assert "launch_with_entered_login," in callback
    assert "_mark_saved_login_unavailable(" in callback

    load_failure = callback.split(
        "except CredentialStoreError as exc:", maxsplit=1
    )[1].split("_notify_error(exc)", maxsplit=1)[0]
    assert "_mark_saved_login_unavailable(" in load_failure
    assert "if not editing_login" not in load_failure


@pytest.mark.parametrize(
    ("log", "expected"),
    [
        ("Credentials submitted, waiting for Garmin...", False),
        ("MFA required - enter code in the browser window...", True),
        ("Still waiting for MFA code...", True),
        ("MFA required\nLogin successful!", False),
        ("MFA required\nLogin failed", False),
        ("Login successful\nMFA required", True),
    ],
)
def test_mfa_waiting_status_tracks_the_latest_login_event(
    log: str,
    expected: bool,
) -> None:
    assert _sync_waiting_for_mfa(log) is expected
