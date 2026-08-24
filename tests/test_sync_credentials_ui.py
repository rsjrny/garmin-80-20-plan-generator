"""Framework-neutral credential and MFA behavior used by the Sync page."""

from __future__ import annotations

import pytest

from garmin_data_hub.services.garmin_credentials import GarminCredentials
from garmin_data_hub.ui_nicegui.pages import (
    _resolve_sync_credentials,
    _sync_waiting_for_mfa,
)


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
