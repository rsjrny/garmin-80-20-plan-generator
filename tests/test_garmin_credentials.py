from __future__ import annotations

import os
import sys

import pytest

from garmin_data_hub.services import garmin_credentials
from garmin_data_hub.services.garmin_credentials import (
    CredentialStoreUnavailable,
    GarminCredentials,
    WindowsCredentialStore,
    build_sync_environment,
    credential_status,
    delete_credentials,
    load_credentials,
    save_credentials,
)


class FakeStore:
    backend_name = "Test credential store"

    def __init__(self, credentials: GarminCredentials | None = None):
        self.credentials = credentials

    def load(self) -> GarminCredentials | None:
        return self.credentials

    def save(self, credentials: GarminCredentials) -> None:
        self.credentials = credentials

    def delete(self) -> bool:
        existed = self.credentials is not None
        self.credentials = None
        return existed


class FakeWindowsApi:
    def __init__(self, credentials: GarminCredentials | None = None):
        self.credentials = credentials
        self.calls: list[tuple[str, str]] = []

    def read(self, target_name: str) -> GarminCredentials | None:
        self.calls.append(("read", target_name))
        return self.credentials

    def write(self, target_name: str, credentials: GarminCredentials) -> None:
        self.calls.append(("write", target_name))
        self.credentials = credentials

    def delete(self, target_name: str) -> bool:
        self.calls.append(("delete", target_name))
        existed = self.credentials is not None
        self.credentials = None
        return existed


def test_credentials_are_normalized_and_password_is_not_in_repr() -> None:
    credentials = GarminCredentials("  athlete@example.com  ", "top-secret")

    assert credentials.email == "athlete@example.com"
    assert credentials.password == "top-secret"
    assert "top-secret" not in repr(credentials)


@pytest.mark.parametrize(
    ("email", "password"),
    [
        ("", "secret"),
        ("   ", "secret"),
        ("athlete@example.com", ""),
        ("athlete\x00@example.com", "secret"),
        ("athlete@example.com", "secret\x00suffix"),
    ],
)
def test_invalid_credentials_are_rejected(email: str, password: str) -> None:
    with pytest.raises(ValueError):
        GarminCredentials(email, password)


def test_save_load_status_and_delete_use_injected_store() -> None:
    store = FakeStore()

    saved = save_credentials(" athlete@example.com ", "secret", store=store)

    assert saved == GarminCredentials("athlete@example.com", "secret")
    assert load_credentials(store=store) == saved
    assert credential_status(store=store) == garmin_credentials.CredentialStatus(
        is_configured=True,
        email="athlete@example.com",
        backend_name="Test credential store",
    )
    assert delete_credentials(store=store) is True
    assert delete_credentials(store=store) is False
    assert credential_status(store=store).is_configured is False


def test_windows_store_uses_one_fixed_generic_credential_target() -> None:
    api = FakeWindowsApi()
    store = WindowsCredentialStore(api=api)
    credentials = GarminCredentials("athlete@example.com", "secret")

    store.save(credentials)
    assert store.load() == credentials
    assert store.delete() is True

    assert api.calls == [
        ("write", garmin_credentials.WINDOWS_CREDENTIAL_TARGET),
        ("read", garmin_credentials.WINDOWS_CREDENTIAL_TARGET),
        ("delete", garmin_credentials.WINDOWS_CREDENTIAL_TARGET),
    ]


def test_build_sync_environment_overlays_a_copy_without_global_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNCHANGED_GLOBAL", "global-value")
    monkeypatch.delenv("GARMIN_EMAIL", raising=False)
    monkeypatch.delenv("GARMIN_PASSWORD", raising=False)
    base = {
        "PATH": "example-path",
        "GARMIN_EMAIL": "stale-email",
        "GARMIN_PASSWORD": "stale-password",
    }
    credentials = GarminCredentials("athlete@example.com", "current-password")

    environment = build_sync_environment(credentials, base_environment=base)

    assert environment == {
        "PATH": "example-path",
        "GARMIN_EMAIL": "athlete@example.com",
        "GARMIN_PASSWORD": "current-password",
    }
    assert base["GARMIN_EMAIL"] == "stale-email"
    assert base["GARMIN_PASSWORD"] == "stale-password"
    assert "GARMIN_EMAIL" not in os.environ
    assert "GARMIN_PASSWORD" not in os.environ
    assert os.environ["UNCHANGED_GLOBAL"] == "global-value"


def test_build_sync_environment_uses_a_copy_of_process_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRESERVED", "yes")
    credentials = GarminCredentials("athlete@example.com", "secret")

    environment = build_sync_environment(credentials)

    assert environment["PRESERVED"] == "yes"
    assert environment["GARMIN_EMAIL"] == "athlete@example.com"
    assert environment["GARMIN_PASSWORD"] == "secret"
    assert "GARMIN_EMAIL" not in os.environ
    assert "GARMIN_PASSWORD" not in os.environ


def test_default_store_reports_unavailable_away_from_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(garmin_credentials.sys, "platform", "linux")

    with pytest.raises(CredentialStoreUnavailable, match="only available on Windows"):
        garmin_credentials.default_credential_store()


@pytest.mark.skipif(sys.platform != "win32", reason="requires native WinCred")
def test_native_windows_credential_api_can_be_loaded() -> None:
    store = garmin_credentials.default_credential_store()

    assert store.backend_name == "Windows Credential Manager"
