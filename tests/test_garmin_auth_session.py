from __future__ import annotations

from pathlib import Path

import pytest

from garmin_data_hub.services.garmin_auth_session import (
    BrowserSessionResetError,
    reset_garmin_browser_session,
)


def test_reset_removes_only_known_garmin_browser_state(tmp_path: Path) -> None:
    profile = tmp_path / "browser_profile"
    profile.mkdir()
    (profile / "Cookies").write_text("session", encoding="utf-8")
    session = tmp_path / "garmin_session.json"
    session.write_text("{}", encoding="utf-8")
    database = tmp_path / "garmin.db"
    database.write_text("keep", encoding="utf-8")
    legacy_credentials = tmp_path / ".env"
    legacy_credentials.write_text("keep", encoding="utf-8")

    result = reset_garmin_browser_session(tmp_path)

    assert result.removed_browser_profile is True
    assert result.removed_session_file is True
    assert result.removed_anything is True
    assert not profile.exists()
    assert not session.exists()
    assert database.read_text(encoding="utf-8") == "keep"
    assert legacy_credentials.read_text(encoding="utf-8") == "keep"


def test_reset_is_idempotent_when_no_browser_state_exists(tmp_path: Path) -> None:
    result = reset_garmin_browser_session(tmp_path)

    assert result.removed_anything is False


def test_reset_refuses_a_filesystem_root() -> None:
    root = Path(Path.cwd().anchor)

    with pytest.raises(BrowserSessionResetError, match="filesystem root"):
        reset_garmin_browser_session(root)


def test_reset_refuses_unexpected_session_directory(tmp_path: Path) -> None:
    (tmp_path / "garmin_session.json").mkdir()

    with pytest.raises(BrowserSessionResetError, match="found a directory"):
        reset_garmin_browser_session(tmp_path)

