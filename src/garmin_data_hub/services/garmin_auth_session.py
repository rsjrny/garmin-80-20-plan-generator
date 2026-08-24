"""Safe management of Garmin's persisted local browser login state."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


GARMIN_BROWSER_PROFILE_NAME = "browser_profile"
GARMIN_SESSION_FILE_NAME = "garmin_session.json"


class BrowserSessionResetError(RuntimeError):
    """Garmin browser state could not be reset safely."""


@dataclass(frozen=True)
class BrowserSessionResetResult:
    """Summary of local Garmin login state removed by an explicit reset."""

    removed_browser_profile: bool
    removed_session_file: bool

    @property
    def removed_anything(self) -> bool:
        return self.removed_browser_profile or self.removed_session_file


def _validated_data_directory(data_directory: Path) -> Path:
    root = Path(data_directory).resolve()
    if root.parent == root:
        raise BrowserSessionResetError(
            "Refusing to reset Garmin browser state in a filesystem root"
        )
    return root


def _refuse_link(target: Path) -> None:
    is_junction = getattr(target, "is_junction", lambda: False)()
    if target.is_symlink() or is_junction:
        raise BrowserSessionResetError(
            f"Refusing to remove linked Garmin browser state: {target}"
        )


def reset_garmin_browser_session(
    data_directory: Path,
) -> BrowserSessionResetResult:
    """Remove only the known browser profile and cookie-backup paths.

    The caller must obtain explicit user confirmation and ensure no sync is
    active before calling this function.
    """
    root = _validated_data_directory(data_directory)
    profile_path = root / GARMIN_BROWSER_PROFILE_NAME
    session_path = root / GARMIN_SESSION_FILE_NAME
    _refuse_link(profile_path)
    _refuse_link(session_path)
    if session_path.is_dir():
        raise BrowserSessionResetError(
            f"Expected a Garmin session file but found a directory: {session_path}"
        )

    removed_profile = False
    removed_session = False
    try:
        if profile_path.is_dir():
            shutil.rmtree(profile_path)
            removed_profile = True
        elif profile_path.exists():
            profile_path.unlink()
            removed_profile = True

        if session_path.exists():
            session_path.unlink()
            removed_session = True
    except BrowserSessionResetError:
        raise
    except OSError as exc:
        raise BrowserSessionResetError(
            f"Could not reset Garmin browser login state: {exc}"
        ) from exc

    return BrowserSessionResetResult(
        removed_browser_profile=removed_profile,
        removed_session_file=removed_session,
    )
