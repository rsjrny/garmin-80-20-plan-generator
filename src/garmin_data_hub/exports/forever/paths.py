from __future__ import annotations

import os
import subprocess
from pathlib import Path

APP_NAME = "GarminDataHub"


def _git_branch_name() -> str | None:
    for env_name in ("GARMIN_DATA_HUB_BRANCH", "GIT_BRANCH", "BRANCH", "GITHUB_REF"):
        val = os.getenv(env_name)
        if not val:
            continue
        if env_name == "GITHUB_REF" and val.startswith("refs/heads/"):
            return val.split("/")[-1]
        return val

    try:
        repo_root = Path(__file__).resolve().parent.parent.parent
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        branch = result.stdout.strip()
        if branch:
            return branch
    except Exception:
        return None
    return None


def windows_appdata_dir() -> Path:
    branch = _git_branch_name()
    app_folder = APP_NAME if not branch else f"{APP_NAME}-{branch}"
    app_folder = app_folder.replace(os.path.sep, "-").replace(" ", "-")
    return Path.home() / "AppData" / "Local" / app_folder


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p
