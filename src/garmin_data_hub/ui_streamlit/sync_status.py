from __future__ import annotations

import re


def progress_from_log(log_text: str) -> float:
    """Estimate sync progress from the CLI log output.

    The sync runs in several coarse stages, so a heuristic is more appropriate
    than pretending we have exact percentage completion from Garmin.
    """
    lt = (log_text or "").lower()
    progress = 0.05 if lt.strip() else 0.0

    checkpoints = [
        (("started:", "garmin data sync"), 0.05),
        (("already logged in", "opening in existing browser session", "browser"), 0.10),
        (("fetching ", "fetching profile data", "fetching full-range data"), 0.25),
        (
            (
                "activities page:",
                "fetching per-activity details",
                "downloading fit files",
            ),
            0.50,
        ),
        (("[ok] sync completed",), 0.65),
        (("[ok] app schema applied",), 0.78),
        (("[trackpoints]", "[ok] trackpoints:"), 0.88),
        (("[ok] derived tables refreshed",), 0.96),
        (("[success] sync complete", "process exited with code 0"), 1.0),
    ]

    for markers, value in checkpoints:
        if any(marker in lt for marker in markers):
            progress = max(progress, value)

    return min(max(progress, 0.0), 1.0)


def derived_refresh_summary_from_log(log_text: str) -> dict[str, int] | None:
    """Extract the derived-metrics refresh counts from the CLI log when present."""
    match = re.search(
        r"\[ok\]\s+derived tables refreshed:\s*targets=(\d+),\s*upserted=(\d+),\s*zones=(\d+)",
        log_text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    return {
        "target_activities": int(match.group(1)),
        "rows_upserted": int(match.group(2)),
        "zones_updated": int(match.group(3)),
    }


def sync_completed_from_state(return_code: int | None, log_text: str) -> bool:
    """Determine whether the sync has already finished.

    We trust either the subprocess exit code or the final success marker written
    by the CLI log, which is useful if the UI missed the exact process exit.
    """
    if return_code is not None:
        return True

    lt = (log_text or "").lower()
    return "[success] sync complete" in lt or "process exited with code 0" in lt
