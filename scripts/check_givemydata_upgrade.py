"""Audit a givemydata candidate on disposable copies; never approve an upgrade."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def read_only(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def backup(source, destination):
    # SQLite's online backup includes committed WAL contents.
    with contextlib.closing(read_only(source)) as src:
        with contextlib.closing(sqlite3.connect(destination)) as dst:
            src.backup(dst)


def schema(path):
    with contextlib.closing(read_only(path)) as conn:
        objects = {}
        for kind, name, table, sql in conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ):
            item = {"type": kind, "table": table, "sql": sql}
            if kind == "table":
                item["columns"] = [list(r) for r in conn.execute(
                    f"PRAGMA table_xinfo({quote(name)})"
                )]
                item["foreign_keys"] = [list(r) for r in conn.execute(
                    f"PRAGMA foreign_key_list({quote(name)})"
                )]
            objects[name] = item
        return objects


def diff(before, after):
    removed = sorted(before.keys() - after.keys())
    added = sorted(after.keys() - before.keys())
    changed = {}
    for name in sorted(before.keys() & after.keys()):
        old, new = before[name], after[name]
        if old == new:
            continue
        item = {"before": old, "after": new}
        if old["type"] == new["type"] == "table":
            old_cols = {r[1]: r[2:] for r in old["columns"]}
            new_cols = {r[1]: r[2:] for r in new["columns"]}
            item["removed_columns"] = sorted(old_cols.keys() - new_cols.keys())
            item["added_columns"] = sorted(new_cols.keys() - old_cols.keys())
            item["changed_columns"] = sorted(
                c for c in old_cols.keys() & new_cols.keys() if old_cols[c] != new_cols[c]
            )
        changed[name] = item
    return {"added": added, "removed": removed, "changed": changed}


def breaking_changes(delta):
    problems = [f"Removed schema object: {name}" for name in delta["removed"]]
    for name, item in delta["changed"].items():
        if item["before"]["type"] != "table" or item["after"]["type"] != "table":
            problems.append(f"Changed schema object requires review: {name}")
        elif (item["removed_columns"] or item["changed_columns"]
              or item["before"]["foreign_keys"] != item["after"]["foreign_keys"]):
            problems.append(f"Incompatible table change: {name}")
        elif re.sub(r"\s+", " ", item["before"]["sql"] or "") != re.sub(
            r"\s+", " ", item["after"]["sql"] or ""
        ) and not item["added_columns"]:
            problems.append(f"Changed table constraints require review: {name}")
    return problems


def app_schema_names():
    from garmin_data_hub.paths import read_schema_sql
    from garmin_data_hub.db import migrate
    source = read_schema_sql() + "\n" + Path(migrate.__file__).read_text(encoding="utf-8")
    return set(re.findall(
        r"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|VIEW|INDEX|TRIGGER)\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_]\w*)", source, re.I
    ))


def fingerprints(path, tables):
    """Order-independent row digests; reports contain no row values."""
    result = {}
    with contextlib.closing(read_only(path)) as conn:
        existing = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        for table in sorted(tables & existing):
            count, accumulator = 0, 0
            for row in conn.execute(f"SELECT * FROM {quote(table)}"):
                payload = json.dumps(list(row), default=lambda v: {"bytes": v.hex()},
                                     ensure_ascii=True, separators=(",", ":"))
                accumulator = (accumulator + int.from_bytes(
                    hashlib.sha256(payload.encode()).digest(), "big"
                )) % (1 << 256)
                count += 1
            result[table] = {"rows": count, "digest": f"{accumulator:064x}"}
    return result


def integrity(path):
    with contextlib.closing(read_only(path)) as conn:
        issues = [str(r[0]) for r in conn.execute("PRAGMA integrity_check") if r[0] != "ok"]
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            issues.append(f"Foreign key violations: {len(violations)}")
        return issues


def smoke(path):
    """Check app entry points plus SQL that some UI helpers suppress."""
    from garmin_data_hub.db.migrate import apply_schema
    from garmin_data_hub.db.sqlite import connect_sqlite
    from garmin_data_hub.analytics.post_sync_refresh import refresh_exact_activity_metrics
    from garmin_data_hub.ui_nicegui import data
    from garmin_data_hub.analytics.chart_overview import prepare_overview, overview_figures
    results = []
    errors = []

    def check(name, action):
        try:
            action()
            results.append({"check": name, "status": "passed"})
        except Exception as exc:
            errors.append(name)
            results.append({"check": name, "status": "failed", "error_type": type(exc).__name__})

    conn = connect_sqlite(path)
    try:
        check("app migrations", lambda: apply_schema(conn))
        ids = []

        def activity_contract():
            nonlocal ids
            conn.execute("SELECT activity_id, activity_type, start_time_gmt, "
                         "distance_meters, elapsed_duration_seconds, moving_duration_seconds, "
                         "average_speed, average_hr, max_hr, elevation_gain, "
                         "training_stress_score FROM activity LIMIT 0")
            ids = [r[0] for r in conn.execute(
                "SELECT activity_id FROM activity ORDER BY activity_id DESC LIMIT 10"
            )]
        check("required activity columns", activity_contract)

        def refresh():
            summary = refresh_exact_activity_metrics(conn, ids)
            if summary.get("errors", 0):
                raise RuntimeError("Metric refresh reported failures")
        check("sample activity metric refresh", refresh)
        check("dashboard", lambda: data.dashboard_data(path))

        def activities():
            rows = data.list_activities(path, limit=10)
            if ids and not rows:
                raise RuntimeError("Activity list silently returned no rows")
            for activity_id in ids:
                if data.activity_detail(path, activity_id) is None:
                    raise RuntimeError("Activity detail unavailable")
        check("activity list and details", activities)

        def charts():
            frame = data.chart_dataframe(path, start_date="1900-01-01")
            expected = conn.execute(
                "SELECT COUNT(*) FROM activity WHERE start_time_gmt >= '1900-01-01'"
            ).fetchone()[0]
            if expected and frame.empty:
                raise RuntimeError("Chart query silently returned no rows")
            from datetime import date
            prepared = prepare_overview(frame, date(1900, 1, 1), date.today())
            overview_figures(prepared)
        check("chart data and figures", charts)
        check("planning rows", lambda: data.plan_rows(path))
        check("workout compliance", lambda: data.compliance_data(path))
    finally:
        conn.close()
    errors.extend(integrity(path))
    return {"checks": results, "problems": errors, "sample_limit": 10,
            "sample_activity_count": len(ids)}


def worker(mode, database, result_file, days):
    """Runs with the candidate interpreter; production version guards stay intact."""
    from garmin_data_hub.cli_backup_ingest import (
        _upstream_privacy_boundary, _transient_worker_credentials,
    )
    credentials = {k: os.environ.pop(k) for k in ("GARMIN_EMAIL", "GARMIN_PASSWORD")
                   if os.environ.get(k)}
    version = importlib.metadata.version("garmin-givemydata")
    import logging

    class ErrorCounter(logging.Handler):
        errors = 0

        def emit(self, record):
            if record.levelno >= logging.ERROR:
                self.errors += 1

    counter = ErrorCounter()
    with _upstream_privacy_boundary(tuple(credentials.values())):
        logging.getLogger().addHandler(counter)
        if mode == "init":
            from garmin_mcp.db import init_db
            with contextlib.closing(sqlite3.connect(database)) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA foreign_keys=ON")
                init_db(conn)
                conn.commit()
        else:
            with _transient_worker_credentials(credentials):
                from seleniumbase.core import browser_launcher
                driver_dir = database.parent / "drivers"
                driver_dir.mkdir()
                browser_launcher.override_driver_dir(str(driver_dir))
                from garmin_givemydata import main
                sys.argv = ["garmin-givemydata", "--days", str(days), "--no-trackpoints", "--no-files"]
                try:
                    code = main()
                except SystemExit as exc:
                    code = exc.code
                if code is not None and (type(code) is not int or code != 0):
                    raise RuntimeError("Candidate sync failed")
    result_file.write_text(json.dumps({"version": version, "status": "completed",
                                      "logged_errors": counter.errors}), encoding="utf-8")


def run_worker(python, mode, database, result_file, *, days=7, timeout=1800):
    from garmin_data_hub.cli_backup_ingest import _build_garmin_worker_environment
    environment = _build_garmin_worker_environment(database.parent)
    environment["GARMIN_DATA_HUB_DB"] = str(database)
    if mode == "sync":
        from garmin_data_hub.services.garmin_credentials import load_credentials, build_sync_environment
        credentials = load_credentials()
        if credentials is None:
            raise RuntimeError("Save Garmin credentials in the app before requesting --sync")
        environment = build_sync_environment(credentials, base_environment=environment)
    command = [str(python), "-I", str(Path(__file__).resolve()), "--_worker", mode,
               "--db", str(database), "--result", str(result_file), "--days", str(days)]
    from garmin_data_hub.ui_nicegui.process_tree import (
        attach_process_tree, fallback_process_tree, popen_process_tree_kwargs,
    )
    process = subprocess.Popen(command, cwd=database.parent, env=environment,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               **popen_process_tree_kwargs())
    tree = None
    try:
        tree = attach_process_tree(process)
        code = process.wait(timeout=timeout)
    except BaseException:
        (tree or fallback_process_tree(process)).terminate(process, timeout=10)
        raise
    finally:
        if tree is not None:
            tree.close_after_exit()
    if code or not result_file.is_file():
        raise RuntimeError(f"Candidate {mode} failed (exit {code}); "
                           "candidate API or dependencies may be incompatible")
    return json.loads(result_file.read_text(encoding="utf-8"))


def audit(source, python, output, *, sync=False, days=7, timeout=1800):
    source, python, output = source.resolve(), python.resolve(), output.resolve()
    if not source.is_file():
        raise ValueError("Source database does not exist")
    if not python.is_file():
        raise ValueError("Candidate Python executable does not exist")
    if output == source.parent or output in source.parents or source.is_relative_to(output):
        raise ValueError("Output must be separate from the source database directory")
    output.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="givemydata-upgrade-", dir=output))
    backup_path = run / "backup.sqlite"
    sandbox = run / "candidate"
    fresh_dir = run / "fresh"
    sandbox.mkdir()
    fresh_dir.mkdir()
    candidate = sandbox / "garmin.db"
    fresh = fresh_dir / "garmin.db"
    report = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(source), "backup": str(backup_path),
        "sandbox": str(candidate), "live_sync_requested": sync,
        "live_sync_completed": False, "status": "failed", "problems": [],
        "limitations": [
            "Passing checks do not approve or install an upgrade.",
            "Offline initialization cannot validate Garmin login, API responses, or download behavior.",
            "Metric refresh samples at most ten activities; UI rendering and all historical data are not exhaustively tested.",
            "Schema comparison cannot detect changed units or meanings in unchanged columns.",
            "Use a trusted candidate package; this is path isolation, not an operating-system security sandbox.",
        ],
    }
    problems = report["problems"]
    stage = "backup and baseline integrity"
    try:
        backup(source, backup_path)
        backup(backup_path, candidate)
        before = schema(backup_path)
        report["before_schema"] = before
        problems.extend(integrity(backup_path))
        owned = app_schema_names()
        original_rows = fingerprints(backup_path, owned)
        report["app_rows_before"] = original_rows
        stage = "fresh candidate initialization"
        candidate_info = run_worker(python, "init", fresh, run / "fresh-result.json",
                                    timeout=timeout)
        report["candidate_version"] = candidate_info["version"]
        if candidate_info.get("logged_errors", 0):
            problems.append("Candidate logged errors initializing the fresh database")
        from garmin_data_hub.cli_backup_ingest import _SUPPORTED_GIVEMYDATA_VERSION
        report["supported_version"] = _SUPPORTED_GIVEMYDATA_VERSION
        report["version_changed"] = candidate_info["version"] != _SUPPORTED_GIVEMYDATA_VERSION
        new_schema = schema(fresh)
        report["fresh_candidate_schema"] = new_schema
        upstream_before = {k: v for k, v in before.items() if v["table"] not in owned}
        upstream_after = {k: v for k, v in new_schema.items() if v["table"] not in owned}
        report["fresh_schema_diff"] = diff(upstream_before, upstream_after)
        problems.extend(breaking_changes(report["fresh_schema_diff"]))
        stage = "candidate initialization on copy"
        info = run_worker(python, "init", candidate, run / "copy-result.json", timeout=timeout)
        if info["version"] != report["candidate_version"]:
            raise RuntimeError("Candidate version changed during the audit")
        if info.get("logged_errors", 0):
            problems.append("Candidate logged errors initializing the copy")

        def check_copy(label):
            after = schema(candidate)
            report[label + "_schema_diff"] = diff(before, after)
            problems.extend(breaking_changes(report[label + "_schema_diff"]))
            rows = fingerprints(candidate, owned)
            report[label + "_app_rows"] = rows
            if original_rows != rows:
                problems.append("Candidate changed or removed app-owned rows")
            # Updates/new activities are expected; losing existing identities is not.
            with contextlib.closing(read_only(candidate)) as conn:
                conn.execute("ATTACH DATABASE ? AS baseline",
                             (backup_path.resolve().as_uri() + "?mode=ro",))
                missing = conn.execute(
                    "SELECT COUNT(*) FROM baseline.activity b WHERE NOT EXISTS "
                    "(SELECT 1 FROM main.activity a WHERE a.activity_id=b.activity_id)"
                ).fetchone()[0]
                if missing:
                    problems.append(f"Candidate removed existing activity identities: {missing}")
            problems.extend(integrity(candidate))
            smoke_path = run / (label + "-smoke.sqlite")
            backup(candidate, smoke_path)
            report[label + "_app_smoke"] = smoke(smoke_path)
            problems.extend(report[label + "_app_smoke"]["problems"])

        stage = "offline app compatibility"
        check_copy("offline")
        if sync and not problems:
            stage = "candidate live sync"
            with contextlib.closing(read_only(candidate)) as conn:
                last_sync_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sync_log").fetchone()[0]
            info = run_worker(python, "sync", candidate, run / "sync-result.json",
                              days=days, timeout=timeout)
            if info["version"] != report["candidate_version"]:
                raise RuntimeError("Candidate version changed during sync")
            report["live_sync_completed"] = True
            with contextlib.closing(read_only(candidate)) as conn:
                entries = conn.execute(
                    "SELECT status FROM sync_log WHERE id > ?", (last_sync_id,)
                ).fetchall()
            report["new_sync_log_entries"] = len(entries)
            if not entries or any(r[0] != "ok" for r in entries):
                problems.append("Candidate did not record a successful sync")
            if info.get("logged_errors", 0):
                problems.append("Candidate logged errors during sync")
            stage = "post-sync app compatibility"
            check_copy("post_sync")
        elif sync:
            report["live_sync_skipped"] = "Offline checks found problems"
        if not problems:
            report["status"] = "checks_passed_review_required"
    except KeyboardInterrupt:
        problems.append(f"Audit interrupted during {stage}")
    except Exception as exc:
        problems.append(f"Audit could not complete during {stage}: {type(exc).__name__}")
    finally:
        (run / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        lines = ["# givemydata upgrade audit", "", f"Status: {report['status']}",
                 f"Candidate version: {report.get('candidate_version', 'unavailable')}",
                 f"Live sync completed: {report['live_sync_completed']}", "",
                 "## Findings", ""]
        lines.extend([f"- {p}" for p in problems] or ["- Automated checks passed; review is still required."])
        lines.extend(["", "Detailed schemas and checks: [report.json](report.json)", ""])
        for label in ("offline", "post_sync"):
            checks = report.get(label + "_app_smoke")
            if checks:
                lines.extend([f"## {label} app checks", "",
                              f"Activities sampled: {checks['sample_activity_count']}", ""])
                lines.extend(f"- {c['check']}: {c['status']}" for c in checks["checks"])
        lines.extend(["", "## Limits", ""] + [f"- {p}" for p in report["limitations"]])
        (run / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return run, report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Existing production database (read only)")
    parser.add_argument("--candidate-python", type=Path, default=Path(sys.executable),
                        help="Python in a separate environment containing the candidate package")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "givemydata-upgrades")
    parser.add_argument("--sync", action="store_true", help="Also perform a real Garmin sync on the copy")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--timeout", type=int, default=1800, help="Candidate worker timeout in seconds")
    parser.add_argument("--_worker", choices=("init", "sync"), help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.days < 1 or args.timeout < 1:
        parser.error("--days and --timeout must be positive")
    if args._worker:
        worker(args._worker, args.db.resolve(), args.result.resolve(), args.days)
        return 0
    try:
        run, report = audit(args.db, args.candidate_python, args.output, sync=args.sync,
                            days=args.days, timeout=args.timeout)
    except (ValueError, OSError) as exc:
        print(f"Cannot start audit: {exc}", file=sys.stderr)
        return 2
    print(f"{report['status']}: {run / 'report.md'}")
    print(f"Backup: {run / 'backup.sqlite'}")
    return 0 if report["status"] == "checks_passed_review_required" else 1


if __name__ == "__main__":
    raise SystemExit(main())
