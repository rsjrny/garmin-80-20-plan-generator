# Checking a garmin-givemydata upgrade

The application currently supports exactly garmin-givemydata 0.1.12. Its normal
sync refuses other versions. Use this checker **before changing that pin**, with
a trusted candidate installed in a separate Python environment. It records the
candidate version and whether it differs from the app's supported version.

Run the checker from the repository using the application's Python environment.
The candidate interpreter only runs upstream initialization and, when requested,
upstream sync. App compatibility checks use the application's existing runtime.

## Prepare a separate candidate environment

Replace X.Y.Z with the exact version you want to evaluate:

~~~powershell
.\.venv\Scripts\python.exe -m venv .tools\givemydata-candidate
.\.tools\givemydata-candidate\Scripts\python.exe -m pip install -e .
.\.tools\givemydata-candidate\Scripts\python.exe -m pip install --upgrade "garmin-givemydata==X.Y.Z"
~~~

The final command intentionally changes the pinned dependency **inside the
disposable candidate environment**. Do not run it against the application's
normal environment. Do not change pyproject.toml or the production version guard
just to make this checker run.

## Run offline checks

Replace the database path with your actual database:

~~~powershell
.\.venv\Scripts\python.exe scripts\check_givemydata_upgrade.py --db "C:\path\to\garmin.db" --candidate-python ".tools\givemydata-candidate\Scripts\python.exe"
~~~

You can omit --candidate-python to check the currently installed version first.
The checker runs even if that version still matches the supported version, so
you can establish a baseline.

Each run creates a unique directory under reports/givemydata-upgrades. Override
the parent directory with --output; it must be separate from the source database
directory. Artifacts include:

- backup.sqlite: a consistent SQLite online backup, including committed WAL data.
- fresh/garmin.db: the candidate's schema initialized from an empty database.
- candidate/garmin.db: the copied database after upstream initialization.
- offline-smoke.sqlite: a separate copy used for app migrations and metric refresh.
- report.json and report.md: schema changes, preservation checks, app checks,
  candidate version, outcome, and limitations.

The source database is opened read only. The checker neither installs packages,
changes the app's version pin, nor copies results back into production.

## Include a real Garmin sync

Save your credentials in the application's Windows Credential Manager first,
then add --sync. A browser login may still be required for the isolated session:

~~~powershell
.\.venv\Scripts\python.exe scripts\check_givemydata_upgrade.py --db "C:\path\to\garmin.db" --candidate-python ".tools\givemydata-candidate\Scripts\python.exe" --sync --days 7
~~~

Sync only starts if offline schema, integrity, preservation, and app checks pass.
It uses the copied database and its own data directory and browser session.
It passes --no-trackpoints and --no-files to test summary ingestion without
redownloading the entire historical archive into an empty sandbox. Credentials use
the existing privacy boundary; they are not placed on command lines or in the
reports. Upstream output is suppressed. Candidate child processes are cleaned up
on timeout or interruption. --timeout sets the per-worker limit in seconds
(default 1800).

After sync, the checker requires a new successful sync_log entry, flags upstream
error logs without retaining their contents, and repeats the schema, preservation,
integrity, and app checks. A zero process exit alone is insufficient. It does not replace the full production archive-ingestion pipeline:
downloaded FIT/GPX/TCX parsing and browser UI behavior need separate validation.

## Interpreting results

Exit codes:

- 0: checks_passed_review_required. The selected automated checks passed.
- 1: the audit found problems or could not complete. Inspect the report.
- 2: invalid arguments or the audit could not start.

A fresh candidate schema is compared with existing upstream objects, so removed
or changed columns cannot be hidden by CREATE TABLE IF NOT EXISTS. The copied
database is also checked for changed schema objects, foreign key violations,
lost activity identities, and altered app-owned rows (including plans).
Additions are reported for review.

App checks cover current migrations, required activity columns, metric refresh
for up to ten activities, dashboard reads, activity list/details, chart data and
figure construction, planning rows, and workout compliance. Failed SQL cannot
pass solely because a UI helper returns an empty fallback.

Passing is **not an upgrade approval**. Review changed constraints, newly added
required columns, changed units/meaning, candidate release notes, the complete
sync/archive workflow, UI behavior, and the broader app test suite before
updating the supported pin and rebuilding the packaged app. Offline checks
cannot validate Garmin's API or authentication. Empty or sparse databases
provide less coverage. A changed upstream initialization API fails the audit
and requires updating this checker.

Use trusted candidate packages. Environment paths separate ordinary operations;
they do not prevent arbitrary candidate code from accessing other files.
Backups contain your personal Garmin and planning data; keep the report folder
private.
