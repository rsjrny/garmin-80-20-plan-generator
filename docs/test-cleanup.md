# Test cleanup

Implemented 2026-10-04 following the [pre-cleanup review](test-suite-review.md).
The review CSV is a historical inventory, not a current collection manifest.

## Changes

- Removed the constant-only artifact-hash assertion and two redundant threshold
  regressions. Existing provenance and traceability checks remain.
- Replaced ten misleading FTP characterization cases with valid running-power
  duration boundaries and explicit tests of current rounding policy. Retained
  non-running rejection, power provenance and cutoff coverage. Current policy
  does not enforce the obsolete 80–500 W range.
- Replaced eight UI source-text checks with actual NiceGUI callback tests covering
  clipboard output and disabled state, Help links, recovery output and empty
  data, offline generation confirmation, workbook output, stale-plan rejection,
  and Data Query shared-job, credential and target-safety behavior.
- Strengthened ARCH-REV-004 with independently computed hashes, runtime/UI
  invariance and prescription sensitivity. ARCH-REV-011 now inspects an approved
  revision before and after a real persisted activity match and evaluation.
- Added controlled signed/unsigned PowerShell release execution and final-byte
  checksum verification. The ordering check locates the signing invocation.
  Compiler, signing and removal commands are stand-ins inside a temporary test
  directory; this does not validate a production signature or full installer.
- Replaced two forwarding-function mock checks with archive-to-SQLite ingestion
  tests using upstream record conversion and both archive/member ID routing.
  Only binary FIT decoding is substituted.
- Kept all fourteen Chromium scenarios. Replaced fifty fixed browser sleeps with
  observable state waits or rendering/layout waits. Retained cross-tab, chart,
  map, stale-editor and responsive-layout assertions.
- Moved screenshots and browser server logs to per-test directories. CI can
  retain failure screenshots without modifying tracked reports.
- Extracted shared approved-revision fixtures and NiceGUI test support. Restored
  imported app modules after NiceGUI route teardown to preserve suite isolation.
  Updated obsolete checkpoint descriptions in the affected threshold/sync tests.
- Split CI into Linux non-browser, Linux browser, and Windows release/credential/
  packaged-runtime/process jobs. Registered the browser marker; ordinary pytest
  still runs everything. All jobs should be required in branch protection.

## Running tests

Install the dev extra. Chromium is needed for the browser group:

~~~powershell
python -m pip install -e ".[dev]"
python -m playwright install chromium
python -m pytest -m "not browser"
python -m pytest -m browser
python -m pytest
~~~

Set GARMIN_TEST_ARTIFACTS to retain browser diagnostics outside the default
per-test temporary directories. CI uses .tools/browser-artifacts and uploads
screenshots when the browser job fails. These images are diagnostics, not
visual-difference tests. PowerShell tests skip when the shell is unavailable;
Windows CI executes them.

## Validation

Full combined local run: **1,454 passed in 413.17 seconds (6 minutes 53 seconds)**,
exit status 0. No cases failed, errored or skipped; all fourteen browser scenarios
were retained. The earlier non-browser run passed 1,440 cases in 236.26 seconds.
The cleaned suite has one more collected case than the pre-cleanup audit because
behavioral replacements add positive, negative and state-variant coverage.

The JUnit report and execution logs are retained under .tools as local audit
artifacts. Documentation links, CI job/test targets and git diff --check passed.
Tracked reports were unchanged by the cleaned test runs.

Controlled process-local probes now fail all three strengthened assertions when
hash generation, runtime evaluation, or the actual signing invocation is removed.
The probe changes no production files and intentionally exits with test failures.

Local verification uses Windows and the workspace Python environment. A fresh
Linux/Python 3.11 GitHub run remains necessary to confirm the changed CI workflow.
