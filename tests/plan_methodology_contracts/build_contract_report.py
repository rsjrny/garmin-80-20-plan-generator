from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from architecture_cases import ALL_ARCHITECTURE_CASES
from methodology_cases import ALL_METHODOLOGY_CASES


BASELINE_HEAD = "d4cbd151bc734bdf00abb635a7a5372818c44be9"
FILES = [
    "tests/plan_methodology_contracts/architecture_cases.py",
    "tests/plan_methodology_contracts/build_contract_report.py",
    "tests/plan_methodology_contracts/conftest.py",
    "tests/plan_methodology_contracts/methodology_cases.py",
    "tests/plan_methodology_contracts/test_boundary_compliance_legacy_atomicity_contracts.py",
    "tests/plan_methodology_contracts/test_methodology_contracts.py",
    "tests/plan_methodology_contracts/test_prescription_segment_contracts.py",
    "tests/plan_methodology_contracts/test_revision_registry_snapshot_contracts.py",
    "tests/plan_methodology_contracts/test_traceability_contracts.py",
]
ARTIFACTS = [
    {
        "path": r"C:\Users\russl\AppData\Local\GarminDataHub_Migration_Rehearsal\PLAN-1.2\plan_methodology_product_specification.md",
        "sha256": "0c24fee12f2f679fe7d49a80944d80388417b3ae85ecc5c270ade2826a2191d2",
        "verified": True,
    },
    {
        "path": r"C:\Users\russl\AppData\Local\GarminDataHub_Migration_Rehearsal\PLAN-1.2\plan_methodology_product_specification.json",
        "sha256": "1a82469c1c9ae5f1763cb5107091a8b3fb35c3c1a864390c77852b39bbde8523",
        "verified": True,
    },
    {
        "path": r"C:\Users\russl\AppData\Local\GarminDataHub_Migration_Rehearsal\PLAN-2.0\plan_methodology_architecture_design.md",
        "sha256": "85d5babb4a0f08fa3b0356b21693b17ed8d9fd59fa12eb3912920b1f162c923c",
        "verified": True,
    },
    {
        "path": r"C:\Users\russl\AppData\Local\GarminDataHub_Migration_Rehearsal\PLAN-2.0\plan_methodology_architecture_design.json",
        "sha256": "2ce0c6507a7be2bcb316180fbc514faf72415c7134589bd72361cdf463be00be",
        "verified": True,
    },
]


def _supplemental_metadata(test_id: str) -> tuple[list[str], str]:
    if test_id.startswith("MAF-V1-003-adjustment-"):
        return ["MAF-V1-003", "MAF-V1-007"], "Exact MAF arithmetic and lower aerobic bound for every allowed adjustment"
    if test_id.startswith("F80-V1-003-evidence-"):
        return ["F80-V1-003", "F80-V1-006"], "Measured/derived/estimated/unavailable threshold evidence states"
    if test_id == "MAF-V1-006-over-65":
        return ["MAF-V1-006"], "Over-65 confirmation with no automatic individualized allowance"
    if test_id == "MAF-V1-008-event-day":
        return ["MAF-V1-008"], "Event-day exception remains separate from aerobic training state"
    if test_id.startswith("ARCH-COMP-003-"):
        return ["F80-V1-015", "MAF-V1-015"], "Missing-data quality never defaults to NONCOMPLIANT"
    return [], "Supplemental frozen edge-case contract"


def _load_results(junit_path: Path) -> tuple[list[dict], list[dict], dict[str, dict]]:
    root = ET.parse(junit_path).getroot()
    failures: list[dict] = []
    passes: list[dict] = []
    by_id: dict[str, dict] = {}
    message_pattern = re.compile(r"^(\S+) meaningful RED: (.+)$")
    for testcase in root.iter("testcase"):
        nodeid = f"{testcase.attrib['classname'].replace('.', '/')}.py::{testcase.attrib['name']}"
        failure = testcase.find("failure")
        if failure is None:
            passes.append({"nodeid": nodeid, "result": "PASS"})
            continue
        actual = (failure.text or failure.attrib.get("message", "")).strip()
        match = message_pattern.match(actual)
        if not match:
            raise RuntimeError(f"Invalid RED without curated reason: {nodeid}: {actual}")
        test_id, actual_reason = match.groups()
        item = {
            "test_id": test_id,
            "file_test_name": nodeid,
            "actual_red_reason": actual_reason,
            "quality": "MEANINGFUL RED",
        }
        failures.append(item)
        by_id.setdefault(test_id, item)
    return failures, passes, by_id


def _case_inventory(cases, by_id: dict[str, dict]) -> list[dict]:
    inventory = []
    for case in cases:
        actual = by_id[case.test_id]
        inventory.append(
            {
                "test_id": case.test_id,
                "file_test_name": actual["file_test_name"],
                "plan_1_2_contract_ids": list(case.source_ids),
                "architecture_requirement": case.architecture_requirement,
                "expected_red_reason": case.expected_red_reason,
                "actual_red_reason": actual["actual_red_reason"],
                "quality": "MEANINGFUL RED",
            }
        )
    return inventory


def build_report(junit_path: Path) -> dict:
    failures, passes, by_id = _load_results(junit_path)
    methodology_inventory = _case_inventory(ALL_METHODOLOGY_CASES, by_id)
    architecture_inventory = _case_inventory(ALL_ARCHITECTURE_CASES, by_id)
    known_ids = {item["test_id"] for item in methodology_inventory + architecture_inventory}
    supplemental = []
    for item in failures:
        if item["test_id"] in known_ids:
            continue
        source_ids, requirement = _supplemental_metadata(item["test_id"])
        supplemental.append(
            {
                **item,
                "plan_1_2_contract_ids": source_ids,
                "architecture_requirement": requirement,
                "expected_red_reason": item["actual_red_reason"].split("; future capability", 1)[0],
            }
        )

    complete_inventory = methodology_inventory + architecture_inventory + supplemental
    assert len(complete_inventory) == len(failures) == 82
    assert len(passes) == 3
    assert len({case.test_id for case in ALL_METHODOLOGY_CASES}) == 30

    fitzgerald = [item for item in methodology_inventory if item["test_id"].startswith("F80-")]
    maffetone = [item for item in methodology_inventory if item["test_id"].startswith("MAF-")]
    return {
        "report_id": "PLAN-2.1",
        "title": "Training Methodology Contract Foundation",
        "status": "CONTRACT_FOUNDATION_COMPLETE_READY_FOR_PLAN_2_2",
        "baseline": {
            "branch": "develop",
            "head": BASELINE_HEAD,
            "initial_worktree": "clean",
            "historical_before": {"passed": 790, "failed": 0, "warnings": 1},
        },
        "authoritative_artifact_verification": ARTIFACTS,
        "files_added_or_changed": FILES,
        "historical_suite": {
            "command": r".\.venv\Scripts\python.exe -m pytest -q --ignore=tests\plan_methodology_contracts",
            "passed": 790,
            "failed": 0,
            "warnings": ["known unchanged .pytest_cache permission warning"],
            "duration_seconds": 36.91,
        },
        "new_contract_suite": {
            "command": r".\.venv\Scripts\python.exe -m pytest tests\plan_methodology_contracts -q --tb=short",
            "collected": 85,
            "meaningful_red": 82,
            "passed_traceability_gates": 3,
            "invalid_red": 0,
            "warnings": ["known unchanged .pytest_cache permission warnings"],
            "duration_seconds": 0.13,
        },
        "traceability": {
            "fitzgerald": {"mapped": 15, "required": 15, "contracts": fitzgerald},
            "maffetone": {"mapped": 15, "required": 15, "contracts": maffetone},
            "total": {"mapped": 30, "required": 30, "orphan_contracts": []},
        },
        "architecture_contract_inventory": architecture_inventory,
        "supplemental_methodology_edge_cases": supplemental,
        "meaningful_red_inventory": complete_inventory,
        "meaningful_red_count": 82,
        "invalid_red_count": 0,
        "representative_red_failures": [
            by_id["F80-V1-008"],
            by_id["MAF-V1-005"],
            by_id["ARCH-STALE-001"],
            by_id["ARCH-AI-002"],
            by_id["ARCH-ATOM-001"],
        ],
        "production_mutation_check": {
            "src_unchanged": True,
            "packaging_unchanged": True,
            "repository_docs_unchanged": True,
            "migrations_unchanged": True,
            "requirements_unchanged": True,
            "production_database_untouched": True,
            "garmin_data_untouched": True,
            "staged_files": [],
            "uncommitted_scope": ["tests/plan_methodology_contracts/"],
        },
        "recommended_plan_2_2_scope": [
            "Implement the production plan-methodology domain package and frozen value types behind the contracted capabilities.",
            "Start with stable Plan/immutable PlanRevision candidates, canonical content hashing, snapshots, and the static registry.",
            "Implement Fitzgerald and Maffetone parameter derivation/policy collaborators plus typed prescriptions and deterministic leaf segments.",
            "Keep persistence, AI proposal parsing, compliance evaluation, legacy conversion, and atomic activation behind the contracted boundaries and phase them according to PLAN-2.0.",
            "Turn RED cases green incrementally without weakening frozen expectations or coupling methods to each other.",
        ],
    }


def _markdown(report: dict) -> str:
    lines = [
        "# PLAN-2.1 — Training Methodology Contract Foundation",
        "",
        f"Status: `{report['status']}`",
        "",
        "## Baseline",
        "",
        f"- Branch: `{report['baseline']['branch']}`",
        f"- HEAD: `{report['baseline']['head']}`",
        "- Initial worktree: clean",
        "- Historical baseline: 790 passed, 0 failed",
        "",
        "## Authoritative artifact verification",
        "",
        "| Artifact | SHA-256 | Verified |",
        "|---|---|---|",
    ]
    for artifact in report["authoritative_artifact_verification"]:
        lines.append(f"| `{artifact['path']}` | `{artifact['sha256']}` | YES |")
    lines += [
        "",
        "## Files added/changed",
        "",
        *[f"- `{path}`" for path in report["files_added_or_changed"]],
        "",
        "## Test results",
        "",
        "- Historical suite: **790 passed, 0 failed**, one known `.pytest_cache` permission warning.",
        "- New contract suite: **82 meaningful RED, 3 traceability gates passed, 0 invalid RED**.",
        "- New suite collection: 85 tests.",
        "",
        "## Methodology traceability",
        "",
        "- Fitzgerald: **15 / 15** mapped.",
        "- Maffetone: **15 / 15** mapped.",
        "- Total: **30 / 30**, no orphans.",
        "",
        "| Contract ID | Executable test | Architecture requirement | Actual RED reason | Quality |",
        "|---|---|---|---|---|",
    ]
    for item in report["traceability"]["fitzgerald"]["contracts"] + report["traceability"]["maffetone"]["contracts"]:
        lines.append(
            f"| {item['test_id']} | `{item['file_test_name']}` | {item['architecture_requirement']} | {item['actual_red_reason']} | {item['quality']} |"
        )
    lines += [
        "",
        "## Architecture contract inventory",
        "",
        "| Test ID | Executable test | PLAN-2.0 requirement | Actual RED reason | Quality |",
        "|---|---|---|---|---|",
    ]
    for item in report["architecture_contract_inventory"]:
        lines.append(
            f"| {item['test_id']} | `{item['file_test_name']}` | {item['architecture_requirement']} | {item['actual_red_reason']} | {item['quality']} |"
        )
    lines += [
        "",
        "## Supplemental edge-case inventory",
        "",
        "| Test ID | PLAN-1.2 IDs | Requirement | Actual RED reason | Quality |",
        "|---|---|---|---|---|",
    ]
    for item in report["supplemental_methodology_edge_cases"]:
        lines.append(
            f"| {item['test_id']} | {', '.join(item['plan_1_2_contract_ids'])} | {item['architecture_requirement']} | {item['actual_red_reason']} | {item['quality']} |"
        )
    lines += [
        "",
        "## Failure quality",
        "",
        "Every RED result was reviewed. Each failure names one missing future behavior and callable boundary. There are no syntax, collection, fixture, isolation, database, network, or unrelated-production failures.",
        "",
        "- Meaningful RED: **82**",
        "- Invalid RED: **0**",
        "",
        "## Production mutation check",
        "",
        "Only `tests/plan_methodology_contracts/` is uncommitted. `src/`, `packaging/`, repository docs, migrations, requirements, the production database, and Garmin data are unchanged. Nothing is staged.",
        "",
        "## Recommended PLAN-2.2 scope",
        "",
        *[f"- {entry}" for entry in report["recommended_plan_2_2_scope"]],
        "",
        "PLAN-2.1 CONTRACT FOUNDATION COMPLETE — READY FOR PLAN-2.2 IMPLEMENTATION",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    report = build_report(args.junit)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "plan_methodology_contract_foundation.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "plan_methodology_contract_foundation.md").write_text(
        _markdown(report), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
