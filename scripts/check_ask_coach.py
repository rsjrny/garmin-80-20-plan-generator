"""Exercise Ask Coach with real data and the saved Codex login.

Run from the repository: python scripts/check_ask_coach.py
This sends the same summarized context as the page. Recovery sharing is off
unless --include-recovery is passed. Nothing is written to the database.
"""

from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path

from garmin_data_hub.paths import default_db_path
from garmin_data_hub.services.coach_chat import build_chat_context, generate_chat_answer
from garmin_data_hub.services.codex_prerequisites import detect_codex_prerequisites


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, help="Database (defaults to the app database)")
    parser.add_argument("--include-recovery", action="store_true")
    args = parser.parse_args()
    status = detect_codex_prerequisites()
    if not status.ready_for_generation:
        raise SystemExit(f"Codex is not ready: {status.auth_detail or status.auth_state}")
    context = build_chat_context(args.db or default_db_path(), include_recovery=args.include_recovery)
    print("Source values for reviewing the answers:", flush=True)
    print(json.dumps({
        "as_of_date": context["as_of_date"],
        "plan_comparison": context["plan_comparison"],
        "recovery_included": context["recovery_included"],
        "recovery": context.get("recovery"),
    }, indent=2), flush=True)
    history: list[dict[str, str]] = []
    cancel_event = threading.Event()
    for question in (
        "How does my completed training compare with my plan over the comparison "
        "period? State the exact dates and hours. Explain what this comparison cannot tell us.",
        "Given that comparison, can you tell whether my latest sleep and HRV support "
        "training today? Cite the dates of any measurements you use, and say what is missing.",
    ):
        print(f"\nQuestion: {question}", flush=True)
        answer = generate_chat_answer(
            context, question, history, cancel_event=cancel_event, executable=status.codex.path,
        )
        print(json.dumps(answer, indent=2, ensure_ascii=False), flush=True)
        history.extend([
            {"role": "user", "content": question},
            {"role": "assistant", "content": answer["answer"]},
        ])
    print("\nLive conversation completed. Compare evidence against the source values above.")


if __name__ == "__main__":
    main()
