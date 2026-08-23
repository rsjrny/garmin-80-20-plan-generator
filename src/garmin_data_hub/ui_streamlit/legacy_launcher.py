"""Optional source launcher for the retained Streamlit fallback."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    try:
        from streamlit.web import cli
    except ImportError as exc:
        raise SystemExit(
            'Streamlit is optional. Install it with: pip install -e ".[streamlit]"'
        ) from exc

    app_path = Path(__file__).resolve().parent / "app.py"
    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--global.developmentMode=false",
        "--server.enableCORS=false",
    ]
    raise SystemExit(cli.main())


if __name__ == "__main__":
    main()
