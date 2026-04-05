import runpy
import sys
from pathlib import Path
import os

def main():
    """
    Launcher for the Streamlit application that uses runpy to execute
    Streamlit's command line interface. This is a more robust way to
    run Streamlit from a PyInstaller bundle.
    """
    # When frozen, the entry point for the app is relative to the executable
    if getattr(sys, 'frozen', False):
        # The spec file should be configured to place app.py at this location
        app_path = Path(sys.executable).parent / "garmin_data_hub" / "ui_streamlit" / "app.py"
        if not app_path.exists():
            # Fallback for one-dir mode where structure might be different
            # sys._MEIPASS is the temporary directory PyInstaller creates
            app_path_fallback = Path(sys._MEIPASS) / "garmin_data_hub" / "ui_streamlit" / "app.py"
            if not app_path_fallback.exists():
                print(f"[ERROR] Application entry point not found at fallback path: {app_path_fallback}")
                sys.exit(1)
            app_path = app_path_fallback
    else:
        # Running from source, locate the original app.py
        app_path = Path(__file__).resolve().parents[1] / "src" / "garmin_data_hub" / "ui_streamlit" / "app.py"

    if not app_path.exists():
        print(f"[ERROR] Could not find app.py. Final attempted path: {app_path}")
        sys.exit(1)

    # Set the arguments for Streamlit's CLI
    # We are essentially running `python -m streamlit run app.py ...`
    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--global.developmentMode=false",
        "--server.port=8501",
        "--server.enableCORS",
        "false",
        "--server.enableXsrfProtection",
        "false",
    ]

    # Use runpy to execute Streamlit's main entry point
    try:
        runpy.run_module("streamlit", run_name="__main__")
    except Exception as e:
        print(f"[ERROR] Failed to launch Streamlit: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
