import streamlit as st
import sqlite3
import json
from pathlib import Path
from garmin_data_hub.paths import default_db_path
from garmin_data_hub.db import queries


def get_setting(conn, key, default):
    return queries.get_setting(conn, key, default)


def set_setting(conn, key, value):
    return queries.set_setting(conn, key, value)


def render_sidebar(conn=None):
    """
    Renders the common sidebar elements and returns the global settings.
    If conn is provided, it uses it to load/save settings.
    """

    # If no connection provided, create a temporary one just for settings
    local_conn = False
    if conn is None:
        try:
            conn = sqlite3.connect(str(default_db_path()))
            local_conn = True
        except Exception:
            pass

    with st.sidebar:
        st.header("Settings")

        # Global Unit System
        current_unit = "Imperial"
        if conn:
            current_unit = queries.get_setting(conn, "unit_system", "Imperial")

        unit_index = (
            ["Metric", "Imperial"].index(current_unit)
            if current_unit in ["Metric", "Imperial"]
            else 1
        )

        new_unit = st.radio(
            "Global Units",
            ["Metric", "Imperial"],
            index=unit_index,
            key="global_unit_selector",
        )

        if conn and new_unit != current_unit:
            queries.set_setting(conn, "unit_system", new_unit)
            st.rerun()  # Reload page to apply new units immediately

        st.divider()

        if st.button("🚪 Exit Application", use_container_width=True):
            st.write("To exit, close this browser tab or press Ctrl+W")
            import streamlit.components.v1 as components

            components.html(
                """
                <script>
                window.open('', '_self', ''); 
                window.close();
                </script>
                """,
                height=0,
            )

    if local_conn and conn:
        conn.close()

    return new_unit
