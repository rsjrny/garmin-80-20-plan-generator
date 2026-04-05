from __future__ import annotations
import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import pydeck as pdk

import os
from pathlib import Path
from datetime import datetime
import json
import math

from garmin_data_hub.paths import default_db_path, ensure_app_dirs
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.analytics.queries import (
    list_recent_activities,
    get_activity_records,
)
from garmin_data_hub.db.queries import get_activity_trackpoints
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
from garmin_data_hub.db import queries

st.set_page_config(page_title="Activities", layout="wide")


# --- Helper Functions for Enhanced Mapping ---
def calculate_hr_zones(hrmax: int | None, lthr: int | None) -> dict | None:
    """Calculate 5-zone heart rate zones from HRmax and LTHR."""
    if hrmax is None:
        return None

    # Use Coggan/Friel zone model
    if lthr is None:
        # Fallback to percentage-based zones
        return {
            "zone1": (int(hrmax * 0.50), int(hrmax * 0.60)),  # Recovery
            "zone2": (int(hrmax * 0.60), int(hrmax * 0.70)),  # Aerobic
            "zone3": (int(hrmax * 0.70), int(hrmax * 0.80)),  # Tempo
            "zone4": (int(hrmax * 0.80), int(hrmax * 0.90)),  # Threshold
            "zone5": (int(hrmax * 0.90), hrmax),  # Max
        }
    else:
        # LTHR-based zones (more accurate)
        return {
            "zone1": (int(hrmax * 0.50), int(lthr * 0.85)),  # Recovery
            "zone2": (int(lthr * 0.85), int(lthr * 0.89)),  # Aerobic
            "zone3": (int(lthr * 0.90), int(lthr * 0.94)),  # Tempo
            "zone4": (int(lthr * 0.95), int(lthr * 1.00)),  # Threshold
            "zone5": (int(lthr * 1.01), hrmax),  # Max
        }


def get_hr_zone(hr_bpm: float, zones: dict | None) -> int:
    """Get HR zone (1-5) for a given heart rate."""
    if zones is None or pd.isna(hr_bpm):
        return 0

    hr = int(hr_bpm)
    if hr >= zones["zone5"][0]:
        return 5
    elif hr >= zones["zone4"][0]:
        return 4
    elif hr >= zones["zone3"][0]:
        return 3
    elif hr >= zones["zone2"][0]:
        return 2
    elif hr >= zones["zone1"][0]:
        return 1
    return 0


def get_hr_zone_color(zone: int) -> list[int]:
    """Get RGB color for HR zone."""
    colors = {
        0: [128, 128, 128],  # Gray - No data
        1: [0, 136, 255],  # Blue - Recovery
        2: [0, 255, 0],  # Green - Aerobic
        3: [255, 255, 0],  # Yellow - Tempo
        4: [255, 136, 0],  # Orange - Threshold
        5: [255, 0, 0],  # Red - Max
    }
    return colors.get(zone, [128, 128, 128])


def get_speed_color(speed_mps: float, min_speed: float, max_speed: float) -> list[int]:
    """Get RGB color based on speed (gradient from blue to red)."""
    if pd.isna(speed_mps) or max_speed == min_speed:
        return [128, 128, 128]

    # Normalize speed to 0-1 range
    normalized = (speed_mps - min_speed) / (max_speed - min_speed)
    normalized = max(0, min(1, normalized))  # Clamp to [0, 1]

    # Blue (slow) to Green to Yellow to Red (fast)
    if normalized < 0.33:
        # Blue to Green
        ratio = normalized / 0.33
        return [0, int(136 + 119 * ratio), int(255 - 255 * ratio)]
    elif normalized < 0.67:
        # Green to Yellow
        ratio = (normalized - 0.33) / 0.34
        return [int(255 * ratio), 255, 0]
    else:
        # Yellow to Red
        ratio = (normalized - 0.67) / 0.33
        return [255, int(255 - 255 * ratio), 0]


def get_pace_color(
    pace_min_per_km: float, min_pace: float, max_pace: float
) -> list[int]:
    """Get RGB color based on pace (gradient - inverted since lower pace is faster)."""
    if pd.isna(pace_min_per_km) or max_pace == min_pace:
        return [128, 128, 128]

    # Normalize pace (inverted - lower pace is faster)
    normalized = (max_pace - pace_min_per_km) / (max_pace - min_pace)
    normalized = max(0, min(1, normalized))

    # Blue (slow) to Red (fast)
    if normalized < 0.33:
        ratio = normalized / 0.33
        return [0, int(136 + 119 * ratio), int(255 - 255 * ratio)]
    elif normalized < 0.67:
        ratio = (normalized - 0.33) / 0.34
        return [int(255 * ratio), 255, 0]
    else:
        ratio = (normalized - 0.67) / 0.33
        return [255, int(255 - 255 * ratio), 0]


def calculate_splits(
    df: pd.DataFrame, split_distance_m: float, unit_system: str
) -> list[dict]:
    """Calculate split markers at regular intervals."""
    splits = []
    if "distance_m" not in df.columns or df["distance_m"].isna().all():
        return splits

    current_split = split_distance_m
    for idx, row in df.iterrows():
        if pd.notna(row["distance_m"]) and row["distance_m"] >= current_split:
            if pd.notna(row["lat_deg"]) and pd.notna(row["lon_deg"]):
                split_num = int(current_split / split_distance_m)
                if unit_system == "Imperial":
                    label = f"{split_num} mi"
                else:
                    label = f"{split_num} km"

                splits.append(
                    {
                        "position": [float(row["lon_deg"]), float(row["lat_deg"])],
                        "label": label,
                        "distance_m": current_split,
                    }
                )
                current_split += split_distance_m

    return splits


def detect_climbs(
    df: pd.DataFrame,
    min_grade_pct: float = 2.0,
    min_distance_m: float = 200.0,
    min_gain_m: float = 10.0,
) -> pd.DataFrame:
    """Detect climb segments from ordered trackpoints and score 1-5 within activity."""
    required = {"distance_m", "altitude_m", "grade_pct_smoothed"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame()

    work = df.dropna(subset=["distance_m", "altitude_m", "grade_pct_smoothed"]).copy()
    if work.empty:
        return pd.DataFrame()

    work = work.sort_values("distance_m").reset_index(drop=True)
    is_climb = work["grade_pct_smoothed"] >= float(min_grade_pct)

    segments: list[dict] = []
    in_segment = False
    start_idx = 0

    for i, flag in enumerate(is_climb.tolist()):
        if flag and not in_segment:
            in_segment = True
            start_idx = i
        elif not flag and in_segment:
            end_idx = i - 1
            in_segment = False

            d0 = float(work.loc[start_idx, "distance_m"])
            d1 = float(work.loc[end_idx, "distance_m"])
            a0 = float(work.loc[start_idx, "altitude_m"])
            a1 = float(work.loc[end_idx, "altitude_m"])
            dist = max(0.0, d1 - d0)
            gain = max(0.0, a1 - a0)
            avg_grade = (gain / dist * 100.0) if dist > 0 else 0.0
            if dist >= min_distance_m and gain >= min_gain_m:
                # Effort-like score emphasizes both vertical and steepness.
                effort = gain * max(avg_grade, 0.1)
                segments.append(
                    {
                        "start_distance_m": d0,
                        "end_distance_m": d1,
                        "distance_m": dist,
                        "gain_m": gain,
                        "avg_grade_pct": avg_grade,
                        "effort_score": effort,
                    }
                )

    if in_segment:
        end_idx = len(work) - 1
        d0 = float(work.loc[start_idx, "distance_m"])
        d1 = float(work.loc[end_idx, "distance_m"])
        a0 = float(work.loc[start_idx, "altitude_m"])
        a1 = float(work.loc[end_idx, "altitude_m"])
        dist = max(0.0, d1 - d0)
        gain = max(0.0, a1 - a0)
        avg_grade = (gain / dist * 100.0) if dist > 0 else 0.0
        if dist >= min_distance_m and gain >= min_gain_m:
            effort = gain * max(avg_grade, 0.1)
            segments.append(
                {
                    "start_distance_m": d0,
                    "end_distance_m": d1,
                    "distance_m": dist,
                    "gain_m": gain,
                    "avg_grade_pct": avg_grade,
                    "effort_score": effort,
                }
            )

    if not segments:
        return pd.DataFrame()

    out = pd.DataFrame(segments)
    # 1-5 difficulty scale relative to climbs in this run.
    pct = out["effort_score"].rank(method="average", pct=True)
    out["difficulty_1_5"] = np.ceil(pct * 5).clip(lower=1, upper=5).astype(int)
    out = out.sort_values("effort_score", ascending=False).reset_index(drop=True)
    out.insert(0, "climb", range(1, len(out) + 1))
    return out


# --- Persistence Functions ---
def load_activity_preferences(conn):
    try:
        return queries.load_activity_preferences(conn)
    except Exception as e:
        st.warning(f"Could not load preferences: {e}")
        return {
            "selected_activity": None,
            "color_mode": "Heart Rate Zone",
            "show_splits": True,
            "view_3d": False,
            "basemap": "CARTO Positron",
            "analysis_tab": "Elevation",
        }


def save_activity_preferences(conn, prefs):
    try:
        queries.save_activity_preferences(conn, prefs)
    except Exception as e:
        st.error(f"Failed to save preferences: {e}")


# Initialize session state for selections
if "activities_selected_activity" not in st.session_state:
    st.session_state.activities_selected_activity = None
if "activities_color_mode" not in st.session_state:
    st.session_state.activities_color_mode = "Heart Rate Zone"
if "activities_show_splits" not in st.session_state:
    st.session_state.activities_show_splits = True
if "activities_view_3d" not in st.session_state:
    st.session_state.activities_view_3d = False
if "activities_basemap" not in st.session_state:
    st.session_state.activities_basemap = "CARTO Positron"
if "activities_analysis_tab" not in st.session_state:
    st.session_state.activities_analysis_tab = "Elevation"


# --- Cached Query Functions ---
import time


@st.cache_data(show_spinner=False)
def get_activity_stats(db_path: str, db_mtime: float) -> dict:
    conn = connect_sqlite(db_path)
    try:
        return queries.get_activity_stats(conn)
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def get_activity_track_counts(
    db_path: str, db_mtime: float, activity_ids: tuple[int, ...]
) -> dict[int, dict[str, int]]:
    """Return split count, trackpoint count, and GPS availability per activity."""
    if not activity_ids:
        return {}
    conn = connect_sqlite(db_path)
    try:
        placeholders = ",".join("?" for _ in activity_ids)
        # Split counts from activity_splits
        split_rows = conn.execute(
            f"SELECT activity_id, COUNT(*) AS split_count FROM activity_splits WHERE activity_id IN ({placeholders}) GROUP BY activity_id",
            activity_ids,
        ).fetchall()
        split_counts = {int(r[0]): int(r[1]) for r in split_rows}

        # Trackpoint counts from activity_trackpoint
        trackpoint_rows = conn.execute(
            f"SELECT activity_id, COUNT(*) AS trackpoint_count FROM activity_trackpoint WHERE activity_id IN ({placeholders}) GROUP BY activity_id",
            activity_ids,
        ).fetchall()
        trackpoint_counts = {int(r[0]): int(r[1]) for r in trackpoint_rows}

        # GPS availability from activity.start_latitude
        gps_rows = conn.execute(
            f"SELECT activity_id, start_latitude FROM activity WHERE activity_id IN ({placeholders})",
            activity_ids,
        ).fetchall()
        gps_map = {int(r[0]): (1 if r[1] is not None else 0) for r in gps_rows}

        out = {}
        for activity_id in activity_ids:
            aid = int(activity_id)
            out[aid] = {
                "record_count": split_counts.get(aid, 0),
                "trackpoint_count": trackpoint_counts.get(aid, 0),
                "gps_count": gps_map.get(aid, 0),
            }
        return out
    finally:
        conn.close()


ensure_app_dirs()
db_path = default_db_path()

conn = connect_sqlite(db_path)
schema_path = Path(__file__).resolve().parents[2] / "db" / "schema.sql"
apply_schema(conn, schema_path)

# Load preferences from database on first load
if "prefs_loaded" not in st.session_state:
    loaded_prefs = load_activity_preferences(conn)
    st.session_state.activities_selected_activity = loaded_prefs.get(
        "selected_activity"
    )
    st.session_state.activities_color_mode = loaded_prefs.get(
        "color_mode", "Heart Rate Zone"
    )
    st.session_state.activities_show_splits = loaded_prefs.get("show_splits", True)
    st.session_state.activities_view_3d = loaded_prefs.get("view_3d", False)
    st.session_state.activities_basemap = loaded_prefs.get("basemap", "CARTO Positron")
    st.session_state.activities_analysis_tab = loaded_prefs.get(
        "analysis_tab", "Elevation"
    )
    st.session_state.prefs_loaded = True

# Render Sidebar and get global units
unit_system = render_sidebar(conn)

st.header("Activities")

# --- Database Overview ---
st.subheader("Database Overview")
col1, col2, col3, col4 = st.columns(4)

db_mtime = os.path.getmtime(db_path) if os.path.exists(db_path) else time.time()
stats = get_activity_stats(db_path, db_mtime)
total_activities = stats["total_activities"]
total_distance_m = stats["total_distance_m"]
total_duration_s = stats["total_duration_s"]
last_activity_iso = stats["last_activity_iso"]

col1.metric("Total Activities", total_activities)

# Total Distance
total_distance_km = (total_distance_m / 1000.0) if total_distance_m else 0.0

if unit_system == "Imperial":
    total_distance_mi = total_distance_km * 0.621371
    col2.metric("Total Distance", f"{total_distance_mi:,.1f} mi")
else:
    col2.metric("Total Distance", f"{total_distance_km:,.1f} km")

# Total Duration
total_duration_h = (total_duration_s / 3600.0) if total_duration_s else 0.0
col3.metric("Total Duration", f"{total_duration_h:,.1f} h")

# Last Activity Date
if last_activity_iso:
    try:
        dt = datetime.fromisoformat(last_activity_iso)
        last_activity_date = dt.strftime("%Y-%m-%d")
    except ValueError:
        last_activity_date = str(last_activity_iso)
else:
    last_activity_date = "N/A"
col4.metric("Last Activity", last_activity_date)

st.divider()

# --- Recent Activities List ---
st.subheader("Recent Activities")
rows = list_recent_activities(conn, limit=500)

df = pd.DataFrame(rows)
if df.empty:
    st.info("No activities yet. Go to Import first.")
    conn.close()
else:
    # Convert distance in table based on unit system
    if unit_system == "Imperial":
        df["distance_mi"] = df["distance_km"] * 0.621371
        df_display = df.drop(columns=["distance_km"]).rename(
            columns={"distance_mi": "distance"}
        )
        dist_col_name = "distance"
        dist_unit = "mi"
    else:
        df_display = df.rename(columns={"distance_km": "distance"})
        dist_col_name = "distance"
        dist_unit = "km"

    st.dataframe(df_display, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Activity Detail")

    base_activities = [r for r in rows if r["distance_km"] is not None]
    activity_ids = tuple(r["activity_id"] for r in base_activities)
    track_counts = get_activity_track_counts(db_path, db_mtime, activity_ids)

    # Load athlete profile for HR zone calculations
    athlete_profile = queries.get_athlete_profile(conn)
    hrmax = athlete_profile.get("hrmax_calc") if athlete_profile else None
    lthr = athlete_profile.get("lthr_calc") if athlete_profile else None

    # Create a selection list (includes trackpoint count so map-capable entries are obvious)
    activity_options = {
        (
            f"{r['start_utc']} - {r['sport']} "
            f"({r['distance_km'] * (0.621371 if unit_system == 'Imperial' else 1.0):.2f} {dist_unit}) "
            f"[Track:{track_counts.get(r['activity_id'], {}).get('trackpoint_count', 0)} pts]"
        ): r["activity_id"]
        for r in base_activities
    }

    # Use saved selection if available
    saved_selection = st.session_state.activities_selected_activity
    selected_index = 0
    if saved_selection and saved_selection in list(activity_options.keys()):
        selected_index = list(activity_options.keys()).index(saved_selection)

    option_labels = list(activity_options.keys())
    first_with_track_index = None
    for i, label in enumerate(option_labels):
        if track_counts.get(activity_options[label], {}).get("trackpoint_count", 0) > 0:
            first_with_track_index = i
            break

    if first_with_track_index is not None:
        selected_activity_id = activity_options[option_labels[selected_index]]
        selected_track_count = track_counts.get(selected_activity_id, {}).get(
            "trackpoint_count", 0
        )
        if selected_track_count == 0:
            selected_index = first_with_track_index

    selected_label = st.selectbox(
        "Select Activity to View",
        options=list(activity_options.keys()),
        index=selected_index,
        key="activity_selector",
        on_change=lambda: st.session_state.update(
            {"activities_selected_activity": st.session_state.activity_selector}
        ),
    )

    # Save preferences ONLY when they change (not on every render)
    current_prefs = {
        "selected_activity": st.session_state.activities_selected_activity,
        "color_mode": st.session_state.activities_color_mode,
        "show_splits": st.session_state.activities_show_splits,
        "view_3d": st.session_state.activities_view_3d,
        "basemap": st.session_state.activities_basemap,
        "analysis_tab": st.session_state.activities_analysis_tab,
    }

    # Only save if preferences have changed
    if (
        "last_saved_prefs" not in st.session_state
        or st.session_state.last_saved_prefs != current_prefs
    ):
        save_activity_preferences(conn, current_prefs)
        st.session_state.last_saved_prefs = current_prefs.copy()

    if selected_label:
        activity_id = activity_options[selected_label]
        selected_record_count = track_counts.get(activity_id, {}).get("record_count", 0)
        selected_trackpoint_count = track_counts.get(activity_id, {}).get(
            "trackpoint_count", 0
        )
        if selected_record_count == 0 and selected_trackpoint_count == 0:
            st.info(
                "Selected activity has no recorded track points. Choose another activity to display a map."
            )

        # Fetch records
        records_df = get_activity_records(conn, activity_id)
        trackpoints_df = get_activity_trackpoints(conn, activity_id)
        selected_row = next((r for r in rows if r["activity_id"] == activity_id), None)

        # --- Map ---
        start_lat = selected_row["start_latitude"] if selected_row else None
        start_lon = selected_row["start_longitude"] if selected_row else None
        if (
            (start_lat is None or start_lon is None)
            and not trackpoints_df.empty
            and {"lat_deg", "lon_deg"}.issubset(trackpoints_df.columns)
        ):
            valid_center = trackpoints_df.dropna(subset=["lat_deg", "lon_deg"])
            if not valid_center.empty:
                start_lat = float(valid_center.iloc[0]["lat_deg"])
                start_lon = float(valid_center.iloc[0]["lon_deg"])

        st.subheader("Map")
        if start_lat is not None and start_lon is not None:
            basemap_options = [
                "CARTO Positron",
                "CARTO Voyager",
                "CARTO Dark Matter",
                "Track only",
            ]
            if (
                "activities_basemap" not in st.session_state
                or st.session_state.activities_basemap not in basemap_options
            ):
                st.session_state.activities_basemap = "CARTO Positron"
            basemap = st.session_state.activities_basemap
            col_map1, col_map2, col_map3 = st.columns(3)
            with col_map1:
                st.selectbox(
                    "Basemap", options=basemap_options, key="activities_basemap"
                )
            with col_map2:
                st.checkbox("3D Terrain", key="activities_view_3d")
            with col_map3:
                color_mode = st.selectbox(
                    "Track Color",
                    options=["Heart Rate Zone", "Speed", "Pace"],
                    key="activities_color_mode",
                )
            view_3d = st.session_state.get("activities_view_3d", False)
            basemap_styles = {
                "CARTO Positron": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                "CARTO Voyager": "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                "CARTO Dark Matter": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                "Track only": None,
            }
            map_style = basemap_styles.get(basemap)
            view_state = pdk.ViewState(
                latitude=float(start_lat),
                longitude=float(start_lon),
                zoom=12,
                pitch=45 if view_3d else 0,
            )

            # Build map layers from trackpoints when available.
            layers = []

            # Start marker (green)
            start_layer = pdk.Layer(
                "ScatterplotLayer",
                data=[
                    {
                        "position": [float(start_lon), float(start_lat)],
                        "color": [0, 200, 0, 220],
                        "label": "Start",
                    }
                ],
                get_position="position",
                get_color="color",
                get_radius=45,
                radius_min_pixels=3,
                radius_max_pixels=6,
                pickable=True,
            )
            layers.append(start_layer)

            # Track polyline (colored based on selection)
            if not trackpoints_df.empty:
                # Calculate min/max for speed/pace coloring
                min_speed = (
                    trackpoints_df["speed_mps"].min()
                    if "speed_mps" in trackpoints_df.columns
                    else 0
                )
                max_speed = (
                    trackpoints_df["speed_mps"].max()
                    if "speed_mps" in trackpoints_df.columns
                    else 1
                )

                # For pace, convert speed to min/km or min/mile
                if unit_system == "Imperial":
                    pace_factor = 0.621371 * 60  # minutes per mile
                else:
                    pace_factor = 60  # minutes per km

                if max_speed == 0:
                    max_speed = 1
                if min_speed == max_speed:
                    min_speed = max_speed - 1

                # Prepare path data with coloring
                path_data = []
                for idx, row in trackpoints_df.iterrows():
                    if pd.notna(row["lat_deg"]) and pd.notna(row["lon_deg"]):
                        # Determine color based on selected mode
                        if color_mode == "Heart Rate Zone" and "heart_rate_bpm" in row:
                            hr_zones = calculate_hr_zones(hrmax or 180, lthr)
                            zone = get_hr_zone(row["heart_rate_bpm"], hr_zones)
                            color = get_hr_zone_color(zone)
                        elif (
                            color_mode == "Speed"
                            and "speed_mps" in row
                            and pd.notna(row["speed_mps"])
                        ):
                            color = get_speed_color(
                                row["speed_mps"], min_speed, max_speed
                            )
                        elif (
                            color_mode == "Pace"
                            and "speed_mps" in row
                            and pd.notna(row["speed_mps"])
                        ):
                            pace = pace_factor / max(row["speed_mps"], 0.1)
                            pace_min = pace_factor / max_speed
                            pace_max = pace_factor / max(min_speed, 0.1)
                            color = get_pace_color(pace, pace_min, pace_max)
                        else:
                            color = [100, 150, 255, 200]  # Default blue

                        path_data.append(
                            {
                                "position": [
                                    float(row["lon_deg"]),
                                    float(row["lat_deg"]),
                                ],
                                "color": color,
                            }
                        )

                if path_data:
                    # Extract path geometry for polyline
                    path = [[pt["position"][0], pt["position"][1]] for pt in path_data]

                    track_layer = pdk.Layer(
                        "PathLayer",
                        data=[{"path": path, "color": [255, 0, 0]}],
                        get_path="path",
                        get_color="color",
                        width_min_pixels=1,
                        pickable=True,
                    )
                    layers.append(track_layer)

                    # Add colored point layer for detailed coloring
                    point_layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=path_data,
                        get_position="position",
                        get_color="color",
                        get_radius=20,
                        radius_min_pixels=1,
                        radius_max_pixels=2,
                        pickable=False,
                    )
                    layers.append(point_layer)

            # Finish marker (red)
            end_lat = (
                selected_row["end_latitude"]
                if selected_row and "end_latitude" in selected_row
                else None
            )
            end_lon = (
                selected_row["end_longitude"]
                if selected_row and "end_longitude" in selected_row
                else None
            )
            if end_lat is not None and end_lon is not None:
                end_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=[
                        {
                            "position": [float(end_lon), float(end_lat)],
                            "color": [255, 0, 0, 220],
                            "label": "Finish",
                        }
                    ],
                    get_position="position",
                    get_color="color",
                    get_radius=45,
                    radius_min_pixels=3,
                    radius_max_pixels=6,
                    pickable=True,
                )
                layers.append(end_layer)

            deck = pdk.Deck(
                map_style=map_style,
                initial_view_state=view_state,
                layers=layers,
                height=400,
                tooltip={"text": "GPS Track"},
            )
            st.pydeck_chart(deck, width="stretch")

            track_info = f"GPS track: {len(trackpoints_df)} trackpoints"
            if not trackpoints_df.empty and "altitude_m" in trackpoints_df.columns:
                min_alt = trackpoints_df["altitude_m"].min()
                max_alt = trackpoints_df["altitude_m"].max()
                track_info += f" | Elevation: {min_alt:.0f}m - {max_alt:.0f}m"
            st.caption(track_info)
        else:
            st.info("No GPS coordinates available for this activity.")

        st.divider()

        # --- Activity Analysis Charts ---
        st.subheader("Activity Analysis")

        if records_df.empty:
            st.info("No split records available for this activity.")
        else:

            # Build cumulative distance axis from split distance_meters
            if "distance_meters" in records_df.columns:
                records_df["distance_km_cumulative"] = (
                    records_df["distance_meters"].cumsum() / 1000.0
                )
                if unit_system == "Imperial":
                    records_df["distance_display"] = (
                        records_df["distance_km_cumulative"] * 0.621371
                    )
                    dist_label = "Distance (mi)"
                else:
                    records_df["distance_display"] = records_df[
                        "distance_km_cumulative"
                    ]
                    dist_label = "Distance (km)"
            else:
                records_df["distance_display"] = records_df.get(
                    "split_number", pd.Series(range(len(records_df)))
                )
                dist_label = "Split"

            analysis_tab_options = ["Elevation", "Heart Rate", "Speed/Pace", "Combined"]
            if st.session_state.activities_analysis_tab not in analysis_tab_options:
                st.session_state.activities_analysis_tab = "Elevation"

            st.selectbox(
                "Default Analysis View",
                options=analysis_tab_options,
                key="activities_analysis_tab",
                help="This tab opens first when you revisit this page.",
            )

            ordered_tabs = [st.session_state.activities_analysis_tab] + [
                tab
                for tab in analysis_tab_options
                if tab != st.session_state.activities_analysis_tab
            ]
            chart_tabs = dict(zip(ordered_tabs, st.tabs(ordered_tabs)))

            with chart_tabs["Elevation"]:
                # Prefer high-resolution trackpoints for a smoother elevation profile.
                if (
                    not trackpoints_df.empty
                    and "altitude_m" in trackpoints_df.columns
                    and "distance_m" in trackpoints_df.columns
                ):
                    chart_data = trackpoints_df.dropna(
                        subset=["altitude_m", "distance_m"]
                    ).copy()
                    if not chart_data.empty:
                        chart_data["distance_km_cumulative"] = (
                            chart_data["distance_m"] / 1000.0
                        )
                        if unit_system == "Imperial":
                            chart_data["distance_display"] = (
                                chart_data["distance_km_cumulative"] * 0.621371
                            )
                            dist_title = "Distance (mi)"
                        else:
                            chart_data["distance_display"] = chart_data[
                                "distance_km_cumulative"
                            ]
                            dist_title = "Distance (km)"

                        smooth_window = 201
                        grade_window = 51
                        show_grade_overlay = st.checkbox(
                            "Overlay grade (%)",
                            value=True,
                            key="activities_show_grade_overlay",
                        )

                        min_grade_pct = 2.0
                        min_climb_distance_m = 200
                        min_climb_gain_m = 10
                        show_climb_markers = st.checkbox(
                            "Mark climb difficulty",
                            value=True,
                            key="activities_show_climb_markers",
                            help="Show shaded climb sections and C1-C5 markers on the chart.",
                        )

                        # Local grade from adjacent trackpoints.
                        delta_d = chart_data["distance_m"].diff()
                        delta_z = chart_data["altitude_m"].diff()
                        grade_raw = np.where(
                            delta_d > 0,
                            (delta_z / delta_d) * 100.0,
                            np.nan,
                        )
                        chart_data["grade_pct_raw"] = pd.Series(
                            grade_raw, index=chart_data.index
                        ).replace([np.inf, -np.inf], np.nan)
                        chart_data["grade_pct_smoothed"] = (
                            chart_data["grade_pct_raw"]
                            .rolling(window=grade_window, min_periods=1, center=True)
                            .mean()
                            .clip(lower=-25, upper=25)
                        )

                        chart_data["altitude_smoothed"] = (
                            chart_data["altitude_m"]
                            .rolling(window=smooth_window, min_periods=1, center=True)
                            .mean()
                        )

                        if unit_system == "Imperial":
                            chart_data["altitude_display"] = (
                                chart_data["altitude_smoothed"] * 3.28084
                            )
                            alt_label = "Elevation (ft)"
                        else:
                            chart_data["altitude_display"] = chart_data[
                                "altitude_smoothed"
                            ]
                            alt_label = "Elevation (m)"

                        # Downsample for interactive rendering while preserving the smoothed shape.
                        if len(chart_data) > 2500:
                            step = int(np.ceil(len(chart_data) / 2500))
                            chart_plot = chart_data.iloc[::step].copy()
                        else:
                            chart_plot = chart_data

                        climbs_df = detect_climbs(
                            chart_data,
                            min_grade_pct=float(min_grade_pct),
                            min_distance_m=float(min_climb_distance_m),
                            min_gain_m=float(min_climb_gain_m),
                        )

                        st.caption(
                            f"Detected climbs: {len(climbs_df)} (toggle 'Mark climb difficulty' to show on chart)"
                        )

                        climb_bands = None
                        climb_labels = None
                        climb_points = None
                        if show_climb_markers and not climbs_df.empty:
                            climbs_plot = climbs_df.copy()
                            if unit_system == "Imperial":
                                climbs_plot["start_display"] = (
                                    climbs_plot["start_distance_m"] / 1609.344
                                )
                                climbs_plot["end_display"] = (
                                    climbs_plot["end_distance_m"] / 1609.344
                                )
                            else:
                                climbs_plot["start_display"] = (
                                    climbs_plot["start_distance_m"] / 1000.0
                                )
                                climbs_plot["end_display"] = (
                                    climbs_plot["end_distance_m"] / 1000.0
                                )

                            climbs_plot["difficulty_label"] = climbs_plot[
                                "difficulty_1_5"
                            ].apply(lambda x: f"C{int(x)}")
                            climbs_plot["difficulty_str"] = climbs_plot[
                                "difficulty_1_5"
                            ].astype(str)
                            climbs_plot["mid_distance_m"] = (
                                climbs_plot["start_distance_m"]
                                + climbs_plot["end_distance_m"]
                            ) / 2.0

                            alt_lookup = chart_plot[["distance_m", "altitude_display"]]
                            label_rows: list[dict] = []
                            for _, climb in climbs_plot.iterrows():
                                nearest_idx = (
                                    (alt_lookup["distance_m"] - climb["mid_distance_m"])
                                    .abs()
                                    .idxmin()
                                )
                                label_rows.append(
                                    {
                                        "mid_display": (
                                            float(climb["mid_distance_m"]) / 1609.344
                                            if unit_system == "Imperial"
                                            else float(climb["mid_distance_m"]) / 1000.0
                                        ),
                                        "altitude_display": float(
                                            alt_lookup.loc[
                                                nearest_idx, "altitude_display"
                                            ]
                                        ),
                                        "difficulty_1_5": int(climb["difficulty_1_5"]),
                                        "difficulty_str": str(
                                            int(climb["difficulty_1_5"])
                                        ),
                                        "difficulty_label": str(
                                            climb["difficulty_label"]
                                        ),
                                    }
                                )

                            climbs_label_df = pd.DataFrame(label_rows)

                            climb_bands = (
                                alt.Chart(climbs_plot)
                                .mark_rect(opacity=0.2)
                                .encode(
                                    x=alt.X("start_display:Q", title=dist_title),
                                    x2="end_display:Q",
                                    color=alt.Color(
                                        "difficulty_str:N",
                                        title="Climb Difficulty",
                                        scale=alt.Scale(
                                            domain=["1", "2", "3", "4", "5"],
                                            range=[
                                                "#86efac",
                                                "#22c55e",
                                                "#eab308",
                                                "#f97316",
                                                "#ef4444",
                                            ],
                                        ),
                                    ),
                                )
                            )

                            climb_labels = (
                                alt.Chart(climbs_label_df)
                                .mark_text(fontSize=13, dy=-10, fontWeight="bold")
                                .encode(
                                    x=alt.X("mid_display:Q", title=dist_title),
                                    y=alt.Y("altitude_display:Q", title=alt_label),
                                    text="difficulty_label:N",
                                    color=alt.Color(
                                        "difficulty_str:N",
                                        scale=alt.Scale(
                                            domain=["1", "2", "3", "4", "5"],
                                            range=[
                                                "#16a34a",
                                                "#15803d",
                                                "#ca8a04",
                                                "#ea580c",
                                                "#dc2626",
                                            ],
                                        ),
                                        legend=None,
                                    ),
                                )
                            )

                            climb_points = (
                                alt.Chart(climbs_label_df)
                                .mark_circle(
                                    size=110,
                                    opacity=0.98,
                                    stroke="white",
                                    strokeWidth=1.2,
                                )
                                .encode(
                                    x=alt.X("mid_display:Q", title=dist_title),
                                    y=alt.Y("altitude_display:Q", title=alt_label),
                                    color=alt.Color(
                                        "difficulty_str:N",
                                        scale=alt.Scale(
                                            domain=["1", "2", "3", "4", "5"],
                                            range=[
                                                "#16a34a",
                                                "#15803d",
                                                "#ca8a04",
                                                "#ea580c",
                                                "#dc2626",
                                            ],
                                        ),
                                        legend=None,
                                    ),
                                )
                            )

                        elevation_chart = (
                            alt.Chart(chart_plot)
                            .mark_line(color="steelblue", strokeWidth=2)
                            .encode(
                                x=alt.X("distance_display:Q", title=dist_title),
                                y=alt.Y("altitude_display:Q", title=alt_label),
                                tooltip=[
                                    alt.Tooltip(
                                        "distance_display:Q",
                                        title=dist_title,
                                        format=".2f",
                                    ),
                                    alt.Tooltip(
                                        "altitude_display:Q",
                                        title=alt_label,
                                        format=".1f",
                                    ),
                                ],
                            )
                        )

                        if climb_bands is not None:
                            elevation_chart = alt.layer(climb_bands, elevation_chart)
                        if climb_points is not None:
                            elevation_chart = alt.layer(elevation_chart, climb_points)
                        if climb_labels is not None:
                            elevation_chart = alt.layer(elevation_chart, climb_labels)

                        if show_grade_overlay:
                            grade_chart = (
                                alt.Chart(chart_plot)
                                .mark_line(color="#ea580c", strokeWidth=1.6)
                                .encode(
                                    x=alt.X("distance_display:Q", title=dist_title),
                                    y=alt.Y(
                                        "grade_pct_smoothed:Q",
                                        title="Grade (%)",
                                        axis=alt.Axis(orient="right"),
                                        scale=alt.Scale(zero=False),
                                    ),
                                    tooltip=[
                                        alt.Tooltip(
                                            "distance_display:Q",
                                            title=dist_title,
                                            format=".2f",
                                        ),
                                        alt.Tooltip(
                                            "grade_pct_smoothed:Q",
                                            title="Grade (%)",
                                            format=".2f",
                                        ),
                                    ],
                                )
                            )
                            layered = alt.layer(
                                elevation_chart, grade_chart
                            ).resolve_scale(y="independent")
                            st.altair_chart(layered.interactive(), width="stretch")
                        else:
                            st.altair_chart(
                                elevation_chart.interactive(), width="stretch"
                            )

                        if climbs_df.empty:
                            st.info("No climbs detected with current thresholds.")
                        else:
                            if unit_system == "Imperial":
                                climbs_display = climbs_df.copy()
                                climbs_display["start"] = (
                                    climbs_display["start_distance_m"] / 1609.344
                                )
                                climbs_display["end"] = (
                                    climbs_display["end_distance_m"] / 1609.344
                                )
                                climbs_display["distance"] = (
                                    climbs_display["distance_m"] / 1609.344
                                )
                                climbs_display["gain"] = (
                                    climbs_display["gain_m"] * 3.28084
                                )
                                cols = [
                                    "climb",
                                    "start",
                                    "end",
                                    "distance",
                                    "gain",
                                    "avg_grade_pct",
                                    "difficulty_1_5",
                                ]
                                st.dataframe(
                                    climbs_display[cols].rename(
                                        columns={
                                            "start": "start (mi)",
                                            "end": "end (mi)",
                                            "distance": "distance (mi)",
                                            "gain": "gain (ft)",
                                            "avg_grade_pct": "avg grade (%)",
                                            "difficulty_1_5": "difficulty (1-5)",
                                        }
                                    ),
                                    width="stretch",
                                    hide_index=True,
                                )
                            else:
                                climbs_display = climbs_df.copy()
                                climbs_display["start"] = (
                                    climbs_display["start_distance_m"] / 1000.0
                                )
                                climbs_display["end"] = (
                                    climbs_display["end_distance_m"] / 1000.0
                                )
                                climbs_display["distance"] = (
                                    climbs_display["distance_m"] / 1000.0
                                )
                                cols = [
                                    "climb",
                                    "start",
                                    "end",
                                    "distance",
                                    "gain_m",
                                    "avg_grade_pct",
                                    "difficulty_1_5",
                                ]
                                st.dataframe(
                                    climbs_display[cols].rename(
                                        columns={
                                            "start": "start (km)",
                                            "end": "end (km)",
                                            "distance": "distance (km)",
                                            "gain_m": "gain (m)",
                                            "avg_grade_pct": "avg grade (%)",
                                            "difficulty_1_5": "difficulty (1-5)",
                                        }
                                    ),
                                    width="stretch",
                                    hide_index=True,
                                )
                    else:
                        st.info("No elevation data available")
                elif (
                    "altitude_m" in records_df.columns
                    and "distance_display" in records_df.columns
                ):
                    chart_data = records_df.dropna(
                        subset=["altitude_m", "distance_display"]
                    ).copy()

                    if not chart_data.empty:
                        if unit_system == "Imperial":
                            chart_data["altitude_display"] = (
                                chart_data["altitude_m"] * 3.28084
                            )
                            alt_label = "Elevation (ft)"
                        else:
                            chart_data["altitude_display"] = chart_data["altitude_m"]
                            alt_label = "Elevation (m)"

                        chart = (
                            alt.Chart(chart_data)
                            .mark_line(color="steelblue", strokeWidth=2, point=True)
                            .encode(
                                x=alt.X("distance_display:Q", title=dist_label),
                                y=alt.Y("altitude_display:Q", title=alt_label),
                                tooltip=[
                                    alt.Tooltip(
                                        "distance_display:Q",
                                        title=dist_label,
                                        format=".2f",
                                    ),
                                    alt.Tooltip(
                                        "altitude_display:Q",
                                        title=alt_label,
                                        format=".1f",
                                    ),
                                ],
                            )
                        )
                        st.altair_chart(chart.interactive(), width="stretch")
                    else:
                        st.info("No elevation data available")
                else:
                    st.info("No elevation data available")

            with chart_tabs["Heart Rate"]:
                if (
                    "heart_rate_bpm" in records_df.columns
                    and "distance_display" in records_df.columns
                ):
                    hr_data = records_df.dropna(
                        subset=["heart_rate_bpm", "distance_display"]
                    ).copy()

                    if not hr_data.empty:
                        # Get athlete profile for HR zones
                        athlete_profile = queries.get_athlete_profile(conn)
                        hr_zones_chart = None
                        if athlete_profile:
                            hrmax = athlete_profile.get(
                                "hrmax_override"
                            ) or athlete_profile.get("hrmax_calc")
                            lthr = athlete_profile.get(
                                "lthr_override"
                            ) or athlete_profile.get("lthr_calc")
                            hr_zones_chart = calculate_hr_zones(hrmax, lthr)

                        base = alt.Chart(hr_data).encode(
                            x=alt.X("distance_display:Q", title=dist_label)
                        )
                        hr_line = base.mark_line(color="red", strokeWidth=2).encode(
                            y=alt.Y(
                                "heart_rate_bpm:Q",
                                title="Heart Rate (bpm)",
                                scale=alt.Scale(zero=False),
                            ),
                            tooltip=[
                                alt.Tooltip(
                                    "distance_display:Q", title=dist_label, format=".2f"
                                ),
                                alt.Tooltip(
                                    "heart_rate_bpm:Q", title="HR (bpm)", format=".0f"
                                ),
                            ],
                        )
                        st.altair_chart(hr_line.interactive(), width="stretch")

                        if hr_zones_chart:
                            hr_data["hr_zone"] = hr_data["heart_rate_bpm"].apply(
                                lambda hr: get_hr_zone(hr, hr_zones_chart)
                            )
                            zone_counts = hr_data["hr_zone"].value_counts().sort_index()
                            st.caption("**Time in HR Zones**")
                            zone_cols = st.columns(5)
                            for i, col in enumerate(zone_cols):
                                zone_num = i + 1
                                count = zone_counts.get(zone_num, 0)
                                pct = (
                                    (count / len(hr_data) * 100)
                                    if len(hr_data) > 0
                                    else 0
                                )
                                col.metric(f"Zone {zone_num}", f"{pct:.1f}%")
                    else:
                        st.info("No heart rate data available")
                else:
                    st.info("No heart rate data available")

            with chart_tabs["Speed/Pace"]:
                if (
                    "speed_mps" in records_df.columns
                    and "distance_display" in records_df.columns
                ):
                    speed_data = records_df.dropna(
                        subset=["speed_mps", "distance_display"]
                    ).copy()

                    if not speed_data.empty:
                        if unit_system == "Imperial":
                            speed_data["speed_display"] = (
                                speed_data["speed_mps"] * 2.23694
                            )
                            speed_label = "Speed (mph)"
                            speed_data["pace_display"] = 60 / (
                                speed_data["speed_mps"] * 2.23694
                            )
                            pace_label = "Pace (min/mi)"
                        else:
                            speed_data["speed_display"] = speed_data["speed_mps"] * 3.6
                            speed_label = "Speed (km/h)"
                            speed_data["pace_display"] = 1000 / (
                                speed_data["speed_mps"] * 60
                            )
                            pace_label = "Pace (min/km)"

                        speed_data["pace_display"] = speed_data["pace_display"].replace(
                            [np.inf, -np.inf], np.nan
                        )

                        col_speed, col_pace = st.columns(2)
                        with col_speed:
                            st.caption("**Speed**")
                            chart = (
                                alt.Chart(speed_data)
                                .mark_line(color="blue", strokeWidth=2)
                                .encode(
                                    x=alt.X("distance_display:Q", title=dist_label),
                                    y=alt.Y(
                                        "speed_display:Q",
                                        title=speed_label,
                                        scale=alt.Scale(zero=False),
                                    ),
                                    tooltip=[
                                        alt.Tooltip(
                                            "distance_display:Q",
                                            title=dist_label,
                                            format=".2f",
                                        ),
                                        alt.Tooltip(
                                            "speed_display:Q",
                                            title=speed_label,
                                            format=".2f",
                                        ),
                                    ],
                                )
                            )
                            st.altair_chart(chart.interactive(), width="stretch")

                        with col_pace:
                            st.caption("**Pace**")
                            pace_chart_data = speed_data.dropna(subset=["pace_display"])
                            if not pace_chart_data.empty:
                                chart = (
                                    alt.Chart(pace_chart_data)
                                    .mark_line(color="green", strokeWidth=2)
                                    .encode(
                                        x=alt.X("distance_display:Q", title=dist_label),
                                        y=alt.Y(
                                            "pace_display:Q",
                                            title=pace_label,
                                            scale=alt.Scale(zero=False, reverse=True),
                                        ),
                                        tooltip=[
                                            alt.Tooltip(
                                                "distance_display:Q",
                                                title=dist_label,
                                                format=".2f",
                                            ),
                                            alt.Tooltip(
                                                "pace_display:Q",
                                                title=pace_label,
                                                format=".2f",
                                            ),
                                        ],
                                    )
                                )
                                st.altair_chart(chart.interactive(), width="stretch")
                    else:
                        st.info("No speed data available")
                else:
                    st.info("No speed data available")

            with chart_tabs["Combined"]:
                if "distance_display" in records_df.columns:
                    has_hr = "heart_rate_bpm" in records_df.columns
                    has_spd = "speed_mps" in records_df.columns
                    has_alt = "altitude_m" in records_df.columns

                    combined_cols = ["distance_display"]
                    if has_hr:
                        combined_cols.append("heart_rate_bpm")
                    if has_spd:
                        combined_cols.append("speed_mps")
                    if has_alt:
                        combined_cols.append("altitude_m")

                    combo_data = records_df[combined_cols].dropna(
                        subset=["distance_display"]
                    )

                    if not combo_data.empty:
                        charts = []
                        if has_hr:
                            charts.append(
                                alt.Chart(combo_data.dropna(subset=["heart_rate_bpm"]))
                                .mark_line(color="red", strokeWidth=1.5)
                                .encode(
                                    x=alt.X("distance_display:Q", title=dist_label),
                                    y=alt.Y(
                                        "heart_rate_bpm:Q",
                                        title="HR (bpm)",
                                        scale=alt.Scale(zero=False),
                                    ),
                                )
                            )
                        if has_spd:
                            charts.append(
                                alt.Chart(combo_data.dropna(subset=["speed_mps"]))
                                .mark_line(color="blue", strokeWidth=1.5)
                                .encode(
                                    x=alt.X("distance_display:Q", title=dist_label),
                                    y=alt.Y(
                                        "speed_mps:Q",
                                        title="Speed (m/s)",
                                        scale=alt.Scale(zero=False),
                                    ),
                                )
                            )
                        if charts:
                            combined_chart = charts[0]
                            for c in charts[1:]:
                                combined_chart = combined_chart + c
                            st.altair_chart(
                                combined_chart.resolve_scale(
                                    y="independent"
                                ).interactive(),
                                width="stretch",
                            )
                        else:
                            st.info("No chart data available")
                    else:
                        st.info("No data available")
                else:
                    st.info("No data available")

        st.divider()
        st.subheader("Activity Stats")

        # Selected activity summary values
        distance_km = selected_row.get("distance_km") if selected_row else None
        elapsed_min = selected_row.get("elapsed_min") if selected_row else None
        avg_hr = selected_row.get("avg_hr") if selected_row else None
        max_hr = selected_row.get("max_hr") if selected_row else None

        avg_speed_mps = None
        avg_pace_min_per_km = None
        if distance_km and elapsed_min and distance_km > 0 and elapsed_min > 0:
            avg_speed_mps = (distance_km * 1000.0) / (elapsed_min * 60.0)
            avg_pace_min_per_km = elapsed_min / distance_km

        elevation_min = None
        elevation_max = None
        if not trackpoints_df.empty and "altitude_m" in trackpoints_df.columns:
            alt_series = trackpoints_df["altitude_m"].dropna()
            if not alt_series.empty:
                elevation_min = float(alt_series.min())
                elevation_max = float(alt_series.max())

        stats_c1, stats_c2, stats_c3, stats_c4 = st.columns(4)

        if distance_km is not None:
            if unit_system == "Imperial":
                stats_c1.metric("Distance", f"{distance_km * 0.621371:.2f} mi")
            else:
                stats_c1.metric("Distance", f"{distance_km:.2f} km")
        else:
            stats_c1.metric("Distance", "—")

        if elapsed_min is not None:
            hours = int(elapsed_min // 60)
            minutes = int(elapsed_min % 60)
            seconds = int(round((elapsed_min - int(elapsed_min)) * 60))
            stats_c2.metric("Duration", f"{hours:02d}:{minutes:02d}:{seconds:02d}")
        else:
            stats_c2.metric("Duration", "—")

        stats_c3.metric("Avg HR", f"{int(avg_hr)} bpm" if pd.notna(avg_hr) else "—")
        stats_c4.metric("Max HR", f"{int(max_hr)} bpm" if pd.notna(max_hr) else "—")

        stats_c5, stats_c6, stats_c7, stats_c8 = st.columns(4)

        if avg_speed_mps is not None:
            if unit_system == "Imperial":
                stats_c5.metric("Avg Speed", f"{avg_speed_mps * 2.23694:.2f} mph")
            else:
                stats_c5.metric("Avg Speed", f"{avg_speed_mps * 3.6:.2f} km/h")
        else:
            stats_c5.metric("Avg Speed", "—")

        if avg_pace_min_per_km is not None:
            if unit_system == "Imperial":
                pace = avg_pace_min_per_km / 0.621371
                stats_c6.metric("Avg Pace", f"{pace:.2f} min/mi")
            else:
                stats_c6.metric("Avg Pace", f"{avg_pace_min_per_km:.2f} min/km")
        else:
            stats_c6.metric("Avg Pace", "—")

        if elevation_min is not None and elevation_max is not None:
            elev_range = elevation_max - elevation_min
            if unit_system == "Imperial":
                stats_c7.metric("Elevation Range", f"{elev_range * 3.28084:.0f} ft")
            else:
                stats_c7.metric("Elevation Range", f"{elev_range:.0f} m")
        else:
            stats_c7.metric("Elevation Range", "—")

        stats_c8.metric("Trackpoints", f"{len(trackpoints_df):,}")

conn.close()
