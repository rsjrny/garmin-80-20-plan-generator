-- =========================
--  APP-INTERNAL TABLES ONLY
--  (Activity and health data live in the garmin-givemydata SQLite DB.
--   apply_schema() appends these tables to that same DB file.)
-- =========================

-- =========================
--  A) PLANNED WORKOUTS
-- =========================
CREATE TABLE IF NOT EXISTS planned_workout (
    planned_workout_id INTEGER PRIMARY KEY,
    scheduled_date     TEXT NOT NULL,
    workout_name       TEXT,
    description        TEXT,
    planned_distance_m REAL,
    planned_duration_s REAL,
    planned_tss        REAL,
    structure_json     TEXT,
    created_at         TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- =========================
--  B) DERIVED / CALCULATED METRICS
-- =========================
CREATE TABLE IF NOT EXISTS activity_metrics (
  activity_id          INTEGER PRIMARY KEY,
  moving_time_s        REAL,
  stopped_time_s       REAL,
  avg_moving_speed_mps REAL,
  hr_max_est_bpm       REAL,
  lthr_est_bpm         REAL,
  trimp                REAL,
  aerobic_decoupling_pct REAL,
  hr_drift_pct         REAL,
  hr_recovery_60s_bpm  REAL,
  avg_hr_to_max_pct    REAL,
  zone_1_s             REAL,
  zone_2_s             REAL,
  zone_3_s             REAL,
  zone_4_s             REAL,
  zone_5_s             REAL,
  np_w                 REAL,
  if_val               REAL,
  tss                  REAL,
  variability_index    REAL,
  avg_power_w          REAL,
  max_power_w          REAL,
  peak_power_5s_w      REAL,
  peak_power_30s_w     REAL,
  peak_power_60s_w     REAL,
  peak_power_300s_w    REAL,
  peak_power_1200s_w   REAL,
  power_zone_1_s       REAL,
  power_zone_2_s       REAL,
  power_zone_3_s       REAL,
  power_zone_4_s       REAL,
  power_zone_5_s       REAL,
  power_zone_6_s       REAL,
  power_zone_7_s       REAL,
  efficiency_factor    REAL,
  pace_decoupling_pct  REAL,
  avg_cadence_spm      REAL,
  avg_stride_length_m  REAL,
  avg_vertical_osc_cm  REAL,
  avg_ground_contact_ms REAL,
  avg_vertical_ratio   REAL,
  gct_balance_avg_pct  REAL,
  avg_temperature_c    REAL,
  min_temperature_c    REAL,
  max_temperature_c    REAL,
  total_ascent_m       REAL,
  total_descent_m      REAL,
  max_altitude_m       REAL,
  min_altitude_m       REAL,
  training_effect_aerobic   REAL,
  training_effect_anaerobic REAL,
  performance_condition_start REAL,
  performance_condition_end   REAL
);

-- =========================
--  C) ATHLETE PROFILE
-- =========================
CREATE TABLE IF NOT EXISTS athlete_profile (
    profile_id           INTEGER PRIMARY KEY DEFAULT 1,
    hrmax_calc           INTEGER,
    lthr_calc            INTEGER,
    ftp_calc             INTEGER,
    calc_updated_utc     TEXT,
    hrmax_override       INTEGER,
    lthr_override        INTEGER,
    ftp_override         INTEGER,
    resting_hr           INTEGER,
    override_updated_utc TEXT
);

INSERT OR IGNORE INTO athlete_profile(profile_id) VALUES (1);

-- =========================
--  D) APP SETTINGS
-- =========================
CREATE TABLE IF NOT EXISTS app_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- =========================
--  E) TRACKPOINTS (PER-SAMPLE GPS/PHYSIOLOGY STREAM)
-- =========================
CREATE TABLE IF NOT EXISTS activity_trackpoints (
  activity_id    INTEGER NOT NULL,
  seq            INTEGER NOT NULL,
  timestamp_utc  TEXT NOT NULL,
  latitude       REAL,
  longitude      REAL,
  altitude_m     REAL,
  distance_m     REAL,
  speed_mps      REAL,
  heart_rate_bpm INTEGER,
  cadence        INTEGER,
  power_w        INTEGER,
  temperature_c  REAL,
  PRIMARY KEY (activity_id, seq),
  FOREIGN KEY (activity_id) REFERENCES activity(activity_id)
);

CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_activity_time
  ON activity_trackpoints(activity_id, timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_latlon
  ON activity_trackpoints(latitude, longitude);
