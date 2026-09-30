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
    source_plan_id     TEXT,
    source_revision_id TEXT,
    source_workout_id  TEXT,
    created_at         TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_planned_workout_revision_projection
  ON planned_workout(source_plan_id, source_revision_id);

-- =========================
--  A2) IMMUTABLE PLAN REVISIONS (DATA HUB-OWNED)
-- =========================
CREATE TABLE IF NOT EXISTS training_plan (
  plan_id             TEXT PRIMARY KEY,
  current_revision_id TEXT,
  origin              TEXT NOT NULL DEFAULT 'NATIVE'
    CHECK (origin IN ('NATIVE', 'LEGACY_CONVERSION')),
  created_at_utc      TEXT NOT NULL,
  FOREIGN KEY (plan_id, current_revision_id)
    REFERENCES plan_revision(plan_id, revision_id)
    ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS plan_revision (
  revision_id              TEXT PRIMARY KEY,
  plan_id                  TEXT NOT NULL,
  parent_revision_id       TEXT,
  revision_reason          TEXT NOT NULL,
  methodology_id           TEXT NOT NULL,
  methodology_version      INTEGER NOT NULL CHECK (methodology_version >= 0),
  content_sha256           TEXT NOT NULL UNIQUE
    CHECK (
      length(content_sha256) = 64
      AND content_sha256 = lower(content_sha256)
      AND content_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
  manifest_json            TEXT NOT NULL,
  goal_snapshot_json       TEXT NOT NULL,
  athlete_snapshot_json    TEXT NOT NULL,
  parameter_snapshot_json  TEXT NOT NULL,
  constraints_json         TEXT NOT NULL DEFAULT '{}',
  validation_summary_json  TEXT,
  provenance_json          TEXT,
  change_summary_json      TEXT,
  approval_state           TEXT NOT NULL CHECK (approval_state = 'APPROVED'),
  approved_by              TEXT NOT NULL,
  approved_at_utc          TEXT NOT NULL,
  created_at_utc           TEXT NOT NULL,
  UNIQUE (plan_id, revision_id),
  FOREIGN KEY (plan_id) REFERENCES training_plan(plan_id) ON DELETE RESTRICT,
  FOREIGN KEY (plan_id, parent_revision_id)
    REFERENCES plan_revision(plan_id, revision_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_plan_revision_plan_approved
  ON plan_revision(plan_id, approved_at_utc, revision_id);
CREATE INDEX IF NOT EXISTS idx_plan_revision_parent
  ON plan_revision(plan_id, parent_revision_id);
CREATE INDEX IF NOT EXISTS idx_plan_revision_methodology
  ON plan_revision(methodology_id, methodology_version);

CREATE TABLE IF NOT EXISTS plan_revision_workout (
  revision_id             TEXT NOT NULL,
  workout_id              TEXT NOT NULL,
  workout_ordinal         INTEGER NOT NULL CHECK (workout_ordinal >= 0),
  scheduled_date          TEXT NOT NULL,
  sport                   TEXT NOT NULL,
  family                  TEXT NOT NULL,
  purpose                 TEXT NOT NULL,
  title                   TEXT,
  description             TEXT NOT NULL,
  phase                   TEXT,
  event_flag              INTEGER NOT NULL DEFAULT 0 CHECK (event_flag IN (0, 1)),
  quality_flag            INTEGER NOT NULL DEFAULT 0 CHECK (quality_flag IN (0, 1)),
  long_run_flag           INTEGER NOT NULL DEFAULT 0 CHECK (long_run_flag IN (0, 1)),
  workout_metadata_json   TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (revision_id, workout_id),
  UNIQUE (revision_id, workout_ordinal),
  FOREIGN KEY (revision_id) REFERENCES plan_revision(revision_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_plan_revision_workout_calendar
  ON plan_revision_workout(revision_id, scheduled_date, workout_ordinal);

CREATE TABLE IF NOT EXISTS plan_workout_segment (
  revision_id                    TEXT NOT NULL,
  workout_id                     TEXT NOT NULL,
  segment_ordinal                INTEGER NOT NULL CHECK (segment_ordinal >= 0),
  segment_kind                   TEXT NOT NULL,
  purpose                        TEXT,
  load_mode                      TEXT NOT NULL CHECK (load_mode IN ('DURATION', 'DISTANCE', 'OPEN')),
  duration_seconds               INTEGER CHECK (duration_seconds IS NULL OR duration_seconds > 0),
  duration_role                  TEXT CHECK (duration_role IS NULL OR duration_role IN ('AUTHORITATIVE', 'ESTIMATED')),
  distance_metres                INTEGER CHECK (distance_metres IS NULL OR distance_metres > 0),
  distance_role                  TEXT CHECK (distance_role IS NULL OR distance_role IN ('AUTHORITATIVE', 'ESTIMATED')),
  repeat_group                   TEXT,
  repeat_iteration               INTEGER CHECK (repeat_iteration IS NULL OR repeat_iteration > 0),
  duration_conversion_ref        TEXT,
  prescription_methodology_id    TEXT,
  prescription_native_target     TEXT,
  primary_metric                 TEXT,
  primary_unit                   TEXT,
  primary_lower                  TEXT,
  primary_upper                  TEXT,
  primary_lower_inclusive        INTEGER CHECK (primary_lower_inclusive IS NULL OR primary_lower_inclusive IN (0, 1)),
  primary_upper_inclusive        INTEGER CHECK (primary_upper_inclusive IS NULL OR primary_upper_inclusive IN (0, 1)),
  secondary_metric               TEXT,
  secondary_unit                 TEXT,
  secondary_lower                TEXT,
  secondary_upper                TEXT,
  secondary_lower_inclusive      INTEGER CHECK (secondary_lower_inclusive IS NULL OR secondary_lower_inclusive IN (0, 1)),
  secondary_upper_inclusive      INTEGER CHECK (secondary_upper_inclusive IS NULL OR secondary_upper_inclusive IN (0, 1)),
  ceiling_value                  TEXT,
  ceiling_inclusive              INTEGER CHECK (ceiling_inclusive IS NULL OR ceiling_inclusive IN (0, 1)),
  derivation_ref                 TEXT,
  confidence                     TEXT,
  data_quality_requirement       TEXT,
  parameter_snapshot_ref         TEXT,
  confidence_reason              TEXT,
  prescription_qualifiers_json   TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (revision_id, workout_id, segment_ordinal),
  FOREIGN KEY (revision_id, workout_id)
    REFERENCES plan_revision_workout(revision_id, workout_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_plan_workout_segment_target
  ON plan_workout_segment(prescription_methodology_id, prescription_native_target);

-- =========================
--  A3) ACTIVITY / REVISION-WORKOUT MATCHES (DATA HUB-OWNED)
-- =========================
CREATE TABLE IF NOT EXISTS activity_workout_match (
  activity_workout_match_id INTEGER PRIMARY KEY,
  revision_id               TEXT NOT NULL,
  workout_id                TEXT NOT NULL,
  activity_id               INTEGER NOT NULL,
  status                    TEXT NOT NULL
    CHECK (status IN ('CANDIDATE', 'CONFIRMED', 'REJECTED')),
  source                    TEXT NOT NULL
    CHECK (source IN ('MANUAL', 'RECONCILIATION', 'IMPORTED')),
  confidence                TEXT NOT NULL
    CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW', 'UNKNOWN')),
  reviewer                  TEXT,
  reason                    TEXT,
  created_at_utc            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at_utc            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  CHECK (
    status != 'CONFIRMED'
    OR (
      length(trim(COALESCE(reviewer, ''))) > 0
      AND length(trim(COALESCE(reason, ''))) > 0
    )
  ),
  UNIQUE (revision_id, workout_id, activity_id),
  FOREIGN KEY (revision_id, workout_id)
    REFERENCES plan_revision_workout(revision_id, workout_id) ON DELETE RESTRICT,
  FOREIGN KEY (activity_id) REFERENCES activity(activity_id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_activity_workout_match_confirmed_workout
  ON activity_workout_match(revision_id, workout_id)
  WHERE status = 'CONFIRMED';
CREATE UNIQUE INDEX IF NOT EXISTS uq_activity_workout_match_confirmed_activity
  ON activity_workout_match(activity_id)
  WHERE status = 'CONFIRMED';
CREATE INDEX IF NOT EXISTS idx_activity_workout_match_status
  ON activity_workout_match(status, revision_id, workout_id, activity_id);

CREATE TRIGGER IF NOT EXISTS trg_activity_workout_match_confirmed_no_update
BEFORE UPDATE ON activity_workout_match
WHEN OLD.status = 'CONFIRMED'
BEGIN
  SELECT RAISE(ABORT, 'confirmed activity/workout matches are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_activity_workout_match_confirmed_no_delete
BEFORE DELETE ON activity_workout_match
WHEN OLD.status = 'CONFIRMED'
BEGIN
  SELECT RAISE(ABORT, 'confirmed activity/workout matches are immutable');
END;

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
  performance_condition_end   REAL,
  refresh_provenance_version  INTEGER,
  threshold_lthr_bpm          INTEGER,
  threshold_ftp_w             INTEGER,
  threshold_resting_hr_bpm    INTEGER
);

-- =========================
--  C) THRESHOLD CALCULATIONS / ATHLETE PROFILE
-- =========================
CREATE TABLE IF NOT EXISTS threshold_calculation (
    threshold_calculation_id       INTEGER PRIMARY KEY,
    threshold_type                 TEXT NOT NULL,
    calculated_value               INTEGER NOT NULL,
    algorithm_version              TEXT NOT NULL,
    calculated_at_utc              TEXT,
    evidence_cutoff_utc             TEXT,
    evidence_at_utc                 TEXT,
    source_kind                     TEXT NOT NULL,
    source_activity_id              INTEGER,
    source_activity_timestamp_utc   TEXT,
    source_sport                    TEXT,
    evidence_value                  REAL,
    evidence_duration_s             REAL,
    candidate_count                 INTEGER,
    aggregate_evidence_json         TEXT,
    parent_calculation_id           INTEGER,
    FOREIGN KEY (parent_calculation_id)
      REFERENCES threshold_calculation(threshold_calculation_id)
);

CREATE INDEX IF NOT EXISTS idx_threshold_calculation_type_id
  ON threshold_calculation(threshold_type, threshold_calculation_id);

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
    override_updated_utc TEXT,
    hrmax_calculation_id INTEGER,
    lthr_calculation_id INTEGER,
    ftp_calculation_id INTEGER,
    resting_hr_calculation_id INTEGER,
    FOREIGN KEY (hrmax_calculation_id)
      REFERENCES threshold_calculation(threshold_calculation_id),
    FOREIGN KEY (lthr_calculation_id)
      REFERENCES threshold_calculation(threshold_calculation_id),
    FOREIGN KEY (ftp_calculation_id)
      REFERENCES threshold_calculation(threshold_calculation_id),
    FOREIGN KEY (resting_hr_calculation_id)
      REFERENCES threshold_calculation(threshold_calculation_id)
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

-- =========================
--  F) MANUALLY IMPORTED PLAN HISTORY
-- =========================
-- Stores only plans the athlete explicitly approved in the UI. Uploading a
-- ChatGPT response for preview does not write to this table.
CREATE TABLE IF NOT EXISTS plan_import_history (
  plan_import_id          INTEGER PRIMARY KEY,
  source                  TEXT NOT NULL DEFAULT 'chatgpt_manual_upload',
  schema_version          TEXT NOT NULL,
  content_sha256          TEXT NOT NULL UNIQUE,
  plan_name               TEXT NOT NULL,
  replace_start_date      TEXT NOT NULL,
  replace_end_date        TEXT NOT NULL,
  workout_count           INTEGER NOT NULL,
  payload_json            TEXT NOT NULL,
  previous_plan_json      TEXT,
  validation_warnings_json TEXT NOT NULL DEFAULT '[]',
  applied_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_plan_import_history_applied_at
  ON plan_import_history(applied_at);

-- =========================
--  G) HISTORICAL ARCHIVE RECONCILIATION
-- =========================
CREATE TABLE IF NOT EXISTS archive_reconciliation (
  reconciliation_id      INTEGER PRIMARY KEY,
  archive_identity       TEXT NOT NULL,
  target_activity_id     INTEGER,
  archive_format         TEXT NOT NULL
    CHECK (archive_format IN ('fit', 'gpx', 'tcx', 'unknown')),
  source_size            INTEGER NOT NULL CHECK (source_size >= 0),
  source_mtime_ns        INTEGER NOT NULL CHECK (source_mtime_ns >= 0),
  source_sha256          TEXT NOT NULL
    CHECK (
      length(source_sha256) = 64
      AND source_sha256 = lower(source_sha256)
      AND source_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
  algorithm_version      TEXT NOT NULL,
  status                 TEXT NOT NULL
    CHECK (status IN (
      'ingested', 'resolved_no_records', 'unsupported', 'malformed',
      'mismatch', 'ambiguous', 'parser_error', 'write_error'
    )),
  reason_code            TEXT,
  identity_evidence_json TEXT NOT NULL DEFAULT '{}',
  trackpoint_count       INTEGER NOT NULL DEFAULT 0 CHECK (trackpoint_count >= 0),
  attempted_at_utc       TEXT NOT NULL,
  completed_at_utc       TEXT,
  FOREIGN KEY (target_activity_id) REFERENCES activity(activity_id),
  UNIQUE (
    archive_identity, target_activity_id, source_size, source_mtime_ns,
    source_sha256, algorithm_version
  )
);

CREATE INDEX IF NOT EXISTS idx_archive_reconciliation_fingerprint
  ON archive_reconciliation(
    archive_identity, target_activity_id, source_size, source_mtime_ns,
    source_sha256, algorithm_version
  );

CREATE INDEX IF NOT EXISTS idx_archive_reconciliation_status
  ON archive_reconciliation(status, target_activity_id);
