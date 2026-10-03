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
--  A4) EXPLICIT LEGACY PLAN CONVERSION (DATA HUB-OWNED)
-- =========================
CREATE TABLE IF NOT EXISTS legacy_plan_conversion (
  canonical_legacy_plan_id   TEXT PRIMARY KEY,
  source_namespace           TEXT NOT NULL
    CHECK (source_namespace = 'garmin_data_hub.planned_workout.rowset.v1'),
  source_membership_sha256   TEXT NOT NULL
    CHECK (
      length(source_membership_sha256) = 64
      AND source_membership_sha256 = lower(source_membership_sha256)
      AND source_membership_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
  source_snapshot_sha256     TEXT NOT NULL
    CHECK (
      length(source_snapshot_sha256) = 64
      AND source_snapshot_sha256 = lower(source_snapshot_sha256)
      AND source_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
  target_plan_id             TEXT NOT NULL UNIQUE,
  initial_revision_id        TEXT NOT NULL UNIQUE,
  selected_methodology_id    TEXT NOT NULL
    CHECK (selected_methodology_id IN (
      'FITZGERALD_80_20_RUNNING_V1', 'MAFFETONE_RUNNING_V1'
    )),
  conversion_version         TEXT NOT NULL,
  conversion_source          TEXT NOT NULL
    CHECK (conversion_source IN (
      'USER_ACTION', 'APPLICATION_COMMAND', 'REVIEWED_MIGRATION'
    )),
  converted_by               TEXT NOT NULL CHECK (length(trim(converted_by)) > 0),
  converted_at_utc           TEXT NOT NULL CHECK (length(trim(converted_at_utc)) > 0),
  UNIQUE (source_namespace, source_membership_sha256),
  FOREIGN KEY (target_plan_id, initial_revision_id)
    REFERENCES plan_revision(plan_id, revision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS legacy_plan_conversion_source_workout (
  canonical_legacy_plan_id TEXT NOT NULL,
  planned_workout_id       INTEGER NOT NULL UNIQUE,
  source_ordinal           INTEGER NOT NULL CHECK (source_ordinal >= 0),
  PRIMARY KEY (canonical_legacy_plan_id, planned_workout_id),
  UNIQUE (canonical_legacy_plan_id, source_ordinal),
  FOREIGN KEY (canonical_legacy_plan_id)
    REFERENCES legacy_plan_conversion(canonical_legacy_plan_id) ON DELETE RESTRICT,
  FOREIGN KEY (planned_workout_id)
    REFERENCES planned_workout(planned_workout_id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_planned_workout_revision_projection
  ON planned_workout(source_plan_id, source_revision_id, source_workout_id)
  WHERE source_plan_id IS NOT NULL
    AND source_revision_id IS NOT NULL
    AND source_workout_id IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS trg_legacy_plan_conversion_no_update
BEFORE UPDATE ON legacy_plan_conversion
BEGIN
  SELECT RAISE(ABORT, 'legacy plan conversion provenance is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_legacy_plan_conversion_no_delete
BEFORE DELETE ON legacy_plan_conversion
BEGIN
  SELECT RAISE(ABORT, 'legacy plan conversion provenance is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_legacy_plan_conversion_source_no_update
BEFORE UPDATE ON legacy_plan_conversion_source_workout
BEGIN
  SELECT RAISE(ABORT, 'legacy plan conversion membership is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_legacy_plan_conversion_source_no_delete
BEFORE DELETE ON legacy_plan_conversion_source_workout
BEGIN
  SELECT RAISE(ABORT, 'legacy plan conversion membership is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_mapped_legacy_planned_workout_no_update
BEFORE UPDATE ON planned_workout
WHEN EXISTS (
  SELECT 1 FROM legacy_plan_conversion_source_workout AS source_map
  WHERE source_map.planned_workout_id = OLD.planned_workout_id
)
BEGIN
  SELECT RAISE(ABORT, 'mapped legacy planned workouts are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_mapped_legacy_planned_workout_no_delete
BEFORE DELETE ON planned_workout
WHEN EXISTS (
  SELECT 1 FROM legacy_plan_conversion_source_workout AS source_map
  WHERE source_map.planned_workout_id = OLD.planned_workout_id
)
BEGIN
  SELECT RAISE(ABORT, 'mapped legacy planned workouts cannot be deleted');
END;

CREATE VIEW IF NOT EXISTS active_planned_workout AS
SELECT pw.*
FROM planned_workout AS pw
WHERE
  (
    pw.source_plan_id IS NULL
    AND pw.source_revision_id IS NULL
    AND pw.source_workout_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM legacy_plan_conversion_source_workout AS source_map
      WHERE source_map.planned_workout_id = pw.planned_workout_id
    )
  )
  OR
  (
    pw.source_plan_id IS NOT NULL
    AND pw.source_revision_id IS NOT NULL
    AND pw.source_workout_id IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM training_plan AS target
      WHERE target.plan_id = pw.source_plan_id
        AND target.current_revision_id = pw.source_revision_id
    )
  );

-- =========================
--  A5) EDITABLE SEASON PLANNING INTENT (DATA HUB-OWNED)
-- =========================
CREATE TABLE IF NOT EXISTS season_plan (
  season_id TEXT PRIMARY KEY,
  plan_id TEXT UNIQUE REFERENCES training_plan(plan_id) ON DELETE RESTRICT,
  name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 200),
  start_date TEXT NOT NULL CHECK (length(start_date) = 10),
  end_date TEXT NOT NULL CHECK (length(end_date) = 10 AND end_date >= start_date),
  timezone TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','archived')),
  input_schema_version TEXT NOT NULL CHECK (input_schema_version = 'season-input.v1'),
  inputs_json TEXT NOT NULL,
  input_version INTEGER NOT NULL DEFAULT 1 CHECK (input_version >= 1),
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  CHECK (julianday(end_date) - julianday(start_date) BETWEEN 0 AND 365),
  CHECK (status != 'active' OR plan_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_season_plan_dates ON season_plan(status,start_date,end_date);
CREATE TRIGGER IF NOT EXISTS trg_season_plan_identity_no_update
BEFORE UPDATE OF plan_id ON season_plan
WHEN OLD.plan_id IS NOT NULL AND NEW.plan_id IS NOT OLD.plan_id
BEGIN SELECT RAISE(ABORT, 'season plan identity cannot be changed'); END;
CREATE TRIGGER IF NOT EXISTS trg_season_plan_owned_no_delete
BEFORE DELETE ON season_plan WHEN OLD.plan_id IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'archive linked seasons instead of deleting them'); END;

CREATE TABLE IF NOT EXISTS season_event (
  event_id TEXT PRIMARY KEY,
  season_id TEXT NOT NULL REFERENCES season_plan(season_id) ON DELETE RESTRICT,
  name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 200),
  event_date TEXT NOT NULL CHECK (length(event_date) = 10),
  sport TEXT NOT NULL CHECK (sport = 'RUNNING'),
  event_type TEXT NOT NULL CHECK (event_type IN ('road','trail')),
  distance_code TEXT NOT NULL CHECK (distance_code IN ('5K','10K','10M','HM','20M','MAR','50K','50M','100K','100M')),
  distance_metres INTEGER NOT NULL CHECK (distance_metres > 0),
  priority TEXT NOT NULL CHECK (priority IN ('A','B','C')),
  status TEXT NOT NULL CHECK (status IN ('planned','completed','skipped','cancelled')),
  goal_intent TEXT NOT NULL CHECK (goal_intent IN ('COMPLETION','PERFORMANCE')),
  target_seconds INTEGER CHECK (target_seconds IS NULL OR target_seconds > 0),
  target_speed_mps TEXT,
  terrain TEXT NOT NULL DEFAULT '',
  course_notes TEXT NOT NULL DEFAULT '',
  taper_days INTEGER CHECK (taper_days IS NULL OR taper_days BETWEEN 0 AND 28),
  recovery_days INTEGER CHECK (recovery_days IS NULL OR recovery_days BETWEEN 0 AND 42),
  participation_seconds INTEGER CHECK (participation_seconds IS NULL OR (typeof(participation_seconds) = 'integer' AND participation_seconds BETWEEN 1 AND 604800 AND goal_intent = 'COMPLETION')),
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  CHECK (target_seconds IS NULL OR target_speed_mps IS NULL)
);
CREATE INDEX IF NOT EXISTS idx_season_event_calendar ON season_event(season_id,event_date,event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_season_event_active_date
  ON season_event(season_id,event_date) WHERE status IN ('planned','completed');

-- =========================
--  A6) IMMUTABLE SEASON APPLICATION AUDIT (DATA HUB-OWNED)
CREATE TABLE IF NOT EXISTS season_revision_application (
  application_id TEXT PRIMARY KEY,
  preview_id TEXT NOT NULL,
  season_id TEXT NOT NULL REFERENCES season_plan(season_id) ON DELETE RESTRICT,
  plan_id TEXT NOT NULL,
  resulting_revision_id TEXT NOT NULL,
  previous_revision_id TEXT,
  input_version INTEGER NOT NULL CHECK (input_version > 0),
  input_sha256 TEXT NOT NULL CHECK (length(input_sha256)=64),
  candidate_sha256 TEXT NOT NULL CHECK (length(candidate_sha256)=64),
  snapshot_json TEXT NOT NULL,
  review_json TEXT NOT NULL,
  generator_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  local_today TEXT NOT NULL,
  timezone TEXT NOT NULL,
  range_start TEXT NOT NULL,
  range_end TEXT NOT NULL CHECK (range_end >= range_start),
  mode TEXT NOT NULL CHECK (mode IN ('INITIAL_FULL','FROM_TODAY','AFFECTED','CUSTOM','MANUAL_EDIT')),
  approved_by TEXT NOT NULL CHECK (length(trim(approved_by)) > 0),
  applied_at_utc TEXT NOT NULL,
  UNIQUE (season_id,preview_id),
  FOREIGN KEY (plan_id,resulting_revision_id) REFERENCES plan_revision(plan_id,revision_id) ON DELETE RESTRICT,
  FOREIGN KEY (plan_id,previous_revision_id) REFERENCES plan_revision(plan_id,revision_id) ON DELETE RESTRICT
);
CREATE TRIGGER IF NOT EXISTS trg_season_application_owner
BEFORE INSERT ON season_revision_application
WHEN NOT EXISTS (SELECT 1 FROM season_plan WHERE season_id=NEW.season_id AND plan_id=NEW.plan_id)
BEGIN SELECT RAISE(ABORT, 'application revision must belong to its season'); END;
CREATE TRIGGER IF NOT EXISTS trg_season_application_no_update
BEFORE UPDATE ON season_revision_application
BEGIN SELECT RAISE(ABORT, 'season applications are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_season_application_no_delete
BEFORE DELETE ON season_revision_application
BEGIN SELECT RAISE(ABORT, 'season applications are immutable'); END;

--  A7) SEASON WORKOUT PROTECTION AND ORIGINS (DATA HUB-OWNED)
CREATE TABLE IF NOT EXISTS plan_revision_workout_origin (
  revision_id TEXT NOT NULL,
  workout_id TEXT NOT NULL,
  origin_revision_id TEXT NOT NULL,
  origin_workout_id TEXT NOT NULL,
  prescribed_sha256 TEXT NOT NULL CHECK (length(prescribed_sha256)=64),
  PRIMARY KEY (revision_id,workout_id),
  CHECK (revision_id != origin_revision_id),
  FOREIGN KEY (revision_id,workout_id) REFERENCES plan_revision_workout(revision_id,workout_id) ON DELETE RESTRICT,
  FOREIGN KEY (origin_revision_id,origin_workout_id) REFERENCES plan_revision_workout(revision_id,workout_id) ON DELETE RESTRICT
);
CREATE TRIGGER IF NOT EXISTS trg_workout_origin_owner
BEFORE INSERT ON plan_revision_workout_origin
WHEN NOT EXISTS (SELECT 1 FROM plan_revision c JOIN plan_revision o ON c.plan_id=o.plan_id
  WHERE c.revision_id=NEW.revision_id AND o.revision_id=NEW.origin_revision_id)
 OR EXISTS (SELECT 1 FROM plan_revision_workout_origin WHERE revision_id=NEW.origin_revision_id AND workout_id=NEW.origin_workout_id)
BEGIN SELECT RAISE(ABORT, 'workout origin must be flattened within one plan'); END;
CREATE TRIGGER IF NOT EXISTS trg_workout_origin_no_update
BEFORE UPDATE ON plan_revision_workout_origin
BEGIN SELECT RAISE(ABORT, 'workout origins are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_workout_origin_no_delete
BEFORE DELETE ON plan_revision_workout_origin
BEGIN SELECT RAISE(ABORT, 'workout origins are immutable'); END;
CREATE TABLE IF NOT EXISTS season_workout_state (
  season_id TEXT NOT NULL REFERENCES season_plan(season_id) ON DELETE RESTRICT,
  plan_id TEXT NOT NULL REFERENCES training_plan(plan_id) ON DELETE RESTRICT,
  workout_id TEXT NOT NULL,
  origin_revision_id TEXT NOT NULL,
  origin_workout_id TEXT NOT NULL,
  locked INTEGER NOT NULL DEFAULT 0 CHECK (locked IN (0,1)),
  manually_edited INTEGER NOT NULL DEFAULT 0 CHECK (manually_edited IN (0,1)),
  manually_created INTEGER NOT NULL DEFAULT 0 CHECK (manually_created IN (0,1)),
  explicitly_completed INTEGER NOT NULL DEFAULT 0 CHECK (explicitly_completed IN (0,1)),
  version INTEGER NOT NULL CHECK (version > 0),
  editor TEXT NOT NULL CHECK (length(trim(editor))>0),
  reason TEXT NOT NULL CHECK (length(trim(reason))>0),
  updated_at_utc TEXT NOT NULL,
  PRIMARY KEY (plan_id,workout_id),
  FOREIGN KEY (origin_revision_id,origin_workout_id) REFERENCES plan_revision_workout(revision_id,workout_id) ON DELETE RESTRICT
);
CREATE TRIGGER IF NOT EXISTS trg_season_workout_state_owner
BEFORE INSERT ON season_workout_state
WHEN NOT EXISTS (SELECT 1 FROM season_plan s JOIN plan_revision r ON r.plan_id=s.plan_id
 WHERE s.season_id=NEW.season_id AND s.plan_id=NEW.plan_id AND r.revision_id=NEW.origin_revision_id)
BEGIN SELECT RAISE(ABORT, 'workout state must belong to its season'); END;
CREATE TRIGGER IF NOT EXISTS trg_season_workout_state_version
BEFORE UPDATE ON season_workout_state
WHEN NEW.plan_id!=OLD.plan_id OR NEW.workout_id!=OLD.workout_id OR NEW.season_id!=OLD.season_id
 OR NEW.origin_revision_id!=OLD.origin_revision_id OR NEW.origin_workout_id!=OLD.origin_workout_id
 OR NEW.version!=OLD.version+1 OR NEW.explicitly_completed<OLD.explicitly_completed
 OR NEW.manually_created<OLD.manually_created OR NEW.manually_edited<OLD.manually_edited
BEGIN SELECT RAISE(ABORT, 'workout state identity, completion and manual provenance are retained; version must increase'); END;
CREATE TRIGGER IF NOT EXISTS trg_season_workout_state_no_delete
BEFORE DELETE ON season_workout_state
BEGIN SELECT RAISE(ABORT, 'workout protection history is retained'); END;

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
