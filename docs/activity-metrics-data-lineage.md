# Activity Metrics Data Lineage

This note explains **what comes directly from `garmin-givemydata`** versus **what Garmin Data Hub derives or caches** in its own tables.

## High-level ownership

| Table | Owner | Purpose |
|---|---|---|
| `activity` | `garmin-givemydata` | Canonical per-activity summary data imported from Garmin Connect |
| `activity_splits` | `garmin-givemydata` | Split/lap-level summary data |
| `activity_trackpoint` | Garmin Data Hub | App-ingested per-sample FIT trackpoints (HR, speed, power, temp, altitude, etc.) |
| `athlete_profile` | Garmin Data Hub | App-calculated and/or user-overridden HR/FTP profile values |
| `activity_metrics` | Garmin Data Hub | Cached and derived metrics used by charts, compliance, and analysis pages |

> `activity_metrics` is **not** a raw Garmin table. It is an app-owned summary table populated after sync by `refresh_persisted_activity_metrics()`.

---

## Current refresh path

After sync, the app runs:

1. `refresh_post_sync_tables()`
2. `update_athlete_profile()`
3. `refresh_persisted_activity_metrics()`

Relevant code:

- `src/garmin_data_hub/analytics/post_sync_refresh.py`
- `src/garmin_data_hub/analytics/athlete_profile.py`
- `src/garmin_data_hub/db/queries.py`

---

## `activity_metrics` column lineage

### A) Mostly copied or cached from `garmin-givemydata` `activity`

These live in `activity_metrics`, but their underlying values come from `activity` when available.

| `activity_metrics` column | Source | Notes |
|---|---|---|
| `avg_moving_speed_mps` | `activity.average_speed` | Cached for charting |
| `np_w` | `activity.norm_power` | Normalized power copied into app table |
| `if_val` | `activity.intensity_factor` | Falls back to derived value if missing |
| `tss` | `activity.training_stress_score` | Falls back to HR-based estimate if missing |
| `avg_power_w` | `activity.avg_power` or trackpoint aggregate | Trackpoint average overrides when present |
| `max_power_w` | `activity.max_power` or trackpoint aggregate | Trackpoint max overrides when present |
| `avg_cadence_spm` | `activity.avg_cadence` or trackpoint aggregate | Trackpoint average overrides when present |
| `training_effect_aerobic` | `activity.aerobic_training_effect` | Directly sourced from imported activity summary |
| `training_effect_anaerobic` | `activity.anaerobic_training_effect` | Directly sourced from imported activity summary |
| `total_ascent_m` | `activity.elevation_gain` or trackpoint-derived total | Trackpoint total overrides when available |
| `total_descent_m` | `activity.elevation_loss` or trackpoint-derived total | Trackpoint total overrides when available |
| `min_altitude_m` | `activity.min_elevation` or `activity_trackpoint.altitude_m` | Trackpoints preferred when present |
| `max_altitude_m` | `activity.max_elevation` or `activity_trackpoint.altitude_m` | Trackpoints preferred when present |
| `min_temperature_c` | `activity.min_temperature` or trackpoint aggregate | Trackpoints preferred when present |
| `max_temperature_c` | `activity.max_temperature` or trackpoint aggregate | Trackpoints preferred when present |
| `avg_temperature_c` | derived from min/max temp or trackpoint aggregate | Average is app-computed |

### B) App-derived from `activity`, `activity_trackpoint`, and/or `athlete_profile`

| `activity_metrics` column | Derived from | Notes |
|---|---|---|
| `moving_time_s` | `activity.moving_duration_seconds` else `activity.elapsed_duration_seconds` | Cached/fallback value |
| `stopped_time_s` | `elapsed_duration_seconds - moving_duration_seconds` | App-derived |
| `hr_max_est_bpm` | `activity.max_hr` or LTHR-based fallback | App-normalized estimate |
| `lthr_est_bpm` | `athlete_profile.lthr_override` / `lthr_calc` | Derived profile value |
| `trimp` | duration + HR + estimated HR reserve | App-derived training load |
| `aerobic_decoupling_pct` | first/second half `activity_trackpoint` efficiency | App-derived |
| `hr_drift_pct` | first/second half HR drift from trackpoints | App-derived |
| `avg_hr_to_max_pct` | `average_hr / max_hr` | App-derived |
| `zone_1_s` ... `zone_5_s` | HR trackpoints + effective LTHR | App-derived HR zone totals |
| `variability_index` | `norm_power / avg_power` | App-derived |
| `efficiency_factor` | `norm_power / average_hr` or `speed / average_hr` | App-derived |
| `pace_decoupling_pct` | speed-vs-HR first/second half comparison | App-derived |
| `peak_power_5s_w` | rolling average over `activity_trackpoint.power_w` | App-derived |
| `peak_power_30s_w` | rolling average over `activity_trackpoint.power_w` | App-derived |
| `peak_power_60s_w` | rolling average over `activity_trackpoint.power_w` | App-derived |
| `peak_power_300s_w` | rolling average over `activity_trackpoint.power_w` | App-derived |
| `peak_power_1200s_w` | rolling average over `activity_trackpoint.power_w` | App-derived |
| `power_zone_1_s` ... `power_zone_7_s` | power trackpoints + effective FTP | App-derived power zone totals |

### C) Athlete profile values used by the derivations

| Field | Table | Meaning |
|---|---|---|
| `hrmax_calc` / `lthr_calc` | `athlete_profile` | App-calculated HR profile |
| `hrmax_override` / `lthr_override` | `athlete_profile` | User override values |
| `ftp_calc` | `athlete_profile` | App-estimated FTP from recent power-enabled ride activities |
| `ftp_override` | `athlete_profile` | User override FTP |
| `resting_hr` | `athlete_profile` | Optional user-supplied resting HR |

### D) Currently present in schema but **not yet populated** by the active refresh logic

These columns exist for future expansion or legacy compatibility, but are still mostly `NULL` in the current code path.

- `hr_recovery_60s_bpm`
- `avg_stride_length_m`
- `avg_vertical_osc_cm`
- `avg_ground_contact_ms`
- `avg_vertical_ratio`
- `gct_balance_avg_pct`
- `performance_condition_start`
- `performance_condition_end`

---

## Practical rule of thumb

If you need:

- **raw Garmin summary values** → query `activity`
- **lap/split values** → query `activity_splits`
- **sample-level HR/power/GPS values** → query `activity_trackpoint`
- **UI-ready derived metrics** → query `activity_metrics`

---

## Important caveat

`activity_metrics` mixes:

1. **copied summary fields** from `garmin-givemydata`
2. **fallback estimates**
3. **true app-derived metrics** from trackpoints/profile data

So it should be treated as a **convenience/cache table**, not as the authoritative raw source of Garmin data.
