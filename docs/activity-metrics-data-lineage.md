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

The refresh is a current-state cache rebuild. It clears only the columns it
owns, then repopulates them from the current summary and trackpoint sources in
one transaction. A missing current source therefore produces `NULL`; it does
not inherit a value from an older refresh. Numeric zero remains a measured
value, distinct from missing data.

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
| `aerobic_decoupling_pct` | elapsed-time first/second-half power-or-speed efficiency | App-derived; one workload signal is selected for the whole activity |
| `hr_drift_pct` | elapsed-time first/second-half HR drift from trackpoints | App-derived |
| `avg_hr_to_max_pct` | `average_hr / max_hr` | App-derived |
| `zone_1_s` ... `zone_5_s` | HR trackpoints + effective LTHR | App-derived HR zone totals |
| `variability_index` | `norm_power / avg_power` | App-derived |
| `efficiency_factor` | `norm_power / average_hr` or `speed / average_hr` | App-derived |
| `pace_decoupling_pct` | elapsed-time speed-vs-HR first/second half comparison | App-derived; always speed-based |
| `peak_power_5s_w` | exact-duration time-weighted mean of `activity_trackpoint.power_w` | App-derived |
| `peak_power_30s_w` | exact-duration time-weighted mean of `activity_trackpoint.power_w` | App-derived |
| `peak_power_60s_w` | exact-duration time-weighted mean of `activity_trackpoint.power_w` | App-derived |
| `peak_power_300s_w` | exact-duration time-weighted mean of `activity_trackpoint.power_w` | App-derived |
| `peak_power_1200s_w` | exact-duration time-weighted mean of `activity_trackpoint.power_w` | App-derived |
| `power_zone_1_s` ... `power_zone_7_s` | power trackpoints + effective FTP | App-derived power zone totals |

#### Temporal support used by power peaks and decoupling

Trackpoint values are left-held from their timestamp to the next distinct
timestamp. No duration is inferred after the final trackpoint. Intervals of
exactly 30 seconds are eligible; a gap greater than 30 seconds creates no
support and breaks a continuous power run. Duplicate timestamps are resolved
deterministically and never create duration.

Measured zero power or speed is valid data. `NULL` is missing data. Power peaks
require a complete contiguous interval of the requested real elapsed duration,
and integrate measured watt-seconds over that duration; missing power and gaps
cannot be compressed out of a window.

Decoupling uses the wall-clock midpoint of the supported observed span and
splits a crossing interval at that midpoint. Each half is time-weighted, and
workload and HR are averaged over the same paired intervals. Pace decoupling
always uses speed. Aerobic decoupling uses power when paired power coverage is
at least 95%; otherwise it uses speed for the entire calculation. The coverage
numerator is the duration of eligible (at most 30-second) intervals whose
left-held HR is within 35--220 bpm and whose left-held power is measured and
non-negative. The denominator is the duration of all eligible intervals whose
left-held HR is within those bounds. Broken gaps and missing/invalid HR add to
neither duration. Thus, the percentage means power completeness wherever HR
permits decoupling, not power completeness across the whole activity. HR drift
uses the same elapsed midpoint and HR validity bounds but is weighted over
HR-supported intervals independently of workload.

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

The refresh deliberately does not clear these fields because it does not own
their population.

### E) Refresh provenance

| `activity_metrics` column | Meaning |
|---|---|
| `refresh_provenance_version` | Version of the refresh provenance contract that produced the row |
| `threshold_lthr_bpm` | Effective LTHR used by the refresh |
| `threshold_ftp_w` | Effective FTP used by the refresh |
| `threshold_resting_hr_bpm` | Effective resting HR used by the refresh |

`list_activities_needing_metrics()` compares this provenance with the current
effective athlete profile. Threshold updates therefore make dependent cached
metrics stale without relying on incidental `NULL` checks. Any non-`NULL`
provenance version other than the current version is always stale. Legacy rows
have nullable provenance; `lthr_est_bpm` is used as a conservative compatibility
fallback, while legacy FTP/IF and resting-HR-dependent values are invalidated
when their current inputs cannot match the historical defaults.

Normal chart, compliance, activity-detail, and coaching-packet readers also
check provenance before exposing threshold-dependent cache fields. Stale HR or
power zones are shown as zero, and stale derived loads are omitted (or fall back
to the raw Garmin TSS when available).

The Phase 2D temporal fields (`aerobic_decoupling_pct`,
`pace_decoupling_pct`, `hr_drift_pct`, and all five `peak_power_*_w` fields)
have an additional algorithm-freshness rule: normal readers expose them only
when `refresh_provenance_version` exactly equals the current provenance version.
Version 1, legacy `NULL`, and unknown future versions are unavailable to normal
readers. Their stored values may physically remain in SQLite after a pending or
failed refresh, but stay suppressed until a successful current-version refresh
atomically replaces the metrics and provenance. Unrelated threshold-independent
fields remain available under the existing Phase 2B rules.

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
