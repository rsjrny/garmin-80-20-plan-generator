"""Sport-specific performance with explicit signal and threshold qualification."""
from __future__ import annotations
import math
import re
import pandas as pd
import plotly.graph_objects as go

from .chart_overview import pace_text
from .chart_explorer import activity_targets, tag
from .temporal_metrics import POWER_PEAK_DURATIONS_SECONDS
from garmin_data_hub.services.thresholds import RUNNING_SPORTS, RUNNING_FTP_ALGORITHM_VERSION, ESTIMATED_LTHR_ALGORITHM_VERSION
from garmin_data_hub.db.queries import ACTIVITY_METRICS_PROVENANCE_VERSION

PERFORMANCE_CATALOG = (
    ("pace_at_hr","Pace at heart-rate band"), ("hr_at_pace","Heart rate at pace band"),
    ("running_efficiency","Running efficiency"), ("cadence","Cadence"),
    ("long_runs","Longest running activity by week"), ("durability","Qualified running durability"),
    ("power","Average and normalized power"), ("power_efficiency","Power variability and efficiency"),
    ("power_zones","Power zones"), ("power_peaks","Peak-power curve"),
)
DEFAULTS = dict(hr_low=125,hr_high=145,speed_low=2.5,speed_high=1000/300,
                min_minutes=20,terrain="Low ascent",charts=["pace_at_hr","cadence","durability"])
TERRAINS = ("Low ascent","Higher ascent","Unknown ascent","All terrain")
TRACE_COLORS = ("#2455a4", "#168ba5", "#509568", "#bc9235", "#c27646", "#804e95", "#9b415d")
POWER_ZONE_COLORS = ("#2455a4", "#168ba5", "#509568", "#bc9235", "#c27646", "#a95866", "#804e95")


def performance_state(saved):
    saved = saved if isinstance(saved,dict) else {}
    result = dict(DEFAULTS)
    for key,lo,hi in (("hr_low",35,220),("hr_high",35,220),("speed_low",.2,12),("speed_high",.2,12),("min_minutes",10,240)):
        value = saved.get(key)
        if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and lo <= value <= hi:
            result[key] = value
    if result["hr_low"] >= result["hr_high"]:
        result["hr_low"],result["hr_high"] = DEFAULTS["hr_low"],DEFAULTS["hr_high"]
    if result["speed_low"] >= result["speed_high"]:
        result["speed_low"],result["speed_high"] = DEFAULTS["speed_low"],DEFAULTS["speed_high"]
    result["terrain"] = saved.get("terrain") if saved.get("terrain") in TERRAINS else DEFAULTS["terrain"]
    charts = saved.get("charts")
    result["charts"] = [key for key,_ in PERFORMANCE_CATALOG if key in charts] if isinstance(charts,list) else list(DEFAULTS["charts"])
    return result


def parse_pace(text, unit_metres):
    if not isinstance(text,str) or not re.fullmatch(r"\d{1,2}:[0-5]\d",text.strip()):
        raise ValueError("Enter pace as minutes:seconds, for example 5:30.")
    minutes,seconds = map(int,text.strip().split(":"))
    total = minutes*60+seconds
    speed = unit_metres/total if total else 0
    if not .2 <= speed <= 12:
        raise ValueError("Pace must correspond to 0.2-12 m/s.")
    return speed


def threshold_usable(thresholds,key):
    value = thresholds.get(key+"_effective")
    status = thresholds.get(key+"_status") or {}
    if not isinstance(value,(int,float)) or isinstance(value,bool) or not math.isfinite(value) or value <= 0:
        return False
    if status.get("effective_source") == "manual_override":
        return True
    provenance = thresholds.get(key+"_provenance") or {}
    if status.get("effective_source") != "calculated" or status.get("source_age_status") != "current":
        return False
    if key == "ftp":
        return (provenance.get("threshold_type") == "running_ftp" and
                provenance.get("algorithm_version") == RUNNING_FTP_ALGORITHM_VERSION and
                provenance.get("source_sport") in RUNNING_SPORTS and provenance.get("calculated_value") == value)
    return (provenance.get("threshold_type") == "estimated_lthr" and
            provenance.get("algorithm_version") == ESTIMATED_LTHR_ALGORITHM_VERSION and
            provenance.get("calculated_value") == value)


def _band(records,field,lo,hi):
    selected = [(speed,hr,dt) for speed,hr,dt in records if lo <= (hr if field == "hr" else speed) <= hi]
    seconds = sum(dt for speed,hr,dt in selected)
    return seconds, (sum(speed*dt for speed,hr,dt in selected)/seconds if seconds else None), (sum(hr*dt for speed,hr,dt in selected)/seconds if seconds else None)


def finite(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError,ValueError,OverflowError):
        return None


def prepare_performance(prepared,evidence,options):
    """Qualify each activity; never substitute absent measurements with zero."""
    options = performance_state(options)
    frame = prepared["current"].copy().sort_values(["date","activity_id"])
    thresholds = evidence["thresholds"]
    rows = []
    for _,activity in frame.iterrows():
        aid = int(activity.activity_id)
        facts = evidence["summaries"].get(aid,{})
        context = evidence["contexts"].get(aid,{})
        elapsed = finite(activity.get("total_elapsed_s"))
        observed = facts.get("observed_s") or 0
        denominator = max(elapsed or 0,observed)
        coverage = min(1,facts.get("paired_s",0)/denominator) if denominator else 0
        power_coverage = min(1,facts.get("power_s",0)/denominator) if denominator else 0
        distance_m = finite(activity.get("total_distance_m"))
        ascent_m = finite(activity.get("total_ascent_m"))
        density = ascent_m/distance_m*1000 if distance_m is not None and ascent_m is not None and distance_m>0 and ascent_m>=0 else None
        terrain = "Unknown ascent" if density is None else "Low ascent" if density <= 20 else "Higher ascent"
        family = context.get("family","Unclassified")
        row = dict(activity_id=aid,name=activity.get("activity_name") or f"Activity {aid}",date=activity.date,
            week=activity.week,sport=activity.sport,family=family,terrain=terrain,ascent_density=density,
            group=terrain+" / "+family,duration_min=float(activity.duration*60) if pd.notna(activity.duration) else None,
            distance=float(activity.distance) if pd.notna(activity.distance) else None,
            time_source="elapsed fallback" if activity.elapsed_fallback else "moving" if pd.notna(activity.duration) else "missing",
            paired_coverage=coverage,power_coverage=power_coverage,speed_cv=facts.get("speed_cv"),
            temporal_minutes=denominator/60,mean_hr=facts.get("mean_hr"),mean_speed=facts.get("mean_speed"))
        reason = ("Select one running sport" if prepared["sport"] not in RUNNING_SPORTS else
                  "Confirmed quality/event workout" if context.get("hard") else
                  "Confirmed non-running prescription" if context.get("sport") not in {None,"RUNNING"} else
                  "No usable track samples" if not facts else
                  "Insufficient observed duration" if denominator < options["min_minutes"]*60 else
                  "Paired speed/HR coverage below 90%" if coverage < .9-1e-9 else
                  "Speed variation exceeds 10%" if facts.get("speed_cv") is None or facts["speed_cv"] > .1+1e-9 else
                  "Outside terrain selection" if options["terrain"] != "All terrain" and terrain != options["terrain"] else None)
        row["qualification"] = reason or ("Qualified; workout type unconfirmed" if not context else "Qualified")
        row["steady"] = reason is None
        hr_seconds,hr_speed,_ = _band(facts.get("bands",()),"hr",options["hr_low"],options["hr_high"])
        pace_seconds,_,pace_hr = _band(facts.get("bands",()),"speed",options["speed_low"],options["speed_high"])
        row.update(hr_band_minutes=hr_seconds/60,pace_band_minutes=pace_seconds/60,
            speed_at_hr=hr_speed if row["steady"] and hr_seconds >= 600 else None,
            hr_at_pace=pace_hr if row["steady"] and pace_seconds >= 600 else None,
            running_efficiency=facts["mean_speed"]/facts["mean_hr"] if row["steady"] else None)
        aerobic = threshold_usable(thresholds,"lthr") and facts.get("mean_hr") is not None and facts["mean_hr"] <= .9*thresholds["lthr_effective"]
        durability_reason = reason
        if durability_reason is None and denominator < 2400:
            durability_reason = "At least 40 minutes required"
        if durability_reason is None and not aerobic:
            durability_reason = "Current LTHR and HR at or below 90% LTHR required"
        # Unknown leading/trailing elapsed time could shift the midpoint by half
        # its duration. Subtract that worst-case support loss from either half.
        unobserved_elapsed = max(0,denominator-observed)
        elapsed_half_coverage = max(0,facts.get("min_observed_half_paired_s",0)-unobserved_elapsed/2)/(denominator/2) if denominator else 0
        if durability_reason is None and min(facts.get("half_coverage",0),elapsed_half_coverage) < .9-1e-9:
            durability_reason = "Each elapsed half needs 90% paired support"
        if durability_reason is None and activity.get("metric_provenance_version") != ACTIVITY_METRICS_PROVENANCE_VERSION:
            durability_reason = "Stored temporal metrics stale or absent"
        row["durability_reason"] = durability_reason or "Qualified"
        for field in ("pace_decoupling_pct","hr_drift_pct"):
            row[field] = finite(activity.get(field)) if durability_reason is None else None
        cadence = finite(activity.get("avg_cadence_spm"))
        row["cadence"] = cadence if prepared["sport"] in RUNNING_SPORTS and cadence is not None and 0<cadence<=300 else None
        power_reason = ("Running FTP cannot qualify other sports" if prepared["sport"] not in RUNNING_SPORTS else
                        "Running FTP provenance unavailable or stale" if not threshold_usable(thresholds,"ftp") else
                        "Power coverage below 95%" if power_coverage < .95-1e-9 or denominator < 5 else None)
        row["power_reason"] = power_reason or "Qualified"
        def measured_power(field):
            value = finite(activity.get(field))
            return value if power_reason is None and value is not None and value>=0 else None
        row["avg_power"] = measured_power("avg_power_w")
        row["normalized_power"] = measured_power("normalized_power_w")
        row["variability"] = row["normalized_power"]/row["avg_power"] if row["normalized_power"] is not None and row["avg_power"] is not None and row["avg_power"]>0 else None
        hr = finite(activity.get("avg_hr_bpm"))
        row["power_efficiency"] = row["normalized_power"]/hr if row["normalized_power"] is not None and hr is not None and 35<=hr<=220 and denominator and facts.get("power_hr_s",0)/denominator >= .9 else None
        zones = [finite(activity.get(f"power_zone_{zone}_s")) for zone in range(1,8)]
        zones_known = all(value is not None and value>=0 for value in zones)
        zone_total = sum(zones) if zones_known else 0
        zones_usable = (power_reason is None and activity.get("power_zone_status") == "current" and
                        activity.get("power_ftp_w") == thresholds.get("ftp_effective") and
                        zones_known and denominator*.95 <= zone_total <= denominator*1.02 and zone_total>0)
        row["zone_usable"] = zones_usable
        row["zone_reason"] = (power_reason or
            ("Stored zones stale or absent" if activity.get("power_zone_status") != "current" else
             "Zone threshold differs from current running FTP" if activity.get("power_ftp_w") != thresholds.get("ftp_effective") else
             "All seven zone measurements required" if not zones_known else
             "Zone duration coverage outside 95-102%" if not zones_usable else "Qualified"))
        for zone,value in enumerate(zones,1):
            row[f"power_zone_{zone}_s"] = float(value) if zones_usable else None
        for duration in POWER_PEAK_DURATIONS_SECONDS:
            row[f"peak_{duration}"] = measured_power(f"peak_power_{duration}s_w") if activity.get("metric_provenance_version") == ACTIVITY_METRICS_PROVENANCE_VERSION and facts.get("longest_power_run_s",0)>=duration else None
        rows.append(row)
    result = pd.DataFrame(rows)
    return dict(frame=result,prepared=prepared,options=options,thresholds=thresholds)


def performance_figures(data,velocity_display="Pace",intensity="Hours"):
    frame,prepared,options = data["frame"],data["prepared"],data["options"]
    unit_metres = 1609.344 if prepared["imperial"] else 1000
    cards = []
    rules = f"One running subtype; >= {options['min_minutes']:g} min, paired coverage >=90%, speed CV <=10%, confirmed quality/event workouts excluded, terrain: {options['terrain']}. Signal screening cannot establish unconfirmed workout type."
    def points(fig,subset,field,name,unit,pace=False,axis=None,rolling=True):
        subset = subset.dropna(subset=[field]).sort_values(["date","activity_id"])
        if subset.empty:
            return
        for group,part in subset.groupby("group",sort=True):
            values = part[field]
            color = TRACE_COLORS[sum(bool(t.meta) for t in fig.data) % len(TRACE_COLORS)]
            custom = [[r['name'],int(r['activity_id']),pace_text(value*60) if pace else f"{value:.3f}",r['paired_coverage']*100,r['family'],r['terrain']] for (_,r),value in zip(part.iterrows(),values)]
            fig.add_scatter(x=part.date,y=values,mode="markers",name=name+" / "+group,yaxis=axis,
                marker=dict(color=color,size=7),
                customdata=custom,hovertemplate="%{customdata[0]} (ID %{customdata[1]})<br>%{x|%b %d, %Y}<br>%{customdata[2]} "+unit+"<br>Paired coverage %{customdata[3]:.1f}%<br>%{customdata[4]} / %{customdata[5]}<extra></extra>")
            tag(fig.data[-1],activity_targets(part))
            if rolling:
                series = part.set_index("date")[field]
                fig.add_scatter(x=series.index,y=series.rolling("28D",min_periods=2).median(),mode="lines",name="28-day median / "+name+" / "+group,yaxis=axis,connectgaps=False,hoverinfo="skip",showlegend=False,line=dict(color=color,dash="dash"))
        if pace:
            values = subset[field]
            ticks = sorted({float(values.min()+(values.max()-values.min())*i/4) for i in range(5)})
            fig.update_yaxes(autorange="reversed",tickvals=ticks,ticktext=[pace_text(t*60) for t in ticks])
    for key,title in PERFORMANCE_CATALOG:
        if key not in options["charts"]:
            continue
        fig = go.Figure()
        note = rules
        subset = frame.copy()
        if frame.empty:
            cards.append(dict(key=key,title=title,figure=None,note="No activities match these filters.",rows=[]))
            continue
        if key == "pace_at_hr":
            subset["value"] = unit_metres/subset.speed_at_hr/60 if velocity_display != "Speed" else subset.speed_at_hr*(2.236936292 if prepared["imperial"] else 3.6)
            unit = "min/"+prepared["unit"] if velocity_display != "Speed" else "mph" if prepared["imperial"] else "km/h"
            points(fig,subset,"value","In-band pace/speed",unit,pace=velocity_display != "Speed")
            fig.update_yaxes(title=unit)
            note += f" At least 10 observed minutes in HR band {options['hr_low']:g}-{options['hr_high']:g} bpm; weighted mean speed within that band."
        elif key == "hr_at_pace":
            points(fig,subset,"hr_at_pace","In-band HR","bpm")
            fig.update_yaxes(title="bpm")
            note += f" At least 10 observed minutes at {pace_text(unit_metres/options['speed_high'])}-{pace_text(unit_metres/options['speed_low'])} /{prepared['unit']}; weighted HR within that pace band."
        elif key == "running_efficiency":
            points(fig,subset,"running_efficiency","Speed/HR","m/s per bpm")
            fig.update_yaxes(title="m/s per bpm")
            note += " Paired time-weighted mean speed divided by mean HR. This has different units from power/HR."
        elif key == "cadence":
            points(fig,subset,"cadence","Reported cadence","spm")
            fig.update_yaxes(title="reported spm")
            note = "One running subtype; all valid positive reported cadence values <=300. No doubling or per-leg conversion; upstream conventions can differ. Terrain/workout groups stay separate."
        elif key == "long_runs":
            subset = subset[subset.distance.notna() & subset.duration_min.notna()] if prepared["sport"] in RUNNING_SPORTS else subset.iloc[:0]
            subset = subset.sort_values(["distance","duration_min","date","activity_id"],ascending=False).drop_duplicates("week").sort_values("date")
            points(fig,subset,"distance","Weekly longest distance",prepared["unit"],rolling=False)
            points(fig,subset,"duration_min","Duration","min",axis="y2",rolling=False)
            fig.update_yaxes(title=prepared["unit"])
            fig.update_layout(yaxis2=dict(title="min",overlaying="y",side="right",showgrid=False,automargin=True))
            note = "Longest recorded distance each Monday week for one running subtype; tie-break by duration, date, ID. Both distance and duration required. This is not an inferred workout label. Partial date/current weeks follow the page range; points open exact activities."
        elif key == "durability":
            points(fig,subset,"pace_decoupling_pct","Speed/HR decoupling","%")
            points(fig,subset,"hr_drift_pct","HR drift","%")
            fig.update_yaxes(title="%")
            note += " >=40 min, >=90% paired support in each elapsed half, mean HR <=90% current LTHR (override/current estimate). Current stored temporal metrics only. Signed values retained. Heat, terrain and sensor errors can affect them; no readiness claim."
        elif key in {"power","power_efficiency"}:
            if key == "power":
                points(fig,subset,"avg_power","Imported average power","W")
                points(fig,subset,"normalized_power","Imported normalized power","W")
                fig.update_yaxes(title="W")
            else:
                points(fig,subset,"variability","NP / average power","ratio")
                points(fig,subset,"power_efficiency","NP / average HR","W per bpm",axis="y2")
                fig.update_yaxes(title="NP / average power")
                fig.update_layout(yaxis2=dict(title="W per bpm",overlaying="y",side="right",showgrid=False,automargin=True))
            note = "Running only: usable running FTP (explicit override or current verified calculation), >=95% temporal power coverage. Raw average/normalized power remain distinct; measured zero is retained. Variability needs positive average power; NP/HR needs valid HR and >=90% paired power/HR support. Ratios use these named scalars; groups stay separate."
        elif key == "power_zones":
            subset = subset[subset.zone_usable]
            for zone in range(1,8):
                values = subset[f"power_zone_{zone}_s"]/3600 if intensity == "Hours" else subset[f"power_zone_{zone}_s"]/subset[[f"power_zone_{i}_s" for i in range(1,8)]].sum(axis=1)*100
                fig.add_bar(x=subset.date,y=values,name=f"Zone {zone}",marker_color=POWER_ZONE_COLORS[zone-1],customdata=[[r['name'],int(r['activity_id'])] for _,r in subset.iterrows()],hovertemplate="%{customdata[0]} (ID %{customdata[1]})<br>%{fullData.name}: %{y:.1f}<extra></extra>")
                tag(fig.data[-1],activity_targets(subset))
            fig.update_layout(barmode="stack")
            fig.update_yaxes(title="measured hours" if intensity == "Hours" else "% of measured zone time")
            note = "Running FTP only; >=95% power/zone coverage, all seven zones known, current algorithm and threshold snapshot equal to current FTP. Same-day activities are separate categories; percentages cover measured time. Zone bands: <55%, 55-75%, 75-90%, 90-105%, 105-120%, 120-150%, >=150% FTP."
            fig.update_xaxes(type="category")
            for trace in fig.data:
                trace.x = [f"{r.date.date()} / {int(r.activity_id)}" for _,r in subset.iterrows()]
        elif key == "power_peaks":
            powers,targets,labels = [],[],[]
            for duration in POWER_PEAK_DURATIONS_SECONDS:
                valid = subset.dropna(subset=[f"peak_{duration}"])
                if valid.empty:
                    powers.append(None);targets.append(None);labels.append([None,None,0])
                    continue
                row = valid.loc[valid[f"peak_{duration}"].idxmax()]
                powers.append(row[f"peak_{duration}"]);targets.append(dict(kind="activity",key=int(row.activity_id)))
                labels.append([row['name'],int(row.activity_id),len(valid)])
            fig.add_scatter(x=list(POWER_PEAK_DURATIONS_SECONDS),y=powers,mode="lines+markers",name="Best measured power",connectgaps=False,
                line=dict(color=TRACE_COLORS[0]),marker=dict(color=TRACE_COLORS[0],size=7),
                customdata=labels,hovertemplate="%{x}s: %{y:.1f} W<br>%{customdata[0]} (ID %{customdata[1]})<br>%{customdata[2]} contributing activities<extra></extra>")
            tag(fig.data[0],targets)
            fig.update_xaxes(type="log",title="duration (seconds)",tickvals=list(POWER_PEAK_DURATIONS_SECONDS),ticktext=["5s","30s","1m","5m","20m"])
            fig.update_yaxes(title="W")
            note = "Best current persisted exact-duration means (5/30/60/300/1200s) across this period's workout/terrain groups, with source IDs and contributor counts. Running FTP gate and >=95% observed power coverage apply; unsupported durations stay gaps. Curves are observations, not predicted capability."
        if not any(any(pd.notna(v) for v in trace.y) for trace in fig.data):
            fig = None
        if fig is not None:
            if key not in {"power_zones","power_peaks"}:
                fig.update_xaxes(type="date",range=[prepared["start"].isoformat(),(pd.Timestamp(prepared["end"])+pd.Timedelta(days=1)).date().isoformat()],
                                 autorange=False,tickformat="%b %d<br>%Y",nticks=3,tickangle=0)
            fig.update_layout(template="plotly_white",height=340,margin=dict(l=55,r=35,t=20,b=55),font=dict(size=12),showlegend=False,hovermode="closest",transition=dict(duration=0))
        summary = [dict(duration_s=d,power_w=p,activity_id=t['key'] if t else None,
                        activity=label[0],contributing_activities=label[2])
                   for d,p,t,label in zip(POWER_PEAK_DURATIONS_SECONDS,powers,targets,labels)] if key == "power_peaks" else []
        cards.append(dict(key=key,title=title,figure=fig,note=note,rows=subset,summary=summary))
    return cards
