"""Pure calendar-period preparation and focused overview figures."""
from __future__ import annotations
from datetime import date, timedelta
import math
import pandas as pd
import plotly.graph_objects as go

QUICK_RANGES = ("4 weeks", "12 weeks", "6 months", "1 year", "Year to date", "Custom")
ZONE_COLORS = ("#2455a4", "#168ba5", "#509568", "#bc9235", "#c27646")
SPORT_COLORS = ("#2455a4", "#168ba5", "#509568", "#bc9235", "#9a68aa", "#c27646")
ZONES = [f"zone_{i}_s" for i in range(1,6)]


def period_range(label, today):
    if label in {"4 weeks", "12 weeks"}:
        start = today - timedelta(days=(28 if label == "4 weeks" else 84)-1)
    elif label in {"6 months", "1 year"}:
        start = (pd.Timestamp(today)-pd.DateOffset(months=6 if label == "6 months" else 12)+pd.Timedelta(days=1)).date()
    elif label == "Year to date":
        start = today.replace(month=1,day=1)
    else:
        raise ValueError("Choose a quick range or enter custom dates.")
    return start, today


def comparison_range(start, end):
    if start > end:
        raise ValueError("Start date must be on or before end date.")
    previous_end = start-timedelta(days=1)
    return previous_end-timedelta(days=(end-start).days), previous_end


def numeric(frame, column):
    values = pd.to_numeric(frame.get(column,pd.Series(index=frame.index,dtype=float)),errors="coerce")
    return values.where(values.notna() & values.ge(0) & values.map(lambda x: math.isfinite(x) if pd.notna(x) else False))


def prepare_overview(raw, start, end, sport="All sports", unit_system="Metric", load="tss", today=None):
    today = today or date.today()
    prev_start, prev_end = comparison_range(start,end)
    if end > today:
        raise ValueError("End date cannot be in the future.")
    frame = raw.copy()
    if "activity_id" not in frame:
        frame["activity_id"] = pd.Series(index=frame.index, dtype="Int64")
    source = frame.get("activity_date",frame.get("start_time_utc",pd.Series(index=frame.index,dtype=str)))
    frame["date"] = pd.to_datetime(source,errors="coerce",utc=True).dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["date"])
    if "sport" not in frame: frame["sport"] = "unknown"
    if sport != "All sports": frame = frame[frame.sport == sport].copy()
    imperial = str(unit_system).lower() == "imperial"
    frame["distance"] = numeric(frame,"total_distance_m")/(1609.344 if imperial else 1000)
    moving = numeric(frame,"moving_time_s")
    elapsed = numeric(frame,"total_elapsed_s")
    frame["duration"] = moving.where(moving.notna(),elapsed)/3600
    frame["elapsed_fallback"] = moving.isna() & elapsed.notna()
    frame["elevation"] = numeric(frame,"total_ascent_m")*(3.280839895 if imperial else 1)
    frame["load"] = numeric(frame,load)
    for key in ["avg_speed_mps","avg_power_w","normalized_power_w",*ZONES]:
        frame[key] = numeric(frame,key)
    status = frame.get("hr_zone_status",pd.Series("current",index=frame.index))
    frame["zone_usable"] = frame[ZONES].notna().all(axis=1) & frame[ZONES].sum(axis=1).gt(0) & status.eq("current")
    frame.loc[~frame.zone_usable,ZONES] = float("nan")
    frame["week"] = frame.date.dt.to_period("W-SUN").dt.start_time
    current = frame[frame.date.between(pd.Timestamp(start),pd.Timestamp(end))].copy()
    previous = frame[frame.date.between(pd.Timestamp(prev_start),pd.Timestamp(prev_end))].copy()
    weeks = pd.date_range(pd.Timestamp(start)-pd.Timedelta(days=start.weekday()),pd.Timestamp(end)-pd.Timedelta(days=end.weekday()),freq="7D")
    weekly = pd.DataFrame(index=weeks)
    weekly.index.name = "week"
    weekly["activities"] = current.groupby("week").size().reindex(weeks,fill_value=0)
    for key in ["duration","distance","elevation","load",*ZONES]:
        grouped = current.groupby("week")[key]
        weekly[key] = grouped.sum(min_count=1).reindex(weeks)
        weekly[key+"_count"] = grouped.count().reindex(weeks,fill_value=0)
        weekly.loc[weekly.activities.eq(0),key] = 0
    weekly["partial"] = [(w.date() < start or (w+pd.Timedelta(days=6)).date() > end or (w+pd.Timedelta(days=6)).date() > today) for w in weeks]
    weekly["week_label"] = [w.strftime("%b %d")+("*" if p else "") for w,p in zip(weeks,weekly.partial)]
    complete_load = weekly.load.where(weekly.load_count.eq(weekly.activities) & ~weekly.partial)
    weekly["load_rolling"] = complete_load.rolling(4,min_periods=4).mean()
    summary = {}
    for key in ["duration","distance","load","elevation"]:
        value = current[key].sum(min_count=1) if len(current) else 0.0
        old = previous[key].sum(min_count=1) if len(previous) else 0.0
        count, old_count = int(current[key].count()), int(previous[key].count())
        minimum = {"duration":1/60,"distance":.01,"load":1,"elevation":1}[key]
        change = (value/old-1)*100 if count == len(current) and old_count == len(previous) and pd.notna(old) and old >= minimum and pd.notna(value) else None
        summary[key] = dict(value=float(value) if pd.notna(value) else None,previous=float(old) if pd.notna(old) else None,
                            count=count,total=len(current),previous_count=old_count,previous_total=len(previous),change=change)
    family = ("velocity" if any(x in sport.lower() for x in ("run","walk","hik")) else "power" if any(x in sport.lower() for x in ("cycl","bik")) else "duration")
    return dict(current=current, previous=previous,weekly=weekly,summary=summary,start=start,end=end,
                previous_start=prev_start,previous_end=prev_end,sport=sport,load=load.upper(),family=family,
                unit="mi" if imperial else "km",elevation_unit="ft" if imperial else "m",imperial=imperial,
                elapsed_fallback=int(current.elapsed_fallback.sum()),previous_elapsed_fallback=int(previous.elapsed_fallback.sum()),zone_count=int(current.zone_usable.sum()),
                stale_zones=int(current.get("hr_zone_status",pd.Series(index=current.index,dtype=str)).eq("stale").sum()))


def pace_text(seconds):
    seconds = round(seconds)
    return f"{seconds//60}:{seconds%60:02d}"


def overview_figures(data, volume="Time", intensity="Hours", velocity_display="Pace"):
    frame, weekly = data["current"],data["weekly"]
    cards = []
    def finish(title, fig, note):
        if fig is not None:
            fig.update_layout(title=None,template="plotly_white",height=320,margin=dict(l=45,r=15,t=20,b=65),
                              legend=dict(orientation="h",y=-.25,x=0,traceorder="normal"),font=dict(size=12),barmode="stack",hovermode="closest")
            fig.update_xaxes(title=None,automargin=True)
        cards.append(dict(title=title,figure=fig,note=note))
    distance_allowed = data["sport"] != "All sports" and data["family"] in {"velocity","power"}
    field = "distance" if volume == "Distance" and distance_allowed else "duration"
    unit = data["unit"] if field == "distance" else "hours"
    fig = go.Figure()
    sports = sorted(frame.sport.unique())
    for i,sport in enumerate(sports):
        subset = frame[frame.sport == sport]
        sums = subset.groupby("week")[field].sum(min_count=1).reindex(weekly.index)
        counts = subset.groupby("week").size().reindex(weekly.index,fill_value=0)
        sums = sums.where(counts.gt(0),0)
        fig.add_bar(x=weekly.week_label,y=sums,name=sport,marker_color=SPORT_COLORS[i%len(SPORT_COLORS)],
                    hovertemplate=f"%{{x}}<br>{sport}: %{{y:.2f}} {unit}<extra></extra>")
    if not sports:
        fig.add_bar(x=weekly.week_label,y=[0]*len(weekly),name="No activities",marker_color="#2455a4")
    fig.update_yaxes(title=unit)
    finish("Weekly training volume",fig,f"{field.title()} ({unit}); Monday weeks; partial totals exclude missing measurements. * marks partial range boundaries/current week. {data['elapsed_fallback']} elapsed-time fallbacks. {data['summary'][field]['count']} of {len(frame)} activities measured.")
    count,total = data["summary"]["load"]["count"],len(frame)
    fig = None
    if count:
        fig = go.Figure()
        fig.add_bar(x=weekly.week_label,y=weekly.load,name=data["load"],marker_color="#2455a4",
                    customdata=weekly[["load_count","activities"]].values,
                    hovertemplate="%{x}<br>Observed load: %{y:.1f}<br>Coverage: %{customdata[0]} / %{customdata[1]}<extra></extra>")
        fig.add_scatter(x=weekly.week_label,y=weekly.load_rolling,name="4-week mean",mode="lines",line_color="#c27646",connectgaps=False)
        fig.update_yaxes(title=data["load"])
    finish("Weekly training load",fig,f"{data['load']} available for {count} of {total} activities. Partial totals exclude missing values; all-missing weeks are gaps. Rolling mean requires four complete, fully measured weeks.")
    fig = None
    if data["zone_count"]:
        fig = go.Figure()
        totals = weekly[ZONES].sum(axis=1,min_count=1)
        for i,key in enumerate(ZONES):
            values = weekly[key]/3600 if intensity == "Hours" else weekly[key]/totals.where(totals.gt(0))*100
            fig.add_bar(x=weekly.week_label,y=values,name=f"Zone {i+1}",marker_color=ZONE_COLORS[i],hovertemplate="%{x}<br>%{fullData.name}: %{y:.1f}<extra></extra>")
        fig.update_yaxes(title="hours" if intensity == "Hours" else "% of measured zone time")
    finish("Intensity distribution",fig,f"Stored HR zones passing existing provenance checks; {data['zone_count']} of {total} activities usable, {data['stale_zones']} stale. Hours/percentages cover measured zone time only. Missing threshold, stale metrics, or absent HR data can prevent analysis.")
    fig = None
    note = "Select one sport for comparable performance analysis."
    if data["sport"] != "All sports":
        subset = frame.copy()
        family = data["family"]
        if family == "velocity":
            subset = subset[subset.avg_speed_mps.gt(0)].copy()
            is_pace = velocity_display != "Speed"
            subset["value"] = (1609.344 if data["imperial"] else 1000)/subset.avg_speed_mps if is_pace else subset.avg_speed_mps*(2.236936292 if data["imperial"] else 3.6)
            ylabel = f"min/{data['unit']}" if is_pace else ("mph" if data["imperial"] else "km/h")
        elif family == "power":
            subset = subset[subset.avg_power_w.gt(0)].copy()
            subset["value"],ylabel = subset.avg_power_w,"W (average power)"
            is_pace = False
        else:
            subset = subset[subset.duration.notna()].copy()
            subset["value"],ylabel = subset.duration*60,"minutes per activity"
            is_pace = False
        note = f"{data['sport']}: {len(subset)} of {total} activities with valid {ylabel}; points and 28-day rolling median. Darker points indicate longer duration. Indoor/outdoor types are kept separate by the sport filter."
        if not subset.empty:
            subset = subset.sort_values("date")
            values = subset.value
            text = values.map(pace_text) if is_pace else values.map(lambda v:f"{v:.1f}")
            fig = go.Figure()
            fig.add_scatter(x=subset.date,y=values,mode="markers",name="Activities",marker=dict(size=8,color="#2455a4",opacity=(.4+.5*(subset.duration.fillna(0)/max(1,subset.duration.max() if subset.duration.notna().any() else 1)).clip(0,1))),
                            customdata=text,hovertemplate=f"%{{x|%b %d, %Y}}<br>%{{customdata}} {ylabel}<extra></extra>")
            series = subset.set_index("date").value
            rolling = series.rolling("28D",min_periods=2).median()
            fig.add_scatter(x=series.index,y=rolling,mode="lines",name="28-day median",line_color="#c27646",
                            customdata=[pace_text(v) if is_pace and pd.notna(v) else f"{v:.1f}" if pd.notna(v) else "Unavailable" for v in rolling],
                            hovertemplate=f"%{{x|%b %d, %Y}}<br>%{{customdata}} {ylabel}<extra></extra>")
            if family == "power" and subset.normalized_power_w.gt(0).any():
                fig.add_scatter(x=subset.date,y=subset.normalized_power_w.where(subset.normalized_power_w.gt(0)),mode="markers",name="Normalized power",marker_symbol="diamond")
            if family == "duration":
                frequency = frame.groupby("date").size()
                fig.add_scatter(x=frequency.index,y=frequency,mode="markers",name="Daily activity count",yaxis="y2",marker_symbol="diamond")
                fig.update_layout(yaxis2=dict(title="Activities/day",overlaying="y",side="right",showgrid=False,dtick=1,automargin=True,rangemode="tozero"))
                note += " Daily activity count uses the right axis."
            if is_pace:
                low,high = values.min(),values.max()
                ticks = [low+(high-low)*i/4 for i in range(5)] if high>low else [low]
                fig.update_yaxes(autorange="reversed",tickvals=ticks,ticktext=[pace_text(v) for v in ticks],title=ylabel)
            else: fig.update_yaxes(title=ylabel)
    finish("Performance trend",fig,note)
    return cards
