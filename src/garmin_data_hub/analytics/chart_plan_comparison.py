"""Weekly plan comparison, coverage and explicit completion evidence."""
from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import plotly.graph_objects as go

from garmin_data_hub.services.thresholds import RUNNING_SPORTS


def plan_sport_matches(planned, actual):
    actual = str(actual or "").lower()
    return (actual in RUNNING_SPORTS if planned == "RUNNING" else
            actual in {"strength", "strength_training"} if planned == "STRENGTH" else
            actual in {"mobility", "yoga", "pilates"} if planned == "MOBILITY" else False)


def prepare_plan_comparison(snapshot, prepared, plan_id="All active plans", alignment="Calendar weeks", today=None):
    today = today or date.today()
    start, end = prepared["start"], prepared["end"]
    plans = [p for p in snapshot["plans"] if plan_id == "All active plans" or p["id"] == plan_id]
    selected_ids = {p["id"] for p in plans}
    all_workouts = [w for w in snapshot["workouts"] if w["plan_id"] in selected_ids]
    if alignment == "Plan weeks" and len(plans) != 1:
        raise ValueError("Select one active plan to use plan weeks.")
    anchor = date.fromisoformat(plans[0]["anchor"]) if alignment == "Plan weeks" else start-timedelta(days=start.weekday())
    def bucket(day):
        return anchor + timedelta(days=((day-anchor).days//7)*7)
    def shown(w):
        return prepared["sport"] == "All sports" or plan_sport_matches(w["sport"], prepared["sport"])
    windows = [(min(date.fromisoformat(w["date"]) for w in all_workouts if w["plan_id"] == p["id"]),
                max(date.fromisoformat(w["date"]) for w in all_workouts if w["plan_id"] == p["id"])) for p in plans]
    current = prepared["current"].copy()
    current["comparison_week"] = current.date.dt.date.map(bucket)
    by_activity = {int(w["activity_id"]): w for w in snapshot["workouts"] if w["activity_id"] is not None}
    candidate_ids = {int(aid) for w in snapshot["workouts"] for aid in w.get("candidate_activity_ids", [])}
    sources = []
    for _, activity in current.iterrows():
        aid = int(activity.activity_id)
        match = by_activity.get(aid)
        relation = "Candidate match; unconfirmed" if aid in candidate_ids else "Unmatched activity"
        if match:
            relation = "Confirmed match" if match["plan_id"] in selected_ids else "Matched to another plan"
            if not start <= date.fromisoformat(match["date"]) <= end:
                relation += " outside date range"
        sources.append(dict(id=aid, date=activity.date.date().isoformat(), sport=activity.sport,
                            relation=relation, planned_date=match["date"] if match else None,
                            workout=match["title"] if match else None))
    workouts = []
    for w in all_workouts:
        day = date.fromisoformat(w["date"])
        if not start <= day <= end or not shown(w):
            continue
        row = dict(w)
        activity_day = date.fromisoformat(w["activity_date"]) if w["activity_date"] else None
        # A valid recorded activity outside the filter range still proves completion.
        confirmed = w["activity_id"] is not None and not w["missing_activity"] and (activity_day is None or activity_day <= today)
        substitution = confirmed and ((activity_day is not None and activity_day != day) or
                                      (w["activity_sport"] is not None and not plan_sport_matches(w["sport"], w["activity_sport"])))
        row.update(week=bucket(day).isoformat(), due=day < today and not w["rest"],
                   completed=bool(confirmed or w["explicitly_completed"]) and not w["rest"], substitution=bool(substitution))
        row["status"] = ("Rest" if w["rest"] else "Confirmed substitution" if substitution else "Confirmed activity" if confirmed else
                         "Marked complete; no activity match" if w["explicitly_completed"] else
                         "Matched activity unavailable" if w["missing_activity"] else
                         "Candidate awaiting review" if w["candidates"] else
                         "No confirmed completion" if row["due"] else "Not yet due")
        row["activity_scope"] = ("Recorded date unavailable" if confirmed and activity_day is None else
             "Outside date/sport filters" if confirmed and (not start <= activity_day <= end or
             (prepared["sport"] != "All sports" and w["activity_sport"] != prepared["sport"])) else "Within filters" if confirmed else None)
        row["planned_minutes"] = w["duration_s"]/60 if w["duration_s"] is not None else None
        row["planned_distance"] = w["distance_m"]/(1609.344 if prepared["imperial"] else 1000) if w["distance_m"] is not None else None
        workouts.append(row)
    weeks = []
    week = bucket(start)
    while week <= end:
        last = week+timedelta(days=6)
        first_shown, last_shown = max(start,week), min(end,last)
        coverage = sum(any(a <= first_shown+timedelta(days=i) <= b for a,b in windows)
                       for i in range((last_shown-first_shown).days+1))
        group = [w for w in workouts if w["week"] == week.isoformat()]
        training = [w for w in group if not w["rest"]]
        activities = current[current.comparison_week.eq(week)]
        due = [w for w in training if w["due"]]
        partial = first_shown != week or last_shown != last or last >= today or coverage != 7
        label = (f"Plan week {(week-anchor).days//7+1}" if week >= anchor else f"Before plan · {week}") if alignment == "Plan weeks" else week.isoformat()
        row = dict(week=week.isoformat(), label=label+("*" if partial else ""),
            partial=partial, plan_days=coverage, planned_sessions=len(training), rest_days=len(group)-len(training),
            due_sessions=len(due), completed_due=sum(w["completed"] for w in due),
            confirmed=sum(w["status"] in {"Confirmed activity", "Confirmed substitution"} for w in training),
            marked_complete=sum(w["status"] == "Marked complete; no activity match" for w in training),
            substitutions=sum(w["substitution"] for w in training), candidates=sum(w["candidates"] > 0 and not w["completed"] for w in training),
            activities=len(activities), unmatched=sum(int(aid) not in by_activity for aid in activities.activity_id))
        row["completion_pct"] = 100*row["completed_due"]/len(due) if due else None
        for field, source, divisor in (("duration","duration_s",3600), ("distance","distance_m",1609.344 if prepared["imperial"] else 1000), ("load","load",1)):
            values = [w[source]/divisor for w in training if w[source] is not None]
            row["planned_"+field] = sum(values) if values else 0 if not training and coverage else None
            row["planned_"+field+"_count"] = len(values)
            row["estimated_"+field+"_count"] = sum(w.get(field+"_estimated",False) for w in training)
            actual_values = activities[field].dropna()
            row["actual_"+field] = float(actual_values.sum()) if len(actual_values) else 0 if activities.empty else None
            row["actual_"+field+"_count"] = len(actual_values)
            comparable = coverage == (last_shown-first_shown).days+1 and len(values) == len(training) and len(actual_values) == len(activities)
            row[field+"_variance"] = row["actual_"+field]-row["planned_"+field] if comparable else None
            baseline = row["planned_"+field]
            row[field+"_pct"] = row["actual_"+field]/baseline*100 if comparable and baseline is not None and baseline > 0 else None
        weeks.append(row)
        week += timedelta(days=7)
    return dict(weekly=weeks, workouts=workouts, sources=sources, plans=plans, alignment=alignment,
                unit=prepared["unit"], load=prepared["load"], unmanaged=snapshot["unmanaged"])


def comparison_figures(data, volume="Time"):
    rows = data["weekly"]
    cards = []
    field = "distance" if volume == "Distance" else "duration"
    for metric, title, unit in ((field,"Weekly planned versus completed",data["unit"] if field == "distance" else "hours"),
                               ("load","Weekly planned versus completed load",data["load"])):
        fig = go.Figure()
        for prefix, name, color in (("planned", "Planned", "#c27646"), ("actual", "Completed activities", "#2455a4")):
            fig.add_bar(x=[r["label"] for r in rows], y=[r[prefix+"_"+metric] for r in rows], name=name, marker_color=color,
                customdata=[[r["week"],r[prefix+"_"+metric+"_count"],r["planned_sessions"] if prefix == "planned" else r["activities"]] for r in rows],
                meta=dict(targets=[dict(kind="week", key=r["week"]) for r in rows]),
                hovertemplate="%{customdata[0]}<br>%{fullData.name}: %{y:.2f} "+unit+"<br>Measured: %{customdata[1]} / %{customdata[2]}<extra></extra>")
        fig.update_layout(template="plotly_white", barmode="group", height=320, margin=dict(l=45,r=15,t=20,b=85), legend=dict(orientation="h",y=-.3))
        fig.update_yaxes(title=unit)
        fig.update_xaxes(type="category",automargin=True)
        coverage = f"{sum(r['actual_'+metric+'_count'] for r in rows)} of {sum(r['activities'] for r in rows)} completed activities measured."
        planned_coverage = f"Planned measures: {sum(r['planned_'+metric+'_count'] for r in rows)} / {sum(r['planned_sessions'] for r in rows)} sessions ({sum(r['estimated_'+metric+'_count'] for r in rows)} with estimates). "
        if metric == "load" and not any(r["actual_load_count"] for r in rows):
            fig = None
        cards.append(dict(title=title, figure=fig, note=("Partial totals show known measurements; blanks are unavailable. * marks partial date/current/plan coverage. "+planned_coverage if metric != "load" else f"Canonical plans do not currently prescribe {data['load']}; planned load and load variance are unavailable for training sessions. ")+coverage))
    fig = go.Figure()
    fig.add_bar(x=[r["label"] for r in rows], y=[r["completion_pct"] for r in rows], name="Due-session completion",
        customdata=[[r["week"],r["completed_due"],r["due_sessions"]] for r in rows],
        meta=dict(targets=[dict(kind="week",key=r["week"]) for r in rows]),
        hovertemplate="%{customdata[0]}<br>%{y:.1f}%<br>%{customdata[1]} / %{customdata[2]} due sessions<extra></extra>")
    fig.update_layout(template="plotly_white",height=320,margin=dict(l=45,r=15,t=20,b=65))
    fig.update_yaxes(title="% of due sessions",range=[0,100])
    fig.update_xaxes(type="category",automargin=True)
    cards.append(dict(title="Matched workout completion",figure=fig,note="Due means before today. Rest and today's sessions are excluded. Confirmed matches and explicit completion count; candidates do not. This measures completion evidence, not prescribed intensity compliance."))
    return cards
