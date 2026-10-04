"""Read-only planned/completed charts and accessible evidence drill-down."""
from datetime import date, timedelta
import json
import pandas as pd
from nicegui import ui

from garmin_data_hub.analytics.chart_plan_comparison import comparison_figures
from garmin_data_hub.analytics.chart_explorer import resolve_click


def _table(rows, fields, key):
    table = ui.table(columns=[dict(name=k,field=k,label=label,align="left",sortable=True) for k,label in fields],
        rows=rows, row_key=key, pagination=10).classes("w-full")
    table.add_slot("body-cell-activity_id", """<q-td :props="props"><a v-if="props.row.activity_id" :href="'/activities?activity_id=' + props.row.activity_id" class="text-primary underline">Open activity {{ props.row.activity_id }}</a></q-td>""")
    return table


def render_plan_comparison(data, volume):
    for row in data["workouts"]:
        row["identity"] = row["revision_id"]+":"+row["workout_id"]
    ui.label("Plan comparison").classes("text-xl font-semibold")
    with ui.row().classes("gap-3 flex-wrap"):
        ui.link("Review active plan", "/plan")
        ui.link("Review seasons", "/seasons")
    ui.label("Planned sessions follow their scheduled date; completed activity totals follow Garmin's recorded calendar date. Confirmed moved-date or different-sport matches are labeled substitutions. Unmatched activities still contribute to actual totals.").classes("text-sm")
    with ui.expansion("Comparison definitions and coverage",icon="info").classes("w-full"):
        ui.label("Comparisons use the current schedule, not a reconstruction of earlier intentions. Import completeness is unknown. A missing match does not prove a skipped workout. Completion evidence does not measure intensity adherence.").classes("text-sm")
        ui.label("RUNNING plan targets include running, trail running, and indoor running. A single running subtype filters actual activities but cannot split the plan's running targets. Strength includes strength/strength_training; mobility includes mobility/yoga/pilates.").classes("text-sm")
        ui.label("Plan weeks start on the selected plan's first scheduled date. Multiple active plans are additive; overlapping plans may both prescribe sessions. Refresh data rereads current plans and matches.").classes("text-sm")
        ui.label("Known planned totals include labeled estimates only when every segment has that measure. Coverage columns count measured sessions/activities. Variance and volume percentages require full measurement and plan coverage of the selected days; partial week values are to date. Blank percentages have no usable denominator.").classes("text-sm")
    if data["unmanaged"]:
        ui.label(f"{data['unmanaged']} legacy/unmanaged schedule rows excluded from canonical comparison. Review or convert them in Plan.").classes("text-warning")
    if not data["plans"]:
        ui.label("No active plan is available for comparison. Generate or convert a plan to compare its schedule.").classes("text-grey-7")
    workout_fields = [("activity_id","Activity details"),("date","Scheduled date"),("title","Workout"),("sport","Planned sport"),("status","Completion evidence"),
        ("planned_minutes","Planned min"),("duration_estimated","Time estimated"),("planned_distance",f"Planned {data['unit']}"),("distance_estimated","Distance estimated"),
        ("activity_date","Activity date"),("activity_sport","Actual sport"),("activity_scope","Activity filter scope"),("match_reason","Match review reason"),("candidates","Candidate matches"),("plan_id","Plan")]
    source_fields = [("activity_id","Activity details"),("date","Activity date"),("sport","Sport"),("relation","Match relation"),("planned_date","Scheduled date"),("workout","Matched workout")]
    sources = [dict(row,activity_id=row["id"]) for row in data["sources"]]
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-5xl min-w-0"):
        heading = ui.label().classes("text-lg font-semibold")
        body = ui.column().classes("w-full min-w-0")
        ui.button("Close plan week",on_click=dialog.close).props("flat")
    def inspect(key):
        first = date.fromisoformat(key)
        last = first+timedelta(days=6)
        heading.text = f"Plan and activity evidence · {key}"
        body.clear()
        with body:
            ui.link("Review active plan", "/plan")
            scheduled = [w for w in data["workouts"] if w["week"] == key]
            ui.label(f"{len(scheduled)} scheduled rows, including rest.")
            if scheduled:
                _table(scheduled,workout_fields,"identity")
            else:
                ui.label("No scheduled workouts in this selection.")
            activities = [r for r in sources if first <= date.fromisoformat(r["date"]) <= last]
            ui.label(f"{len(activities)} completed activities in this selection.")
            if activities:
                _table(activities,source_fields,"activity_id")
        dialog.open()
    with ui.element("div").classes("grid grid-cols-1 xl:grid-cols-2 w-full gap-4"):
        for index, card in enumerate(comparison_figures(data,volume)):
            with ui.card().classes("gdh-card w-full min-w-0"+(" xl:col-span-2" if index == 0 else "")):
                ui.label(card["title"]).classes("text-lg font-semibold")
                ui.label(card["note"]).classes("text-xs text-grey-7")
                figure = card["figure"]
                if figure is None:
                    ui.label(f"No measured {data['load']} is available for the selected activities.").classes("text-sm text-grey-7")
                    continue
                plot = ui.plotly(figure).classes("w-full h-[22rem]")
                def click(event, figure=figure):
                    target = resolve_click(figure,event.args)
                    if target:
                        inspect(target["key"])
                plot.on("plotly_click",click,js_handler="(event) => { const p = event.points?.[0]; if (p) emit({curveNumber:p.curveNumber, pointNumber:p.pointNumber}); }")
    with ui.row().classes("w-full items-end gap-3 flex-wrap"):
        week = ui.select({r["week"]:r["label"] for r in data["weekly"]},value=data["weekly"][-1]["week"],label="Inspect plan week").props("outlined").classes("w-full sm:w-72")
        ui.button("Show plan week evidence",on_click=lambda:inspect(week.value))
    fields = [("week","Week starting"),("partial","Partial week"),("plan_days","Plan coverage days"),("planned_sessions","Planned sessions"),
        ("rest_days","Rest rows"),("due_sessions","Due sessions"),("completed_due","Completed due sessions"),("completion_pct","Due completion %"),
        ("confirmed","Confirmed activities"),("marked_complete","Marked complete without activity"),
        ("substitutions","Substitutions"),("candidates","Awaiting review"),("unmatched","Unmatched activities")]
    for metric, label in (("duration","hours"),("distance",data["unit"]),("load",data["load"])):
        fields += [("planned_"+metric,f"Planned {label}"),("actual_"+metric,f"Completed {label}"),(metric+"_variance",f"Variance {label}"),
                   (metric+"_pct",f"Volume completion % ({label})"),("planned_"+metric+"_count",f"Measured plan ({label})"),("actual_"+metric+"_count",f"Measured actual ({label})")]
    # Normalize NaN to JSON null before passing the rows to the browser.
    frame = pd.DataFrame(data["weekly"])
    rows = json.loads(frame.to_json(orient="records"))
    with ui.expansion("Plan variance and coverage table",icon="table_chart").classes("w-full"):
        _table(rows,fields,"week")
        export = frame[[key for key,_ in fields]].rename(columns=dict(fields))
        ui.button("Download plan comparison CSV",on_click=lambda:ui.download(export.to_csv(index=False).encode("utf-8"),"plan-comparison.csv"))
    with ui.expansion("Scheduled workout evidence",icon="list").classes("w-full"):
        _table(data["workouts"],workout_fields,"identity")
    with ui.expansion("Completed activity matching",icon="list").classes("w-full"):
        _table(sources,source_fields,"activity_id")
