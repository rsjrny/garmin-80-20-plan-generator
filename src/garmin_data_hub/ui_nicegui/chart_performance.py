"""Advanced Charts controls, qualified figures and accessible metric exports."""
import json
import pandas as pd
from nicegui import ui

from garmin_data_hub.analytics.chart_performance import PERFORMANCE_CATALOG, TERRAINS, performance_state, parse_pace, performance_figures
from garmin_data_hub.analytics.chart_overview import pace_text
from garmin_data_hub.analytics.chart_explorer import resolve_click


class PerformanceControls:
    def __init__(self,saved,unit_system,on_change):
        self.saved = performance_state(saved)
        self.metres = 1609.344 if unit_system == "Imperial" else 1000
        unit = "mi" if unit_system == "Imperial" else "km"
        self.initial_fast = pace_text(self.metres/self.saved["speed_high"])
        self.initial_slow = pace_text(self.metres/self.saved["speed_low"])
        with ui.column().classes("w-full min-w-0") as self.container:
            self.catalog = ui.select(dict(PERFORMANCE_CATALOG),value=self.saved["charts"],multiple=True,label="Performance charts").props("outlined use-chips").classes("w-full sm:max-w-3xl")
            with ui.row().classes("w-full items-end gap-3 flex-wrap"):
                self.hr_low = ui.number("HR band low (bpm)",value=self.saved["hr_low"],min=35,max=220).props("outlined").classes("w-full sm:w-48")
                self.hr_high = ui.number("HR band high (bpm)",value=self.saved["hr_high"],min=35,max=220).props("outlined").classes("w-full sm:w-48")
                self.fast = ui.input(f"Fast pace (min/{unit})",value=self.initial_fast).props("outlined").classes("w-full sm:w-48")
                self.slow = ui.input(f"Slow pace (min/{unit})",value=self.initial_slow).props("outlined").classes("w-full sm:w-48")
                self.minimum = ui.number("Minimum steady minutes",value=self.saved["min_minutes"],min=10,max=240).props("outlined").classes("w-full sm:w-48")
                self.terrain = ui.select(list(TERRAINS),value=self.saved["terrain"],label="Terrain selection").props("outlined").classes("w-full sm:w-56")
            ui.label("Pace bands use minutes:seconds. Low ascent means <=20 m per km (about 106 ft per mile); ascent density is a filter, not grade-adjusted pace.").classes("text-xs text-grey-7")
        for control in (self.catalog,self.hr_low,self.hr_high,self.fast,self.slow,self.minimum,self.terrain):
            control.on_value_change(on_change)

    def options(self):
        try:
            lo,hi = float(self.hr_low.value),float(self.hr_high.value)
            minimum = float(self.minimum.value)
        except (ValueError,TypeError):
            raise ValueError("Enter both heart-rate bounds and minimum duration.") from None
        if not 35 <= lo < hi <= 220:
            raise ValueError("HR band requires 35 <= low < high <= 220 bpm.")
        if not 10 <= minimum <= 240:
            raise ValueError("Minimum steady duration must be 10-240 minutes.")
        high = self.saved["speed_high"] if self.fast.value == self.initial_fast else parse_pace(self.fast.value,self.metres)
        low = self.saved["speed_low"] if self.slow.value == self.initial_slow else parse_pace(self.slow.value,self.metres)
        if low >= high:
            raise ValueError("Fast pace must be faster than slow pace.")
        return dict(hr_low=lo,hr_high=hi,speed_low=low,speed_high=high,min_minutes=minimum,
                    terrain=self.terrain.value,charts=list(self.catalog.value or []))


def render_performance(data,velocity_display,intensity):
    prepared,frame,thresholds = data["prepared"],data["frame"],data["thresholds"]
    ui.label("Performance and durability").classes("text-xl font-semibold")
    ui.label("Compare one running subtype at a time. Signal screening, terrain and confirmed workout groups keep unlike sessions separate. A trend alone does not establish improvement or its cause.").classes("text-sm")
    with ui.expansion("Qualification and threshold sources",icon="info").classes("w-full"):
        ui.label("Temporal support follows the existing elapsed-time engine: duplicate timestamps are resolved by sequence, gaps over 30 seconds are unsupported, and the last sample has no invented duration. Coverage uses the larger of recorded elapsed time and observed track window.").classes("text-sm")
        ui.label("Unclassified means no confirmed original workout identity; low speed variability cannot prove a session was easy or exclude every interval pattern. Confirmed quality/event workouts cannot enter comparable-effort or durability views.").classes("text-sm")
        ui.label("Duration, coverage, variability, ascent and aerobic limits are screening filters for this view, not validated coaching cutoffs.").classes("text-sm")
        for key,name,unit in (("lthr","LTHR","bpm"),("ftp","Running FTP","W")):
            status = thresholds.get(key+"_status") or {}
            provenance = thresholds.get(key+"_provenance") or {}
            ui.label(f"{name}: {thresholds.get(key+'_effective') or 'unavailable'} {unit}; source {status.get('effective_source','unavailable')}; calculated evidence age {status.get('source_age_status','unknown')}; algorithm {provenance.get('algorithm_version','unknown')}; evidence {provenance.get('evidence_at_utc') or 'unknown'}.").classes("text-sm")
        ui.label("A manual override is an explicit user input with unverified evidence age. The app stores running FTP; it cannot qualify cycling power. Running speed/HR and power/HR efficiency have distinct definitions and units. Metrics are not refreshed on this page.").classes("text-sm")
    if prepared["sport"] == "All sports":
        ui.label("Select one running sport for these qualified analyses.").classes("text-grey-7")
    ui.label(f"{len(frame)} selected activities. Blank metrics are unavailable or excluded; the table records qualification reasons. Refresh data rereads tracks, matches and threshold provenance.").classes("text-xs text-grey-7")
    if not data["options"]["charts"]:
        ui.label("Choose charts from Performance charts.")
    with ui.element("div").classes("grid grid-cols-1 xl:grid-cols-2 w-full gap-4"):
        for index,card in enumerate(performance_figures(data,velocity_display,intensity)):
            with ui.card().classes("gdh-card w-full min-w-0"+(" xl:col-span-2" if index == 0 else "")):
                ui.label(card["title"]).classes("text-lg font-semibold")
                ui.label(card["note"]).classes("text-xs text-grey-7")
                figure = card["figure"]
                if figure is None:
                    ui.label("No qualified measurements available for these filters.").classes("text-sm text-grey-7")
                    continue
                targets = {t['key'] for trace in figure.data if trace.meta for t in trace.meta.get('targets',[]) if t and t.get('kind') == 'activity'}
                ui.label(f"{len(targets)} of {len(frame)} activities supply plotted measurements. Dashed lines are 28-day within-group medians; the solid peak-power curve connects observed best values.").classes("text-xs")
                plot = ui.plotly(figure).classes("w-full h-[24rem]")
                def click(event,figure=figure):
                    target = resolve_click(figure,event.args)
                    if target and target["kind"] == "activity":
                        ui.navigate.to(f"/activities?activity_id={target['key']}")
                plot.on("plotly_click",click,js_handler="(event) => { const p = event.points?.[0]; if (p) emit({curveNumber:p.curveNumber, pointNumber:p.pointNumber}); }")
                initial_xrange = list(figure.layout.xaxis.range) if figure.layout.xaxis.range else None
                def reset(plot=plot,figure=figure,initial_xrange=initial_xrange):
                    figure.update_layout(xaxis=dict(autorange=initial_xrange is None,range=initial_xrange),yaxis=dict(autorange="reversed" if figure.layout.yaxis.autorange == "reversed" else True))
                    if "yaxis2" in figure.layout:
                        figure.update_layout(yaxis2=dict(autorange=True))
                    plot.update()
                with ui.row().classes("w-full gap-x-4 gap-y-1 flex-wrap text-xs"):
                    for trace in figure.data:
                        if trace.showlegend is False:
                            continue
                        color = trace.marker.color or trace.line.color or "#2455a4"
                        with ui.row().classes("items-start gap-1 min-w-0"):
                            ui.label("●").style(f"color: {color}")
                            ui.label(trace.name).classes("whitespace-normal break-words min-w-0").style("flex: 1")
                ui.button("Reset zoom",on_click=reset).props("flat dense")
                if card.get("summary"):
                    with ui.expansion("Peak-power source table",icon="table_chart").classes("w-full"):
                        columns = [("duration_s","Duration (seconds)"),("power_w","Best observed power (W)"),
                                   ("activity_id","Source activity ID"),("activity","Activity"),
                                   ("contributing_activities","Contributing activities")]
                        source = ui.table(columns=[dict(name=k,field=k,label=label,align="left") for k,label in columns],
                                          rows=card["summary"],row_key="duration_s").classes("w-full")
                        source.add_slot("body-cell-activity_id","""<q-td :props="props"><a v-if="props.row.activity_id" :href="'/activities?activity_id=' + props.row.activity_id" class="text-primary underline">Open activity {{ props.row.activity_id }}</a></q-td>""")
                        curve = pd.DataFrame(card["summary"]).rename(columns=dict(columns)).to_csv(index=False).encode("utf-8")
                        ui.button("Download peak-power curve CSV",on_click=lambda curve=curve:ui.download(curve,"peak-power-curve.csv"))
    if frame.empty:
        return
    export = frame.copy()
    export["date"] = export.date.dt.strftime("%Y-%m-%d")
    export["week"] = export.week.dt.strftime("%Y-%m-%d")
    export["pace_at_hr"] = frame.speed_at_hr.map(lambda speed:pace_text((1609.344 if prepared["imperial"] else 1000)/speed) if speed is not None and speed>0 else None)
    export["paired_coverage"] *= 100
    export["power_coverage"] *= 100
    options = data["options"]
    export["HR band low (bpm)"] = options["hr_low"]
    export["HR band high (bpm)"] = options["hr_high"]
    metres = 1609.344 if prepared["imperial"] else 1000
    export[f"Fast pace bound (min/{prepared['unit']})"] = pace_text(metres/options["speed_high"])
    export[f"Slow pace bound (min/{prepared['unit']})"] = pace_text(metres/options["speed_low"])
    export["Minimum steady minutes"] = options["min_minutes"]
    export["Terrain selection"] = options["terrain"]
    export["Period start"] = prepared["start"].isoformat()
    export["Period end"] = prepared["end"].isoformat()
    for key,name in (("ftp","Running FTP"),("lthr","LTHR")):
        export[name] = thresholds.get(key+"_effective")
        export[name+" source"] = (thresholds.get(key+"_status") or {}).get("effective_source")
        export[name+" evidence age"] = (thresholds.get(key+"_status") or {}).get("source_age_status")
    labels = dict(activity_id="Activity ID",name="Activity",date="Date",week="Monday week",sport="Sport",family="Confirmed workout family",terrain="Terrain group",
        ascent_density="Ascent density (m/km)",group="Comparison group",duration_min="Moving-preferred minutes",distance=f"Distance ({prepared['unit']})",time_source="Time source",
        temporal_minutes="Elapsed/window minutes",paired_coverage="Paired speed/HR coverage (%)",power_coverage="Power coverage (%)",speed_cv="Speed CV (fraction)",
        mean_hr="Time-weighted HR (bpm)",mean_speed="Time-weighted speed (m/s)",qualification="Steady qualification",steady="Steady eligible",
        hr_band_minutes="Minutes in HR band",pace_band_minutes="Minutes in pace band",speed_at_hr="Speed at HR band (m/s)",pace_at_hr=f"Pace at HR band (min/{prepared['unit']})",
        hr_at_pace="HR at pace band (bpm)",running_efficiency="Running efficiency (m/s per bpm)",durability_reason="Durability qualification",
        pace_decoupling_pct="Speed/HR decoupling (%)",hr_drift_pct="HR drift (%)",cadence="Reported cadence (spm)",power_reason="Power qualification",
        avg_power="Imported average power (W)",normalized_power="Imported normalized power (W)",variability="NP / average power",power_efficiency="NP / average HR (W per bpm)",zone_usable="Power zones eligible",zone_reason="Power zone qualification")
    for duration in (5,30,60,300,1200):
        labels[f"peak_{duration}"] = f"Peak {duration}s (W)"
    for zone in range(1,8):
        labels[f"power_zone_{zone}_s"] = f"Power zone {zone} (seconds)"
    rows = json.loads(export.to_json(orient="records"))
    fields = ["activity_id","date","name","qualification","family","terrain","paired_coverage","hr_band_minutes","pace_band_minutes","pace_at_hr","hr_at_pace","running_efficiency","cadence","distance","duration_min","durability_reason","pace_decoupling_pct","hr_drift_pct","power_reason","power_coverage","avg_power","normalized_power","variability","power_efficiency","zone_reason",*[f"peak_{d}" for d in (5,30,60,300,1200)],*[f"power_zone_{z}_s" for z in range(1,8)]]
    with ui.expansion("Performance measurements and qualification table",icon="table_chart").classes("w-full"):
        table = ui.table(columns=[dict(name=k,field=k,label=labels[k],align="left",sortable=True) for k in fields],rows=rows,row_key="activity_id",pagination=10).classes("w-full")
        table.add_slot("body-cell-activity_id","""<q-td :props="props"><a :href="'/activities?activity_id=' + props.row.activity_id" class="text-primary underline">Open activity {{ props.row.activity_id }}</a></q-td>""")
        ui.label("All selected activities, including excluded rows; separate qualification columns explain missing plots. Table links provide keyboard access. CSV retains all measures, units and reasons.").classes("text-xs")
        ui.button("Download performance data CSV",on_click=lambda:ui.download(export.rename(columns=labels).to_csv(index=False).encode("utf-8"),"performance-data.csv"))
