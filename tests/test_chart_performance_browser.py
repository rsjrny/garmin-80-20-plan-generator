from datetime import date, datetime, timedelta, timezone
import csv
import sqlite3

import pytest
from playwright.sync_api import expect, sync_playwright

from browser_test_support import wait_for_layout
from season_browser_logs import application_logs
from test_activity_calendar_days import _database, _insert_activity
from test_activity_grid_browser import _start_server, _stop_server, _free_loopback_port
from garmin_data_hub.db.queries import ACTIVITY_METRICS_PROVENANCE_VERSION, set_setting


def performance_database(tmp_path):
    db = _database(tmp_path)
    day = date.today()-timedelta(days=1)
    for aid in (101,202,303,404,505):
        _insert_activity(db,aid,local=day.isoformat()+'T12:00:00',gmt=day.isoformat()+'T12:00:00')
    conn = sqlite3.connect(db)
    conn.execute('ALTER TABLE activity ADD COLUMN activity_name TEXT')
    set_setting(conn,'unit_system','Metric')
    set_setting(conn,'nicegui_activity_velocity_display','Pace')
    conn.execute('UPDATE athlete_profile SET ftp_override=250,lthr_override=160')
    conn.execute("UPDATE activity SET activity_name='Steady morning',average_hr=130,avg_power=200,norm_power=220 WHERE activity_id=101")
    conn.execute("UPDATE activity SET activity_name='Steady evening',average_hr=135,avg_power=210,norm_power=230 WHERE activity_id=202")
    conn.execute("UPDATE activity SET activity_name='Variable workout' WHERE activity_id=303")
    conn.execute("UPDATE activity SET activity_name='No tracks' WHERE activity_id=404")
    conn.execute("UPDATE activity SET activity_type='cycling',activity_name='Ride' WHERE activity_id=505")
    for aid in (101,202,303):
        conn.execute('INSERT INTO activity_metrics(activity_id,refresh_provenance_version,threshold_ftp_w,threshold_lthr_bpm,moving_time_s,pace_decoupling_pct,hr_drift_pct,peak_power_5s_w,peak_power_30s_w,peak_power_60s_w,peak_power_300s_w,peak_power_1200s_w,power_zone_1_s,power_zone_2_s,power_zone_3_s,power_zone_4_s,power_zone_5_s,power_zone_6_s,power_zone_7_s) VALUES(?,?,250,160,3500,-2,3,300,280,260,240,220,0,3600,0,0,0,0,0)',(aid,ACTIVITY_METRICS_PROVENANCE_VERSION))
        beginning = datetime.combine(day,datetime.min.time()).replace(hour=12,tzinfo=timezone.utc)
        records = []
        for seq in range(121):
            speed = (2 if seq%2 else 5) if aid == 303 else 3 if aid == 101 else 3.2
            records.append((aid,seq,(beginning+timedelta(seconds=seq*30)).isoformat(),speed,135 if aid==202 else 130,200))
        conn.executemany('INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,speed_mps,heart_rate_bpm,power_w) VALUES(?,?,?,?,?,?)',records)
    conn.commit(); conn.close()
    return db


@pytest.mark.browser
def test_performance_drilldown_bands_exports_refresh_and_responsive(tmp_path,browser_artifacts):
    db = performance_database(tmp_path)
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport=dict(width=1440,height=1000))
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.on('console',lambda message:errors.append(message.text) if message.type=='error' else None)
            def close_drawer():
                backdrop = page.locator('.q-drawer__backdrop')
                if page.viewport_size['width']==390 and backdrop.is_visible():
                    backdrop.click(position=dict(x=350,y=120))
            def select(label,value):
                close_drawer()
                page.get_by_label(label,exact=True).locator('xpath=ancestor::label[1]').click()
                page.get_by_role('option',name=value,exact=True).click()
                expect(page.get_by_label(label,exact=True)).to_have_value(value)
            def ready():
                expect(page.locator('.js-plotly-plot')).to_have_count(3)
                page.locator('.js-plotly-plot').first.locator('.scatterlayer .trace').first.locator('.point').nth(1).wait_for()
            def chart_layout():
                wait_for_layout(page)
                page.wait_for_function("""() => [...document.querySelectorAll('.js-plotly-plot')].every(plot =>
                    !plot.clientWidth || (plot._fullLayout && Math.abs(plot._fullLayout.width-plot.clientWidth) <= 1))""")
            page.goto(f'http://127.0.0.1:{port}/charts',wait_until='networkidle')
            select('Date range','4 weeks')
            select('Sport','running')
            select('Chart section','Performance')
            ready()
            page.wait_for_function("document.querySelector('.js-plotly-plot')?.data?.[0]?.meta?.targets?.length === 2")
            assert page.locator('.js-plotly-plot').first.evaluate("plot => plot._fullLayout.yaxis.range[0] > plot._fullLayout.yaxis.range[1]")
            page.locator('.js-plotly-plot').first.locator('.scatterlayer .trace').first.locator('.point').nth(1).click(force=True)
            page.get_by_text('Activity 202',exact=True).wait_for()
            assert 'activity_id=202' in page.url
            page.get_by_role('link',name='Return to Charts',exact=True).click()
            ready()
            expect(page.get_by_label('Sport',exact=True)).to_have_value('running')
            page.get_by_label('HR band low (bpm)',exact=True).fill('134')
            page.get_by_label('HR band low (bpm)',exact=True).press('Tab')
            page.wait_for_function("document.querySelector('.js-plotly-plot')?.data?.[0]?.meta?.targets?.length === 1")
            page.get_by_label('HR band low (bpm)',exact=True).fill('150')
            page.get_by_label('HR band low (bpm)',exact=True).press('Tab')
            page.get_by_text('Performance unavailable: HR band requires',exact=False).wait_for()
            page.get_by_label('HR band low (bpm)',exact=True).fill('125')
            page.get_by_label('HR band low (bpm)',exact=True).press('Tab')
            ready()
            page.get_by_label('Fast pace (min/km)',exact=True).fill('4:99')
            page.get_by_label('Fast pace (min/km)',exact=True).press('Tab')
            page.get_by_text('Performance unavailable: Enter pace as minutes:seconds',exact=False).wait_for()
            page.get_by_label('Fast pace (min/km)',exact=True).fill('5:00')
            page.get_by_label('Fast pace (min/km)',exact=True).press('Tab')
            ready()
            page.get_by_role('button',name='Reset zoom',exact=True).first.click()
            page.wait_for_function("document.querySelector('.js-plotly-plot')?._fullLayout?.yaxis?.range?.[0] > document.querySelector('.js-plotly-plot')?._fullLayout?.yaxis?.range?.[1]")
            page.get_by_text('Performance measurements and qualification table',exact=True).click()
            page.get_by_text('Speed variation exceeds 10%',exact=True).first.wait_for()
            page.get_by_text('No usable track samples',exact=True).first.wait_for()
            with page.expect_download() as download:
                page.get_by_role('button',name='Download performance data CSV',exact=True).click()
            assert download.value.suggested_filename=='performance-data.csv'
            downloaded = tmp_path/'performance.csv'
            download.value.save_as(downloaded)
            with downloaded.open(newline='',encoding='utf-8') as f:
                exported = list(csv.DictReader(f))
            assert len(exported)==4
            assert all(r['HR band low (bpm)']=='125.0' and r['Fast pace bound (min/km)']=='5:00' for r in exported)
            excluded = next(r for r in exported if r['Activity ID']=='404')
            assert excluded['Pace at HR band (min/km)']==''
            link = page.get_by_role('link',name='Open activity 101',exact=True)
            link.focus(); link.press('Enter')
            page.get_by_text('Activity 101',exact=True).wait_for()
            page.get_by_role('link',name='Return to Charts',exact=True).click()
            ready()
            browser_artifacts.mkdir(parents=True,exist_ok=True)
            page.evaluate('window.scrollTo(0,0)')
            chart_layout()
            page.screenshot(path=str(browser_artifacts/'desktop.png'),full_page=True)
            page.set_viewport_size(dict(width=390,height=844))
            page.reload(wait_until='networkidle')
            close_drawer(); ready()
            expect(page.get_by_label('HR band low (bpm)',exact=True)).to_have_value('125')
            page.get_by_text('Performance measurements and qualification table',exact=True).click()
            page.get_by_role('link',name='Open activity 202',exact=True).wait_for()
            assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
            page.get_by_text('Performance measurements and qualification table',exact=True).click()
            page.evaluate('window.scrollTo(0,0)')
            chart_layout()
            page.screenshot(path=str(browser_artifacts/'narrow.png'),full_page=True)
            # Presentation changes retain cached evidence until explicit refresh.
            conn = sqlite3.connect(db)
            conn.execute('UPDATE athlete_profile SET ftp_override=NULL,ftp_calc=NULL')
            conn.commit(); conn.close()
            page.get_by_label('Performance charts',exact=True).locator('xpath=ancestor::label[1]').click()
            for index,label in enumerate(('Average and normalized power','Power zones','Peak-power curve')):
                page.get_by_role('option',name=label,exact=True).click()
                expect(page.locator('.js-plotly-plot')).to_have_count(4+index)
            page.keyboard.press('Escape')
            expect(page.locator('.js-plotly-plot')).to_have_count(6)
            page.get_by_text('Peak-power source table',exact=True).click()
            page.get_by_text('Duration (seconds)',exact=True).wait_for()
            with page.expect_download() as curve_download:
                page.get_by_role('button',name='Download peak-power curve CSV',exact=True).click()
            assert curve_download.value.suggested_filename == 'peak-power-curve.csv'
            page.get_by_text('Peak-power source table',exact=True).click()
            expect(page.get_by_role('button',name='Download peak-power curve CSV',exact=True)).not_to_be_visible()
            page.evaluate('window.scrollTo(0,0)')
            chart_layout()
            page.screenshot(path=str(browser_artifacts/'power_narrow.png'),full_page=True)
            page.set_viewport_size(dict(width=1440,height=1000))
            chart_layout()
            page.screenshot(path=str(browser_artifacts/'power_desktop.png'),full_page=True)
            page.get_by_role('button',name='Refresh data',exact=True).click()
            expect(page.locator('.js-plotly-plot')).to_have_count(3)
            page.get_by_text('Performance measurements and qualification table',exact=True).click()
            page.get_by_text('Running FTP provenance unavailable or stale',exact=True).first.wait_for()
            select('Sport','cycling')
            expect(page.locator('.js-plotly-plot')).to_have_count(0)
            page.get_by_text('Performance measurements and qualification table',exact=True).click()
            page.get_by_text('Running FTP cannot qualify other sports',exact=True).first.wait_for()
            page.evaluate('window.socket?.disconnect()')
            browser.close()
    finally:
        server = _stop_server(process)
        (browser_artifacts/'browser-server.txt').write_text(server,encoding='utf-8')
    assert not errors,errors
    assert 'Traceback' not in application_logs(server) and 'ERROR:' not in application_logs(server),server


@pytest.mark.browser
def test_performance_empty_and_imperial_saved_pace_bands(tmp_path,browser_artifacts):
    db = _database(tmp_path)
    conn = sqlite3.connect(db)
    set_setting(conn,'unit_system','Imperial')
    conn.commit(); conn.close()
    process = _start_server(db,port := _free_loopback_port())
    errors = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport=dict(width=390,height=844))
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto(f'http://127.0.0.1:{port}/charts',wait_until='networkidle')
            backdrop = page.locator('.q-drawer__backdrop')
            if backdrop.is_visible(): backdrop.click(position=dict(x=350,y=120))
            page.get_by_label('Chart section',exact=True).locator('xpath=ancestor::label[1]').click()
            page.get_by_role('option',name='Performance',exact=True).click()
            expect(page.get_by_label('Fast pace (min/mi)',exact=True)).to_have_value('8:03')
            expect(page.get_by_label('Slow pace (min/mi)',exact=True)).to_have_value('10:44')
            page.get_by_text('No activities match these filters.',exact=True).first.wait_for()
            expect(page.locator('.js-plotly-plot')).to_have_count(0)
            page.get_by_label('Fast pace (min/mi)',exact=True).fill('8:20')
            page.get_by_label('Fast pace (min/mi)',exact=True).press('Tab')
            page.reload(wait_until='networkidle')
            expect(page.get_by_label('Chart section',exact=True)).to_have_value('Performance')
            expect(page.get_by_label('Fast pace (min/mi)',exact=True)).to_have_value('8:20')
            wait_for_layout(page)
            page.evaluate('window.socket?.disconnect()')
            browser.close()
    finally:
        server = _stop_server(process)
        (browser_artifacts/'browser-server.txt').write_text(server,encoding='utf-8')
    assert not errors,errors
    assert 'Traceback' not in application_logs(server) and 'ERROR:' not in application_logs(server),server
