// Client-local hover keeps raw edge identities while Plotly draws a bounded sample.
(async function(config) {
  const registry = window.gdhTrackCharts ||= {};
  for (const [key, old] of Object.entries(registry)) {
    if (!document.getElementById('c' + key)) { old.dispose(); delete registry[key]; }
  }
  const plot = document.getElementById('c' + config.plot);
  const component = getElement(config.plot);
  const map = getElement(config.map)?.map;
  if (!plot || !map) return;
  for (let tries = 0; !plot._fullLayout && tries < 100; tries++)
    await new Promise(resolve => setTimeout(resolve, 50));
  if (!plot._fullLayout || !document.getElementById('c' + config.plot)) return;
  const P = component.Plotly, rows = config.rows;
  const readout = document.getElementById('c' + config.readout);
  readout.textContent = 'Hover or tap the route or a chart, or inspect a position with the keyboard controls.';
  let axis = config.axis, marker = null, pinned = null, current = null, bounds = null;
  let raf = null, requested = undefined, busy = false, redraw = false, disposed = false;
  const displayed = () => plot.isConnected && plot.getClientRects().length > 0 &&
    getComputedStyle(plot).display !== 'none' && plot.clientWidth > 0 && plot.clientHeight > 0;
  const resize = () => {
    if (disposed || !displayed()) return;
    P.Plots.resize(plot).catch(error => {
      // Plotly's deferred resize may run after a tab hides or removes this chart.
      if (!disposed && displayed()) queueMicrotask(() => { throw error; });
    });
  };
  const resizeObserver = new ResizeObserver(resize);
  resizeObserver.observe(plot);
  const x = (row) => axis === 'time' ? (row[1] === null ? null : row[1] / 60) : row[0];
  const finite = v => v !== null && Number.isFinite(v);
  const pace = v => { const s = Math.round(v); return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); };
  const shapes = () => [
    ...(bounds ? [{type:'rect',xref:'x',yref:'paper',x0:bounds[0],x1:bounds[1],y0:0,y1:1,
      fillcolor:'#70489c',opacity:.13,line:{width:0},layer:'below'}] : []),
    ...(current !== null && finite(x(rows[current])) ? [{type:'line',xref:'x',yref:'paper',
      x0:x(rows[current]),x1:x(rows[current]),y0:0,y1:1,line:{color:'#172e50',width:2,dash:'dot'}}] : [])
  ];
  const draw = async () => {
    if (disposed) return;
    if (busy) {redraw=true;return;}
    busy = true;
    try { await P.relayout(plot, {shapes: shapes()}); } finally { busy = false; }
    if (redraw) {redraw=false;draw();}
    if (requested !== undefined) schedule(requested);
  };
  const show = index => {
    if (index === current) return;
    if (index !== null && (!Number.isInteger(index) || !rows[index] || !finite(x(rows[index])) || !config.selectable[index])) return;
    current = index;
    plot.dataset.trackCursor = index === null ? '' : index;
    if (index === null) {
      if (marker) {map.removeLayer(marker); marker = null;}
      readout.textContent = 'Hover or tap the route or a chart, or inspect a position with the keyboard controls.';
    } else {
      const row = rows[index];
      if (!marker) marker = L.circleMarker([row[2],row[3]], {radius:7,color:'#172e50',weight:3,fillColor:'white',fillOpacity:1,interactive:false}).addTo(map);
      else marker.setLatLng([row[2],row[3]]);
      marker.bringToFront();
      marker._gdhTrackCursor = true;
      const fields = [row[0].toFixed(3) + ' ' + config.unit,
        row[1] === null ? 'Elapsed time unavailable' : (row[1]/60).toFixed(2) + ' min elapsed'];
      config.metrics.forEach((metric,i) => {
        const value = row[4+i];
        fields.push(metric.label + ': ' + (value === null ? 'unavailable' :
          (i === 0 ? pace(value) : value.toFixed(0)) + ' ' + metric.unit));
      });
      readout.textContent = 'Sampled route position · ' + fields.join(' · ');
    }
    draw();
  };
  const schedule = index => {
    requested = index;
    if (raf !== null || busy) return;
    raf = requestAnimationFrame(() => {raf = null; const value = requested; requested = undefined; show(value);});
  };
  const hover = e => { const i = e.points?.[0]?.customdata; if (Number.isInteger(i)) schedule(i); };
  const reveal = i => {
    if (i !== null && rows[i] && !map.getBounds().contains([rows[i][2],rows[i][3]]))
      map.panTo([rows[i][2],rows[i][3]], {animate:false});
  };
  const click = e => { const i = e.points?.[0]?.customdata; if (Number.isInteger(i)) {pinned=i; reveal(i); schedule(i);} };
  const clearHover = () => schedule(pinned);
  const selected = e => {
    // Use the actual drag extent, not the extent of the decimated points.
    const extent = Object.entries(e?.range || {}).find(([key]) => /^x\d*$/.test(key))?.[1];
    if (extent && extent.length === 2 && extent.every(Number.isFinite))
      getElement(config.plot).$emit('track-range', {axis, start:Math.max(0,Math.min(...extent)),end:Math.min(config.maximum,Math.max(...extent))});
  };
  plot.on('plotly_hover', hover);
  plot.on('plotly_click', click);
  plot.on('plotly_unhover', clearHover);
  plot.on('plotly_selected', selected);
  const handlers = [];
  map.eachLayer(layer => {
    const props = layer.feature?.properties;
    if (!Number.isInteger(props?.edge_start)) return;
    const nearest = e => {
      const target = map.latLngToContainerPoint(e.latlng);
      let best = null, error = Infinity;
      for (let i=props.edge_start; i<=props.edge_end; i++) {
        if (!config.selectable[i] || !finite(x(rows[i]))) continue;
        const point = map.latLngToContainerPoint([rows[i][2],rows[i][3]]);
        const delta = point.distanceTo(target);
        if (delta < error) {best=i;error=delta;}
      }
      return best;
    };
    const move = e => {const i=nearest(e); if (i!==null) schedule(i);};
    const tap = e => {const i=nearest(e); if (i!==null) {pinned=i;schedule(i);}};
    layer.on('mousemove', move);layer.on('click',tap);layer.on('mouseout',clearHover);
    handlers.push([layer,move,tap]);
  });
  const controller = {
    inspect(value) {
      let best=null, error=Infinity;
      rows.forEach((row,i) => { const v=x(row); if (!finite(v)||!config.selectable[i]) return;
        const delta=Math.abs(value-v);if(delta<error){best=i;error=delta;}});
      pinned=best; reveal(best); schedule(best);
    },
    clear() {pinned=null; schedule(null);},
    select(value) {bounds=value;plot.dataset.trackBounds=JSON.stringify(value);draw();},
    dispose() {
      disposed=true;resizeObserver.disconnect();if(raf!==null)cancelAnimationFrame(raf);
      if(marker)map.removeLayer(marker);
      handlers.forEach(([layer,move,tap])=>{layer.off('mousemove',move);layer.off('click',tap);layer.off('mouseout',clearHover);});
      plot.removeListener('plotly_hover',hover);plot.removeListener('plotly_click',click);
      plot.removeListener('plotly_unhover',clearHover);plot.removeListener('plotly_selected',selected);
    }
  };
  registry[config.plot]?.dispose();
  registry[config.plot]=controller;
  map.once('unload',()=>{controller.dispose();delete registry[config.plot];});
  plot.dataset.trackCursor='';
  plot.dataset.trackLinked='true';
  plot.dataset.trackAxis=axis;
})(CONFIG);
