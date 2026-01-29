(function() {
  const dataEl = document.getElementById('mapData');
  const configEl = document.getElementById('mapConfig');
  if (!dataEl || !configEl) {
    return;
  }

  let mapData = {};
  let mapConfig = {};
  try {
    mapData = JSON.parse(dataEl.textContent || '{}');
    mapConfig = JSON.parse(configEl.textContent || '{}');
  } catch (_) {
    mapData = {};
    mapConfig = {};
  }

  const mapCanvas = document.getElementById('mapCanvas');
  const mapNotice = document.getElementById('mapNotice');
  const warningsEl = document.getElementById('mapWarnings');
  const missingEl = document.getElementById('missingAirports');
  const detailEl = document.getElementById('airportDetail');
  const toggleRoutes = document.getElementById('toggleRoutes');
  const toggleHeatmap = document.getElementById('toggleHeatmap');
  const routeOpacity = document.getElementById('routeOpacity');
  const heatIntensity = document.getElementById('heatIntensity');
  const applyFilters = document.getElementById('applyFilters');

  if (!mapCanvas) {
    return;
  }

  if (!window.L) {
    if (mapCanvas) {
      mapCanvas.classList.add('map-canvas--offline');
      mapCanvas.innerHTML = '<div class="map-fallback">Map library failed to load. Filters and data notes remain available.</div>';
    }
    if (mapNotice) {
      mapNotice.textContent = 'Leaflet failed to load; map rendering is unavailable.';
    }
    setWarningsText('Map rendering disabled because Leaflet failed to load.');
    return;
  }

  const map = L.map(mapCanvas, { zoomControl: true });
  const tileUrl = mapConfig.tile_url || 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const tileLayer = L.tileLayer(tileUrl, {
    attribution: mapConfig.tile_attribution || ''
  }).addTo(map);

  tileLayer.on('tileerror', function() {
    if (mapNotice) {
      mapNotice.textContent = 'Tile server failed to load. Routes and airport lists are still available.';
    }
  });

  const routeLayer = L.layerGroup().addTo(map);
  const airportLayer = L.layerGroup().addTo(map);
  let heatLayer = null;
  let currentHeatMultiplier = Number(heatIntensity ? heatIntensity.value : 1) || 1;

  if (!L.heatLayer && toggleHeatmap) {
    toggleHeatmap.disabled = true;
    toggleHeatmap.checked = false;
    setNotice('Heatmap unavailable (Leaflet.heat not loaded).');
    setWarningsText('Heatmap disabled because Leaflet.heat failed to load.');
  }

  function setNotice(message) {
    if (mapNotice) {
      mapNotice.textContent = message || '';
    }
  }

  function setWarningsText(text) {
    if (!warningsEl) return;
    warningsEl.innerHTML = '';
    warningsEl.textContent = text;
  }

  function updateWarnings(data) {
    if (!warningsEl) return;
    warningsEl.innerHTML = '';
    const warnings = [].concat(data.warnings || []);
    if (data.limits && data.limits.limited_airports) {
      warnings.push('Airport list limited to top ' + data.limits.max_airports + ' by visits.');
    }
    if (data.limits && data.limits.limited_routes) {
      warnings.push('Route list limited to top ' + data.limits.max_routes + ' by count.');
    }
    if (!warnings.length) {
      setWarningsText('No data warnings.');
      return;
    }
    const ul = document.createElement('ul');
    warnings.forEach(msg => {
      const li = document.createElement('li');
      li.textContent = msg;
      ul.appendChild(li);
    });
    warningsEl.appendChild(ul);
  }

  function updateMissingAirports(list) {
    if (!missingEl) return;
    missingEl.innerHTML = '';
    if (!Array.isArray(list) || list.length === 0) {
      missingEl.textContent = 'All airports in this view have coordinates.';
      return;
    }
    const slice = list.slice(0, 12);
    const title = document.createElement('div');
    title.className = 'map-warning';
    title.textContent = 'Missing coordinates for: ' + slice.join(', ') + (list.length > slice.length ? ' +' + (list.length - slice.length) + ' more' : '');
    missingEl.appendChild(title);
  }

  function setBounds(coords) {
    if (!coords.length) {
      map.setView([39.5, -98.35], 4);
      return;
    }
    map.fitBounds(coords, { padding: [24, 24] });
  }

  function markerPopupHtml(airport) {
    const parts = [];
    parts.push('<strong>' + escapeHtml(airport.icao) + '</strong>');
    if (airport.name) {
      parts.push('<div>' + escapeHtml(airport.name) + '</div>');
    }
    parts.push('<div>Visits: ' + (airport.visits || 0) + '</div>');
    return parts.join('');
  }

  function updateDetail(airport) {
    if (!detailEl) return;
    if (!airport) {
      detailEl.textContent = 'Select an airport marker to see details.';
      return;
    }
    const aircraftList = (airport.top_aircrafts || []).join(', ') || 'None';
    detailEl.innerHTML = '';
    const title = document.createElement('div');
    title.className = 'map-detail-title';
    title.textContent = airport.icao + (airport.name ? ' - ' + airport.name : '');
    detailEl.appendChild(title);
    const coords = document.createElement('div');
    coords.className = 'muted';
    coords.textContent = 'Lat ' + airport.lat.toFixed(4) + ', Lon ' + airport.lon.toFixed(4);
    detailEl.appendChild(coords);
    const stats = document.createElement('div');
    stats.innerHTML = '<div>Visits: ' + airport.visits + '</div>' +
      '<div>Arrivals: ' + airport.arrivals + ' / Departures: ' + airport.departures + '</div>' +
      '<div>Last visit: ' + (airport.last_visit || 'Unknown') + '</div>' +
      '<div>Top aircraft: ' + escapeHtml(aircraftList) + '</div>';
    detailEl.appendChild(stats);
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function renderRoutes(data) {
    routeLayer.clearLayers();
    const routes = Array.isArray(data.routes) ? data.routes : [];
    const opacity = Number(routeOpacity ? routeOpacity.value : 0.7) || 0.7;
    routes.forEach(route => {
      const weight = Math.max(1, Math.min(8, Math.sqrt(route.count || 1)));
      const poly = L.polyline(route.line, {
        color: '#2563eb',
        weight: weight,
        opacity: opacity
      });
      poly.bindTooltip(route.dep + ' -> ' + route.arr + ' (' + route.count + ')');
      routeLayer.addLayer(poly);
    });
  }

  function renderAirports(data) {
    airportLayer.clearLayers();
    const airports = Array.isArray(data.airports) ? data.airports : [];
    airports.forEach(airport => {
      const radius = Math.max(4, Math.min(14, Math.sqrt(airport.visits || 1) * 2));
      const marker = L.circleMarker([airport.lat, airport.lon], {
        radius: radius,
        color: '#16a34a',
        fillColor: '#16a34a',
        fillOpacity: 0.6,
        weight: 1
      });
      marker.bindTooltip(markerPopupHtml(airport));
      marker.on('click', () => updateDetail(airport));
      airportLayer.addLayer(marker);
    });
  }

  function renderHeat(data) {
    const airports = Array.isArray(data.airports) ? data.airports : [];
    const points = airports.map(airport => {
      return [airport.lat, airport.lon, (airport.visits || 1) * currentHeatMultiplier];
    });
    if (!heatLayer && L.heatLayer) {
      heatLayer = L.heatLayer(points, { radius: 25, blur: 18, maxZoom: 7 });
    } else if (heatLayer) {
      heatLayer.setLatLngs(points);
    }
    if (heatLayer && toggleHeatmap && toggleHeatmap.checked) {
      if (!map.hasLayer(heatLayer)) {
        heatLayer.addTo(map);
      }
    } else if (heatLayer && map.hasLayer(heatLayer)) {
      map.removeLayer(heatLayer);
    }
  }

  function updateLayers(data) {
    updateWarnings(data);
    updateMissingAirports(data.missing_airports || []);

    const coords = [];
    (data.airports || []).forEach(airport => coords.push([airport.lat, airport.lon]));

    if (!coords.length) {
      setNotice('No airport coordinates available for the selected filters.');
    } else {
      setNotice('');
    }

    if (toggleRoutes && toggleRoutes.checked) {
      if (!map.hasLayer(routeLayer)) {
        map.addLayer(routeLayer);
      }
      renderRoutes(data);
    } else {
      routeLayer.clearLayers();
      if (map.hasLayer(routeLayer)) {
        map.removeLayer(routeLayer);
      }
    }

    renderAirports(data);
    renderHeat(data);
    setBounds(coords);
    updateDetail(null);
  }

  function updateAircraftOptions(data) {
    const select = document.getElementById('mapAircraft');
    if (!select) return;
    const selected = Array.from(select.selectedOptions || []).map(opt => opt.value);
    const preselected = data.applied_filters && Array.isArray(data.applied_filters.aircraft) ? data.applied_filters.aircraft : [];
    const options = (data.filters && data.filters.aircraft_options) ? data.filters.aircraft_options : [];
    select.innerHTML = '';
    options.forEach(opt => {
      const option = document.createElement('option');
      option.value = opt;
      option.textContent = opt;
      if (selected.includes(opt) || preselected.includes(opt)) {
        option.selected = true;
      }
      select.appendChild(option);
    });
  }

  function readFilters() {
    const start = document.getElementById('mapStart');
    const end = document.getElementById('mapEnd');
    const aircraft = document.getElementById('mapAircraft');
    return {
      start: start ? start.value : '',
      end: end ? end.value : '',
      aircraft: aircraft ? Array.from(aircraft.selectedOptions || []).map(opt => opt.value) : []
    };
  }

  function buildQuery(params) {
    const query = new URLSearchParams();
    if (params.start) query.set('start', params.start);
    if (params.end) query.set('end', params.end);
    if (Array.isArray(params.aircraft)) {
      params.aircraft.forEach(value => query.append('aircraft', value));
    }
    if (toggleRoutes) query.set('show_routes', toggleRoutes.checked ? '1' : '0');
    if (toggleHeatmap) query.set('show_heatmap', toggleHeatmap.checked ? '1' : '0');
    return query;
  }

  async function fetchData() {
    const filters = readFilters();
    const query = buildQuery(filters);
    try {
      const res = await fetch('/map/data?' + query.toString());
      const json = await res.json();
      mapData = json || {};
      updateAircraftOptions(mapData);
      updateLayers(mapData);
    } catch (err) {
      setNotice('Failed to load map data.');
    }
  }

  if (applyFilters) {
    applyFilters.addEventListener('click', fetchData);
  }
  if (toggleRoutes) {
    toggleRoutes.addEventListener('change', () => updateLayers(mapData));
  }
  if (toggleHeatmap) {
    toggleHeatmap.addEventListener('change', () => updateLayers(mapData));
  }
  if (routeOpacity) {
    routeOpacity.addEventListener('input', () => renderRoutes(mapData));
  }
  if (heatIntensity) {
    heatIntensity.addEventListener('input', () => {
      currentHeatMultiplier = Number(heatIntensity.value) || 1;
      renderHeat(mapData);
    });
  }

  updateAircraftOptions(mapData);
  updateLayers(mapData);
})();
