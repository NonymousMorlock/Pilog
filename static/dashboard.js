(function(){
  const charts = {};
  const initialDataEl = document.getElementById('initialData');
  const initialFlightsEl = document.getElementById('initialFlights');
  const initialLandingIndexMapEl = document.getElementById('initialLandingIndexMap');
  const initialLandingAvailableEl = document.getElementById('initialLandingAvailable');
  const initialData = initialDataEl ? JSON.parse(initialDataEl.textContent || '{}') : {};
  let allFlights = initialFlightsEl ? JSON.parse(initialFlightsEl.textContent || '[]') : [];
  let landingIdxMap = initialLandingIndexMapEl ? JSON.parse(initialLandingIndexMapEl.textContent || '[]') : [];
  let landingAvailable = initialLandingAvailableEl ? (initialLandingAvailableEl.textContent === 'true') : false;
  let flightPage = 0;
  const flightPageSize = 25;
  let sortKey = null;
  let sortDir = 'asc';
  let searchQuery = '';
  let highlightedRowPos = null; // sticky physical row index within current page

  window.chooseFolder = async function chooseFolder() {
    try {
      const res = await fetch('/pick_folder', { method: 'POST' });
      const json = await res.json();
      if (!res.ok) { showToast(json.error || 'Failed to pick folder', 'error'); return; }
      const formData = new FormData();
      formData.append('folder_path', json.folder_path);
      const setRes = await fetch('/set_folder', { method: 'POST', body: formData });
      const setJson = await setRes.json();
      if (!setRes.ok) { showToast(setJson.error || 'Failed to set folder', 'error'); return; }
      const wf = document.getElementById('watchedFolder');
      if (wf) wf.textContent = json.folder_path;
      const dres = await fetch('/data');
      const djson = await dres.json();
      if (djson && djson.summary) { renderFromSummary(djson.summary); }
      showToast('Watching folder set', 'success');
    } catch (e) { showToast('Error choosing folder: ' + e, 'error'); }
  }

  function createChart(id, type, labels, values, color, opts) {
    if (charts[id]) { try { charts[id].destroy(); } catch(_) {} }
    const horizontal = opts && opts.horizontal;
    const xTitle = opts && opts.xTitle ? String(opts.xTitle) : '';
    const yTitle = opts && opts.yTitle ? String(opts.yTitle) : '';
    const datasetLabel = opts && opts.datasetLabel ? String(opts.datasetLabel) : '';
    charts[id] = new Chart(document.getElementById(id), {
      type: type,
      data: { labels, datasets: [{ label: datasetLabel, data: values, backgroundColor: color, borderColor: color, fill: type === 'line' ? false : true }] },
      options: {
        indexAxis: horizontal ? 'y' : 'x',
        plugins: { legend: { display: !!datasetLabel } },
        scales: {
          x: { title: { display: !!xTitle, text: xTitle } },
          y: { title: { display: !!yTitle, text: yTitle }, beginAtZero: true }
        }
      }
    });
  }

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function getFilteredSortedFlightIndices() {
    const n = Array.isArray(allFlights) ? allFlights.length : 0;
    let indices = Array.from({ length: n }, (_, i) => i);
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      indices = indices.filter(i => {
        const f = allFlights[i] || {};
        return `${f.dep} ${f.arr} ${f.aircraft} ${f.tail}`.toLowerCase().includes(q);
      });
    }
    if (sortKey) {
      indices.sort((ia, ib) => {
        const a = allFlights[ia] || {};
        const b = allFlights[ib] || {};
        const av = a[sortKey];
        const bv = b[sortKey];
        if (av == null && bv == null) return 0;
        if (av == null) return sortDir === 'asc' ? -1 : 1;
        if (bv == null) return sortDir === 'asc' ? 1 : -1;
        if (typeof av === 'number' && typeof bv === 'number') return sortDir === 'asc' ? av - bv : bv - av;
        return sortDir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
      });
    }
    return indices;
  }

  function renderFlightTable() {
    try {
      const tbody = document.getElementById('flightTableBody');
      if (!tbody || !Array.isArray(allFlights)) return;
      tbody.innerHTML = '';
      const order = getFilteredSortedFlightIndices();
      const start = flightPage * flightPageSize;
      const end = Math.min(start + flightPageSize, order.length);
      for (let i = start; i < end; i++) {
        const origIdx = order[i];
        const f = allFlights[origIdx];
        const tr = document.createElement('tr');
        tr.id = 'flight-row-' + origIdx;
        let linkCell = '';
        if (landingAvailable) {
          const li = Array.isArray(landingIdxMap) ? landingIdxMap[origIdx] : null;
          const href = `/landing-rates?date=${encodeURIComponent(f.date)}&aircraft=${encodeURIComponent(f.aircraft)}${li != null ? ('&landingIndex=' + li) : ''}`;
          linkCell = `<td><a href="${href}" style="color:var(--link);">View</a></td>`;
        }
        tr.innerHTML = `
          <td>${esc(f.date)}</td>
          <td>${esc(f.dep)}</td>
          <td>${esc(f.arr)}</td>
          <td>${(Number(f.hours) || 0).toFixed(1)}</td>
          <td>${esc(f.aircraft)}</td>
          <td>${esc(f.tail)}</td>
          ${linkCell}
        `;
        // Apply sticky highlight to the same physical row position
        const localRow = i - start;
        if (highlightedRowPos != null && localRow === highlightedRowPos) {
          tr.style.outline = '2px solid var(--link)';
        }
        tbody.appendChild(tr);
      }
      const info = document.getElementById('flightPageInfo');
      if (info) {
        const totalPages = Math.max(1, Math.ceil(order.length / flightPageSize));
        info.textContent = `Page ${Math.min(flightPage + 1, totalPages)} of ${totalPages}`;
      }
      if (end - start === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = landingAvailable ? 7 : 6;
        td.className = 'muted';
        td.textContent = searchQuery ? 'No matching flights.' : 'No flights to display.';
        tr.appendChild(td);
        tbody.appendChild(tr);
      }
    } catch(_) {}
  }

  function renderFromSummary(summary) {
    const hours = (Number(summary.total_hours) || 0).toFixed(1);
    const hoursEl = document.getElementById('totalHours');
    if (hoursEl) hoursEl.textContent = hours;
    createChart("hoursByAircraft", "bar", Object.keys(summary.flights_by_aircraft), Object.values(summary.flights_by_aircraft), "#2563eb", { datasetLabel: "Hours", xTitle: "Aircraft", yTitle: "Hours" });
    createChart("countByAircraft", "bar", Object.keys(summary.count_by_aircraft), Object.values(summary.count_by_aircraft), "#d97706", { datasetLabel: "Flights", xTitle: "Aircraft", yTitle: "Flights" });
    createChart("routesChart", "bar", Object.keys(summary.flights_by_route), Object.values(summary.flights_by_route), "#16a34a", { datasetLabel: "Flights", xTitle: "Route", yTitle: "Flights", horizontal: true });
    if (charts["hoursOverTime"]) { try { charts["hoursOverTime"].destroy(); } catch(_) {} }
    charts["hoursOverTime"] = new Chart(document.getElementById("hoursOverTime"), {
      type: "line",
      data: {
        labels: Object.keys(summary.flights_by_date),
        datasets: [{ label: "Hours Flown", data: Object.values(summary.flights_by_date), borderColor: "#dc2626", backgroundColor: "#dc2626", fill: false }]
      },
      options: {
        plugins: { legend: { display: true } },
        scales: {
          x: { title: { display: true, text: "Date" } },
          y: { title: { display: true, text: "Hours" }, beginAtZero: true }
        }
      }
    });
  }

  // Initial render
  renderFromSummary(initialData);
  renderFlightTable();
  const prevBtn = document.getElementById('prevFlightPage');
  const nextBtn = document.getElementById('nextFlightPage');
  if (prevBtn) prevBtn.onclick = () => { if (flightPage > 0) { flightPage--; renderFlightTable(); } };
  if (nextBtn) nextBtn.onclick = () => { const order = getFilteredSortedFlightIndices(); const totalPages = Math.max(1, Math.ceil(order.length / flightPageSize)); if (flightPage + 1 < totalPages) { flightPage++; renderFlightTable(); } };

  // Sorting handlers
  (function(){
    const thead = document.querySelector('table thead');
    if (!thead) return;
    thead.addEventListener('click', (e) => {
      const th = e.target.closest('th[data-sort-key]');
      if (!th) return;
      const key = th.getAttribute('data-sort-key');
      if (sortKey === key) { sortDir = sortDir === 'asc' ? 'desc' : 'asc'; } else { sortKey = key; sortDir = 'asc'; }
      // Update aria-sort
      document.querySelectorAll('th[data-sort-key]').forEach(x => x.setAttribute('aria-sort', 'none'));
      th.setAttribute('aria-sort', sortDir === 'asc' ? 'ascending' : 'descending');
      flightPage = 0;
      renderFlightTable();
    });
  })();

  // Search handler
  (function(){
    const input = document.getElementById('flightSearch');
    if (!input) return;
    input.addEventListener('input', () => { searchQuery = input.value || ''; flightPage = 0; renderFlightTable(); });
  })();

  // Socket updates
  const socket = io();
  socket.on('log_update', (payload) => {
    if (payload && payload.summary) { renderFromSummary(payload.summary); }
    if (payload && payload.flights) {
      allFlights = payload.flights || [];
      if (Array.isArray(payload.landing_index_for_flight)) landingIdxMap = payload.landing_index_for_flight;
      if (typeof payload.landing_available === 'boolean') landingAvailable = payload.landing_available;
      const order = getFilteredSortedFlightIndices();
      const totalPages = Math.max(1, Math.ceil(order.length / flightPageSize));
      if (flightPage >= totalPages) flightPage = totalPages - 1;
      renderFlightTable();
    }
    if (payload && payload.filename) {
      const fnEl = document.getElementById('currentFilename');
      if (fnEl) fnEl.textContent = payload.filename;
    }
    if (payload && payload.watched_folder) {
      const wfEl = document.getElementById('watchedFolder');
      if (wfEl) wfEl.textContent = payload.watched_folder;
    }
  });

  // Deep-link target flight row
  (function() {
    const params = new URLSearchParams(window.location.search);
    const idxStr = params.get('flightIndex');
    if (!idxStr) return;
    const origIdx = Number(idxStr);
    if (!Number.isFinite(origIdx)) return;
    const order = getFilteredSortedFlightIndices();
    const pos = order.indexOf(origIdx);
    if (pos >= 0) {
      flightPage = Math.floor(pos / flightPageSize);
      highlightedRowPos = pos % flightPageSize;
    }
    renderFlightTable();
    // Scroll to the highlighted physical row
    try {
      const tbody = document.getElementById('flightTableBody');
      if (tbody && highlightedRowPos != null) {
        const trs = tbody.querySelectorAll('tr');
        const el = trs[highlightedRowPos];
        if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
      }
    } catch(_) {}
  })();
})();


