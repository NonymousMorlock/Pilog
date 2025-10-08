(function(){
  const charts = {};
  let allLandings = [];
  let allLinks = {};
  let landingPage = 0;
  const landingPageSize = 50;
  let sortKey = null;
  let sortDir = 'asc';
  let searchQuery = '';
  let highlightedRowPos = null; // sticky physical row index within current page

  function createChart(id, type, labels, values, color, opts) {
    if (charts[id]) { try { charts[id].destroy(); } catch(_) {} }
    const horizontal = opts && opts.horizontal;
    const xTitle = opts && opts.xTitle ? String(opts.xTitle) : '';
    const yTitle = opts && opts.yTitle ? String(opts.yTitle) : '';
    const datasetLabel = opts && opts.datasetLabel ? String(opts.datasetLabel) : '';
    charts[id] = new Chart(document.getElementById(id), {
      type,
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

  function bandClass(vs) {
    if (vs == null || isNaN(vs)) return '';
    if (vs >= -200) return 'ok';
    if (vs >= -400) return 'warn';
    return 'bad';
  }

  function getFilteredSortedLandingIndices() {
    const n = Array.isArray(allLandings) ? allLandings.length : 0;
    let indices = Array.from({ length: n }, (_, i) => i);
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      indices = indices.filter(i => {
        const l = allLandings[i] || {};
        return `${l.date} ${l.aircraft} ${l.norm_ac}`.toLowerCase().includes(q);
      });
    }
    if (sortKey) {
      indices.sort((ia, ib) => {
        const a = allLandings[ia] || {};
        const b = allLandings[ib] || {};
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

  function renderTable() {
    const tbody = document.getElementById('landingTbody');
    tbody.innerHTML = '';
    const order = getFilteredSortedLandingIndices();
    const start = landingPage * landingPageSize;
    const end = Math.min(start + landingPageSize, order.length);
    for (let i = start; i < end; i++) {
      const origIdx = order[i];
      const l = allLandings[origIdx];
      const link = allLinks && allLinks[origIdx];
      const tr = document.createElement('tr');
      tr.id = 'landing-row-' + origIdx;
      const vs = Number(l.VS);
      const cls = bandClass(vs);
      let linkHtml = '<span class="chip muted">unlinked</span>';
      if (link && link.linkConfidence && link.linkConfidence !== 'unmatched' && link.linkConfidence !== 'ambiguous' && link.flight) {
        const f = link.flight;
        const href = '/?date=' + encodeURIComponent(f.date) + (link.flightIndex != null ? ('&flightIndex=' + link.flightIndex) : '');
        const badge = link.linkConfidence === 'sequence-assumed' ? 'chip warn' : 'chip ok';
        linkHtml = `<a class="${badge}" href="${href}">View flight ${esc(f.dep)}→${esc(f.arr)}</a>`;
      } else if (link && link.linkConfidence) {
        const badge = link.linkConfidence === 'ambiguous' ? 'chip warn' : 'chip bad';
        linkHtml = `<span class="${badge}">${esc(link.linkConfidence)}</span>`;
      }
      tr.innerHTML = `
        <td>${esc(l.time)}</td>
        <td>${esc(l.norm_ac || l.aircraft)}</td>
        <td><span class="chip ${cls}">${isFinite(vs) ? vs.toFixed(1) : ''}</span></td>
        <td>${l.G != null ? Number(l.G).toFixed(2) : ''}</td>
        <td>${l.nose_rate != null ? Number(l.nose_rate).toFixed(2) : ''}</td>
        <td>${l.float != null ? Number(l.float).toFixed(2) : ''}</td>
        <td>${esc(l.quality || '')}</td>
        <td>${linkHtml}</td>
      `;
      // Apply sticky highlight to the same physical row position
      const localRow = i - start;
      if (highlightedRowPos != null && localRow === highlightedRowPos) {
        tr.style.outline = '2px solid var(--link)';
      }
      tbody.appendChild(tr);
    }
    const info = document.getElementById('landingPageInfo');
    if (info) { const totalPages = Math.max(1, Math.ceil(order.length / landingPageSize)); info.textContent = `Page ${Math.min(landingPage + 1, totalPages)} of ${totalPages}`; }
    if (end - start === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 8;
      td.className = 'muted';
      td.textContent = searchQuery ? 'No matching landings.' : 'No landings to display.';
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
  }

  function renderSummary(summary, landings) {
    const vs = landings.map(l => Number(l.VS)).filter(v => isFinite(v));
    const min = Math.min(...vs, -1500), max = Math.max(...vs, -10);
    const bins = 30;
    const width = (max - min) / bins || 1;
    const counts = new Array(bins).fill(0);
    vs.forEach(v => { const idx = Math.min(bins - 1, Math.max(0, Math.floor((v - min) / width))); counts[idx]++; });
    const labels = counts.map((_, i) => (min + i * width).toFixed(0));
    createChart('hist', 'bar', labels, counts, '#2563eb', { datasetLabel: 'Count', xTitle: 'VS bin (fpm)', yTitle: 'Landings' });
    const avg = summary && summary.avg_vs_per_aircraft ? summary.avg_vs_per_aircraft : {};
    createChart('avgByAc', 'bar', Object.keys(avg), Object.values(avg), '#d97706', { datasetLabel: 'Avg VS', xTitle: 'Aircraft', yTitle: 'VS (fpm)' });
  }

  window.chooseLRFolder = async function chooseLRFolder() {
    try {
      const res = await fetch('/pick_landing_rate_folder', { method: 'POST' });
      const json = await res.json();
      if (!res.ok) { showToast(json.error || 'Failed to pick folder', 'error'); return; }
      const form = new FormData();
      form.append('folder_path', json.folder_path);
      const sres = await fetch('/set_landing_rate_folder', { method: 'POST', body: form });
      const sjson = await sres.json();
      if (!sres.ok) { showToast(sjson.error || 'Failed to set folder', 'error'); return; }
      showToast('Landing rates folder set', 'success');
    } catch (e) { showToast('Error: ' + e, 'error'); }
  }

  window.chooseLRFile = async function chooseLRFile() {
    try {
      const res = await fetch('/pick_landing_rate_file', { method: 'POST' });
      const json = await res.json();
      if (!res.ok) { showToast(json.error || 'Failed to pick file', 'error'); return; }
      const form = new FormData();
      form.append('file_path', json.file_path);
      const sres = await fetch('/set_landing_rate_file', { method: 'POST', body: form });
      const sjson = await sres.json();
      if (!sres.ok) { showToast(sjson.error || 'Failed to set file', 'error'); return; }
      showToast('Landing rates file set', 'success');
    } catch (e) { showToast('Error: ' + e, 'error'); }
  }

  // Initial data from server
  const initialSummaryEl = document.getElementById('initialSummary');
  const initialLandingsEl = document.getElementById('initialLandings');
  const initialLinksEl = document.getElementById('initialLinks');
  const initialSummary = initialSummaryEl ? JSON.parse(initialSummaryEl.textContent || '{}') : {};
  allLandings = initialLandingsEl ? JSON.parse(initialLandingsEl.textContent || '[]') : [];
  allLinks = initialLinksEl ? JSON.parse(initialLinksEl.textContent || '{}') : {};
  renderSummary(initialSummary, allLandings);
  renderTable();

  // Deep-link highlight before sockets (persist across refresh)
  (function() {
    const params = new URLSearchParams(window.location.search);
    const qIndex = params.get('landingIndex');
    if (qIndex != null) {
      const origIdx = Number(qIndex);
      const order = getFilteredSortedLandingIndices();
      const pos = order.indexOf(origIdx);
      if (pos >= 0) {
        landingPage = Math.floor(pos / landingPageSize);
        highlightedRowPos = pos % landingPageSize;
      }
      renderTable();
      try {
        const tbody = document.getElementById('landingTbody');
        if (tbody && highlightedRowPos != null) {
          const trs = tbody.querySelectorAll('tr');
          const el = trs[highlightedRowPos];
          if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
        }
      } catch(_) {}
      return;
    }
    const qDate = params.get('date');
    const qAc = (params.get('aircraft') || '').toUpperCase();
    if (qDate && qAc) {
      for (let i = 0; i < allLandings.length; i++) {
        const l = allLandings[i];
        const ac = ((l.norm_ac || l.aircraft) || '').toUpperCase();
        if (l.date === qDate && ac === qAc) {
          const order = getFilteredSortedLandingIndices();
          const pos = order.indexOf(i);
          if (pos >= 0) {
            landingPage = Math.floor(pos / landingPageSize);
            highlightedRowPos = pos % landingPageSize;
          }
          renderTable();
          try {
            const tbody = document.getElementById('landingTbody');
            if (tbody && highlightedRowPos != null) {
              const trs = tbody.querySelectorAll('tr');
              const el = trs[highlightedRowPos];
              if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
            }
          } catch(_) {}
          break;
        }
      }
    }
  })();

  const socket = io();
  socket.on('landing_rate_update', (payload) => {
    if (!payload) return;
    if (payload.source) {
      if (payload.source.folder) document.getElementById('srcFolder').textContent = payload.source.folder || '';
      if (payload.source.file) document.getElementById('srcFile').textContent = payload.source.file || '';
    }
    if (payload.summary && payload.landings) {
      allLandings = payload.landings;
      allLinks = payload.links || {};
      renderSummary(payload.summary, allLandings);
      const order = getFilteredSortedLandingIndices();
      const totalPages = Math.max(1, Math.ceil(order.length / landingPageSize));
      if (landingPage >= totalPages) landingPage = totalPages - 1;
      renderTable();
      const params = new URLSearchParams(window.location.search);
      const qIndex = params.get('landingIndex');
      if (qIndex != null) {
        const origIdx = Number(qIndex);
        const pos = order.indexOf(origIdx);
        if (pos >= 0) {
          landingPage = Math.floor(pos / landingPageSize);
          highlightedRowPos = pos % landingPageSize;
        }
        renderTable();
        try {
          const tbody = document.getElementById('landingTbody');
          if (tbody && highlightedRowPos != null) {
            const trs = tbody.querySelectorAll('tr');
            const el = trs[highlightedRowPos];
            if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); return; }
          }
        } catch(_) {}
      }
      const qDate = params.get('date');
      const qAc = (params.get('aircraft') || '').toUpperCase();
      if (qDate && qAc) {
        for (let i = 0; i < payload.landings.length; i++) {
          const l = payload.landings[i];
          const ac = ((l.norm_ac || l.aircraft) || '').toUpperCase();
          if (l.date === qDate && ac === qAc) {
            const order2 = getFilteredSortedLandingIndices();
            const pos2 = order2.indexOf(i);
            if (pos2 >= 0) {
              landingPage = Math.floor(pos2 / landingPageSize);
              highlightedRowPos = pos2 % landingPageSize;
            }
            renderTable();
            try {
              const tbody = document.getElementById('landingTbody');
              if (tbody && highlightedRowPos != null) {
                const trs = tbody.querySelectorAll('tr');
                const el = trs[highlightedRowPos];
                if (el) { el.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
              }
            } catch(_) {}
            break;
          }
        }
      }
    }
  });

  // Pager buttons
  const prevBtn = document.getElementById('prevLandingPage');
  const nextBtn = document.getElementById('nextLandingPage');
  if (prevBtn) prevBtn.onclick = () => { if (landingPage > 0) { landingPage--; renderTable(); const info = document.getElementById('landingPageInfo'); if (info) { const order = getFilteredSortedLandingIndices(); const totalPages = Math.max(1, Math.ceil(order.length / landingPageSize)); info.textContent = `Page ${Math.min(landingPage + 1, totalPages)} of ${totalPages}`; } } };
  if (nextBtn) nextBtn.onclick = () => { const order = getFilteredSortedLandingIndices(); const totalPages = Math.max(1, Math.ceil(order.length / landingPageSize)); if (landingPage + 1 < totalPages) { landingPage++; renderTable(); const info = document.getElementById('landingPageInfo'); if (info) { info.textContent = `Page ${Math.min(landingPage + 1, totalPages)} of ${totalPages}`; } } };

  // Sorting
  (function(){
    const thead = document.querySelector('table thead');
    if (!thead) return;
    thead.addEventListener('click', (e) => {
      const th = e.target.closest('th[data-sort-key]');
      if (!th) return;
      const key = th.getAttribute('data-sort-key');
      if (sortKey === key) { sortDir = sortDir === 'asc' ? 'desc' : 'asc'; } else { sortKey = key; sortDir = 'asc'; }
      document.querySelectorAll('th[data-sort-key]').forEach(x => x.setAttribute('aria-sort', 'none'));
      th.setAttribute('aria-sort', sortDir === 'asc' ? 'ascending' : 'descending');
      landingPage = 0;
      renderTable();
    });
  })();

  // Search
  (function(){
    const input = document.getElementById('landingSearch');
    if (!input) return;
    input.addEventListener('input', () => { searchQuery = input.value || ''; landingPage = 0; renderTable(); });
  })();

  // Initial pager text
  (function(){ const info = document.getElementById('landingPageInfo'); if (info) { const order = getFilteredSortedLandingIndices(); const totalPages = Math.max(1, Math.ceil(order.length / landingPageSize)); info.textContent = `Page ${Math.min(landingPage + 1, totalPages)} of ${totalPages}`; } })();
})();


