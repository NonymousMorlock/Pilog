(function() {
  try {
    const preferDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const saved = localStorage.getItem('theme');
    const initial = saved || (preferDark ? 'dark' : 'light');
    document.documentElement.setAttribute('data-theme', initial);
    const toggle = document.getElementById('themeToggle');
    if (toggle) {
      toggle.setAttribute('aria-pressed', String(initial === 'dark'));
      toggle.addEventListener('click', () => {
        const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('theme', next);
        toggle.setAttribute('aria-pressed', String(next === 'dark'));
      });
    }
  } catch(_) {}

  window.showToast = function(message, kind) {
    try {
      const container = document.getElementById('toastContainer');
      if (!container) return;
      const div = document.createElement('div');
      div.className = 'toast ' + (kind || '');
      div.textContent = String(message || '');
      container.appendChild(div);
      setTimeout(() => { try { container.removeChild(div); } catch(_){} }, 3500);
    } catch(_) {}
  };
})();


