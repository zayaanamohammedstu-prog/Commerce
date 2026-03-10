/* ── Global Chart.js defaults ─────────────────────────────── */
Chart.defaults.color = '#4e5b70';
Chart.defaults.borderColor = 'rgba(255,255,255,.055)';
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

const C = {
  primary:   '#00d4aa',
  secondary: '#ff6b35',
  violet:    '#9b8ffc',
  yellow:    '#fbbf24',
  green:     '#22c55e',
  pink:      '#ec4899',
  blue:      '#38bdf8',
  lime:      '#84cc16',
  rose:      '#fb7185',
  amber:     '#f59e0b',
};
const PALETTE = Object.values(C);

/* ── Theme helpers ───────────────────────────────────────── */
function isDark() {
  return document.documentElement.getAttribute('data-theme') !== 'light';
}
function chartGridColor() {
  return isDark() ? 'rgba(255,255,255,.04)' : 'rgba(0,0,0,.06)';
}
function chartGridColorR() {
  return isDark() ? 'rgba(255,255,255,.06)' : 'rgba(0,0,0,.07)';
}
function chartTextColor() {
  return isDark() ? '#4e5b70' : '#64748b';
}
function applyChartDefaults() {
  Chart.defaults.color = chartTextColor();
  Chart.defaults.borderColor = isDark() ? 'rgba(255,255,255,.055)' : 'rgba(0,0,0,.08)';
}

/* ── Theme Toggle ────────────────────────────────────────── */
const html        = document.documentElement;
const themeToggle = document.getElementById('themeToggle');

function applyTheme(theme) {
  html.setAttribute('data-theme', theme);
  localStorage.setItem('meridian-theme', theme);
  themeToggle.title = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
  applyChartDefaults();
}

const savedTheme = localStorage.getItem('meridian-theme') || 'dark';
applyTheme(savedTheme);

themeToggle.addEventListener('click', () => {
  applyTheme(isDark() ? 'light' : 'dark');
  loadAll();
});

/* ── Utilities ───────────────────────────────────────────── */
function fmtCurrency(v) {
  if (v == null || isNaN(v)) return '—';
  if (v >= 1_000_000) return '$' + (v / 1_000_000).toFixed(2) + 'M';
  if (v >= 1_000)     return '$' + (v / 1_000).toFixed(1) + 'K';
  return '$' + Number(v).toFixed(2);
}
function fmtNumber(v) {
  if (v == null || isNaN(v)) return '—';
  return Number(v).toLocaleString();
}

/* ── Live Clock ──────────────────────────────────────────── */
const clockEl = document.getElementById('sidebarClock');
function updateClock() {
  clockEl.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
}
updateClock();
setInterval(updateClock, 1000);

/* ── Mobile Sidebar Toggle ───────────────────────────────── */
const sidebar    = document.getElementById('sidebar');
const menuToggle = document.getElementById('menuToggle');
menuToggle.addEventListener('click', () => sidebar.classList.toggle('open'));
document.addEventListener('click', e => {
  if (!sidebar.contains(e.target) && !menuToggle.contains(e.target)) {
    sidebar.classList.remove('open');
  }
});

/* ── Scrollspy ───────────────────────────────────────────── */
const sections = document.querySelectorAll('.section');
const navLinks = document.querySelectorAll('.nav-link');
const bcPage   = document.getElementById('bcPage');

const spy = new IntersectionObserver(entries => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      const id = entry.target.id;
      navLinks.forEach(l => {
        const active = l.getAttribute('href') === '#' + id;
        l.classList.toggle('active', active);
        if (active && bcPage) bcPage.textContent = l.dataset.label;
      });
    }
  });
}, { threshold: 0.25, rootMargin: '-56px 0px -40% 0px' });
sections.forEach(s => spy.observe(s));

/* ── ETL ──────────────────────────────────────────────────── */
const runEtlBtn = document.getElementById('runEtlBtn');
const etlBanner = document.getElementById('etlBanner');

function showBanner(msg, type) {
  etlBanner.textContent = msg;
  etlBanner.className = `etl-banner ${type}`;
  etlBanner.classList.remove('hidden');
  if (type !== 'info') setTimeout(() => etlBanner.classList.add('hidden'), 5000);
}

runEtlBtn.addEventListener('click', async () => {
  runEtlBtn.disabled = true;
  showBanner('Syncing data pipeline…', 'info');
  try {
    const res  = await fetch('/api/etl/run', { method: 'POST' });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      const r = data.rows_loaded;
      showBanner(
        `Sync complete · Products ${r.products} · Customers ${r.customers} · Sales ${r.sales}`,
        'success'
      );
      loadAll();
    } else {
      showBanner('Sync error: ' + (data.message || 'unknown'), 'error');
    }
  } catch (e) {
    showBanner('Network error: ' + e.message, 'error');
  } finally {
    runEtlBtn.disabled = false;
  }
});

/* ── Chart instances ─────────────────────────────────────── */
let salesChart, topProductsChart, categoryChart,
    regionChart, regionOrdersChart, forecastChart;
function destroyChart(ch) { if (ch) ch.destroy(); }

/* ── KPIs ─────────────────────────────────────────────────── */
async function loadKPIs() {
  try {
    const d = await fetch('/api/kpis').then(r => r.json());
    document.getElementById('kpiRevenue').textContent   = fmtCurrency(d.total_revenue);
    document.getElementById('kpiOrders').textContent    = fmtNumber(d.total_orders);
    document.getElementById('kpiUnits').textContent     = fmtNumber(d.total_units_sold);
    document.getElementById('kpiCustomers').textContent = fmtNumber(d.unique_customers);
    document.getElementById('kpiAvgOrder').textContent  = fmtCurrency(d.avg_order_value);
    document.getElementById('kpiProducts').textContent  = fmtNumber(d.unique_products);
  } catch (_) {}
}

/* ── Sales Chart ─────────────────────────────────────────── */
let currentGran = 'monthly';

async function loadSalesChart() {
  try {
    const data    = await fetch(`/api/sales/timeseries?granularity=${currentGran}`).then(r => r.json());
    const labels  = data.map(d => d.period);
    const revenue = data.map(d => d.revenue);
    const units   = data.map(d => d.units);

    destroyChart(salesChart);
    salesChart = new Chart(document.getElementById('salesChart'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'Revenue ($)',
            data: revenue,
            backgroundColor: 'rgba(0,212,170,.5)',
            borderColor: C.primary,
            borderWidth: 1,
            borderRadius: 4,
            yAxisID: 'y',
            order: 2,
          },
          {
            label: 'Units Sold',
            data: units,
            type: 'line',
            borderColor: C.secondary,
            backgroundColor: 'rgba(255,107,53,.1)',
            tension: .45,
            pointRadius: 2,
            pointHoverRadius: 5,
            pointBackgroundColor: C.secondary,
            fill: false,
            yAxisID: 'y1',
            order: 1,
          },
        ],
      },
      options: {
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x:  { grid: { display: false } },
          y:  { position: 'left',  title: { display: true, text: 'Revenue ($)', color: chartTextColor() },
                grid: { color: chartGridColor() } },
          y1: { position: 'right', title: { display: true, text: 'Units', color: chartTextColor() },
                grid: { drawOnChartArea: false } },
        },
        plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } } },
      },
    });
  } catch (_) {}
}

document.getElementById('granGroup').addEventListener('click', e => {
  const btn = e.target.closest('.tgl');
  if (!btn) return;
  document.querySelectorAll('#granGroup .tgl').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentGran = btn.dataset.val;
  loadSalesChart();
});

/* ── Top Products ────────────────────────────────────────── */
async function loadTopProducts() {
  try {
    const data = await fetch('/api/products/top?limit=10').then(r => r.json());
    destroyChart(topProductsChart);
    topProductsChart = new Chart(document.getElementById('topProductsChart'), {
      type: 'bar',
      data: {
        labels: data.map(d => d.name),
        datasets: [{
          label: 'Revenue ($)',
          data: data.map(d => d.revenue),
          backgroundColor: PALETTE.slice(0, 10),
          borderRadius: 5,
          borderSkipped: false,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: chartGridColor() },
               title: { display: true, text: 'Revenue ($)', color: chartTextColor() } },
          y: { grid: { display: false } },
        },
      },
    });
  } catch (_) {}
}

/* ── Category Doughnut ───────────────────────────────────── */
async function loadCategoryChart() {
  try {
    const data = await fetch('/api/sales/category').then(r => r.json());
    destroyChart(categoryChart);
    categoryChart = new Chart(document.getElementById('categoryChart'), {
      type: 'doughnut',
      data: {
        labels: data.map(d => d.category),
        datasets: [{
          data: data.map(d => d.revenue),
          backgroundColor: [C.primary, C.secondary, C.violet, C.yellow, C.green],
          borderColor: 'transparent',
          hoverOffset: 14,
        }],
      },
      options: {
        responsive: true,
        cutout: '72%',
        plugins: {
          legend: { position: 'bottom', labels: { boxWidth: 10, padding: 14 } },
          tooltip: { callbacks: { label: ctx => ` ${fmtCurrency(ctx.parsed)}` } },
        },
      },
    });
  } catch (_) {}
}

/* ── Region Charts ───────────────────────────────────────── */
async function loadRegionCharts() {
  try {
    const data   = await fetch('/api/sales/region').then(r => r.json());
    const labels = data.map(d => d.region);

    destroyChart(regionChart);
    regionChart = new Chart(document.getElementById('regionChart'), {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Revenue ($)',
          data: data.map(d => d.revenue),
          backgroundColor: PALETTE.slice(0, labels.length),
          borderRadius: 5,
          borderSkipped: false,
        }],
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false } },
          y: { grid: { color: chartGridColor() },
               title: { display: true, text: 'Revenue ($)', color: chartTextColor() } },
        },
      },
    });

    destroyChart(regionOrdersChart);
    regionOrdersChart = new Chart(document.getElementById('regionOrdersChart'), {
      type: 'polarArea',
      data: {
        labels,
        datasets: [{
          data: data.map(d => d.order_count),
          backgroundColor: PALETTE.slice(0, labels.length).map(c => c + '99'),
          borderColor: PALETTE.slice(0, labels.length),
          borderWidth: 1,
        }],
      },
      options: {
        responsive: true,
        plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, padding: 12 } } },
        scales: {
          r: { grid: { color: chartGridColorR() }, ticks: { display: false } },
        },
      },
    });
  } catch (_) {}
}

/* ── Forecast ────────────────────────────────────────────── */
let currentHorizon = '30';

async function loadForecast() {
  try {
    const [forecastData, summary] = await Promise.all([
      fetch(`/api/forecast?horizon=${currentHorizon}`).then(r => r.json()),
      fetch('/api/forecast/summary').then(r => r.json()),
    ]);

    const ribbon = document.getElementById('forecastMeta');
    if (summary.status === 'ready') {
      ribbon.innerHTML = `
        <div class="frib-item">
          <span class="frib-label">Algorithm</span>
          <span class="frib-value">${summary.algorithm}</span>
        </div>
        <div class="frib-item">
          <span class="frib-label">Training Start</span>
          <span class="frib-value">${summary.training_start}</span>
        </div>
        <div class="frib-item">
          <span class="frib-label">Training End</span>
          <span class="frib-value">${summary.training_end}</span>
        </div>
        <div class="frib-item">
          <span class="frib-label">Mean Daily Rev</span>
          <span class="frib-value">${fmtCurrency(summary.mean_daily_revenue)}</span>
        </div>
        <div class="frib-item">
          <span class="frib-label">Std Deviation</span>
          <span class="frib-value">${fmtCurrency(summary.std_daily_revenue)}</span>
        </div>
      `;
    } else {
      ribbon.innerHTML =
        '<div class="frib-item"><span class="frib-value">Run ETL to load training data</span></div>';
    }

    destroyChart(forecastChart);
    forecastChart = new Chart(document.getElementById('forecastChart'), {
      type: 'line',
      data: {
        labels: forecastData.map(d => d.date),
        datasets: [{
          label: 'Forecasted Revenue',
          data: forecastData.map(d => d.forecast),
          borderColor: C.green,
          backgroundColor: ctx => {
            const g = ctx.chart.ctx.createLinearGradient(0, 0, 0, ctx.chart.height);
            g.addColorStop(0, 'rgba(34,197,94,.28)');
            g.addColorStop(1, 'rgba(34,197,94,0)');
            return g;
          },
          tension: .45,
          fill: true,
          pointRadius: 3,
          pointHoverRadius: 6,
          pointBackgroundColor: C.green,
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        scales: {
          x: { grid: { display: false }, ticks: { maxTicksLimit: 10 } },
          y: { grid: { color: chartGridColor() },
               title: { display: true, text: 'Revenue ($)', color: chartTextColor() } },
        },
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ` ${fmtCurrency(ctx.parsed.y)}` } },
        },
      },
    });
  } catch (_) {}
}

document.getElementById('horizonGroup').addEventListener('click', e => {
  const btn = e.target.closest('.tgl');
  if (!btn) return;
  document.querySelectorAll('#horizonGroup .tgl').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentHorizon = btn.dataset.val;
  loadForecast();
});

/* ── Load everything ─────────────────────────────────────── */
function loadAll() {
  loadKPIs();
  loadSalesChart();
  loadTopProducts();
  loadCategoryChart();
  loadRegionCharts();
  loadForecast();
}

loadAll();

/* ── Admin helpers ───────────────────────────────────────── */
const STATUS_MSG_DURATION_MS = 6000;

/* ── Admin: ETL card ─────────────────────────────────────── */
const adminEtlBtn    = document.getElementById('adminEtlBtn');
const adminEtlStatus = document.getElementById('adminEtlStatus');

function setAdminStatus(el, msg, type) {
  el.textContent = msg;
  el.className   = `admin-status ${type}`;
  el.classList.remove('hidden');
  if (type !== 'info') setTimeout(() => el.classList.add('hidden'), STATUS_MSG_DURATION_MS);
}

adminEtlBtn.addEventListener('click', async () => {
  adminEtlBtn.disabled = true;
  setAdminStatus(adminEtlStatus, 'Syncing data pipeline…', 'info');
  try {
    const res  = await fetch('/api/etl/run', { method: 'POST' });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      const r = data.rows_loaded;
      setAdminStatus(
        adminEtlStatus,
        `Sync complete · Products ${r.products} · Customers ${r.customers} · Sales ${r.sales}`,
        'success'
      );
      loadAll();
    } else {
      setAdminStatus(adminEtlStatus, 'Error: ' + (data.message || 'unknown'), 'error');
    }
  } catch (e) {
    setAdminStatus(adminEtlStatus, 'Network error: ' + e.message, 'error');
  } finally {
    adminEtlBtn.disabled = false;
  }
});

/* ── Admin: Upload prediction ────────────────────────────── */
const uploadFile      = document.getElementById('uploadFile');
const uploadFilename  = document.getElementById('uploadFilename');
const uploadSubmitBtn = document.getElementById('uploadSubmitBtn');
const uploadStatus    = document.getElementById('uploadStatus');
const uploadResults   = document.getElementById('uploadResults');
const uploadDropzone  = document.getElementById('uploadDropzone');
let   uploadHorizon   = '30';
let   uploadForecastChartInst = null;

// Horizon selector
document.getElementById('uploadHorizonGroup').addEventListener('click', e => {
  const btn = e.target.closest('.tgl');
  if (!btn) return;
  document.querySelectorAll('#uploadHorizonGroup .tgl').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  uploadHorizon = btn.dataset.val;
});

// File selection via input
uploadFile.addEventListener('change', () => {
  const file = uploadFile.files[0];
  if (file) {
    uploadFilename.textContent = file.name;
    uploadFilename.classList.remove('hidden');
    uploadSubmitBtn.disabled = false;
  } else {
    uploadFilename.classList.add('hidden');
    uploadSubmitBtn.disabled = true;
  }
});

// Drag & drop
uploadDropzone.addEventListener('dragover',  e => { e.preventDefault(); uploadDropzone.classList.add('drag-over'); });
uploadDropzone.addEventListener('dragleave', ()  => uploadDropzone.classList.remove('drag-over'));
uploadDropzone.addEventListener('drop', e => {
  e.preventDefault();
  uploadDropzone.classList.remove('drag-over');
  const files = e.dataTransfer.files;
  if (files.length > 0) {
    // Assign files to the hidden input so the change handler fires
    const dt = new DataTransfer();
    dt.items.add(files[0]);
    uploadFile.files = dt.files;
    uploadFile.dispatchEvent(new Event('change'));
  }
});

// Submit
uploadSubmitBtn.addEventListener('click', async () => {
  const file = uploadFile.files[0];
  if (!file) return;

  uploadSubmitBtn.disabled = true;
  setAdminStatus(uploadStatus, 'Uploading and generating forecast…', 'info');
  uploadResults.classList.add('hidden');

  const form = new FormData();
  form.append('file', file);
  form.append('horizon', uploadHorizon);

  try {
    const res  = await fetch('/api/admin/predict/upload', { method: 'POST', body: form });
    const data = await res.json();
    if (res.ok) {
      setAdminStatus(uploadStatus, `Forecast generated · ${data.forecast.length} days ahead`, 'success');
      renderUploadResults(data);
    } else {
      setAdminStatus(uploadStatus, 'Error: ' + (data.error || 'Unknown error'), 'error');
    }
  } catch (e) {
    setAdminStatus(uploadStatus, 'Network error: ' + e.message, 'error');
  } finally {
    uploadSubmitBtn.disabled = false;
  }
});

function renderUploadResults(data) {
  // Metadata ribbon
  document.getElementById('uploadForecastRibbon').innerHTML = `
    <div class="frib-item">
      <span class="frib-label">Training Days</span>
      <span class="frib-value">${data.training_days}</span>
    </div>
    <div class="frib-item">
      <span class="frib-label">Training Start</span>
      <span class="frib-value">${data.training_start}</span>
    </div>
    <div class="frib-item">
      <span class="frib-label">Training End</span>
      <span class="frib-value">${data.training_end}</span>
    </div>
    <div class="frib-item">
      <span class="frib-label">Mean Daily Rev</span>
      <span class="frib-value">${fmtCurrency(data.mean_daily_revenue)}</span>
    </div>
    <div class="frib-item">
      <span class="frib-label">Horizon</span>
      <span class="frib-value">${data.horizon}d</span>
    </div>
  `;

  document.getElementById('uploadResultsDesc').textContent =
    `${data.training_days} training days · ${data.horizon}-day forecast`;

  // Chart
  destroyChart(uploadForecastChartInst);
  uploadForecastChartInst = new Chart(document.getElementById('uploadForecastChart'), {
    type: 'line',
    data: {
      labels: data.forecast.map(d => d.date),
      datasets: [{
        label: 'Forecasted Revenue',
        data: data.forecast.map(d => d.forecast),
        borderColor: C.violet,
        backgroundColor: ctx => {
          const g = ctx.chart.ctx.createLinearGradient(0, 0, 0, ctx.chart.height);
          g.addColorStop(0, 'rgba(155,143,252,.28)');
          g.addColorStop(1, 'rgba(155,143,252,0)');
          return g;
        },
        tension: .45,
        fill: true,
        pointRadius: 3,
        pointHoverRadius: 6,
        pointBackgroundColor: C.violet,
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 8 } },
        y: {
          grid:  { color: chartGridColor() },
          title: { display: true, text: 'Revenue ($)', color: chartTextColor() },
        },
      },
      plugins: {
        legend:  { display: false },
        tooltip: { callbacks: { label: ctx => ` ${fmtCurrency(ctx.parsed.y)}` } },
      },
    },
  });

  // Table
  const rows = data.forecast.map(d =>
    `<tr><td>${d.date}</td><td>${fmtCurrency(d.forecast)}</td></tr>`
  ).join('');
  document.getElementById('uploadForecastTable').innerHTML = `
    <table class="forecast-table">
      <thead><tr><th>Date</th><th>Forecast</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  uploadResults.classList.remove('hidden');
  uploadResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
}
