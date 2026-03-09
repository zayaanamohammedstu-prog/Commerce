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
          y:  { position: 'left',  title: { display: true, text: 'Revenue ($)', color: '#4e5b70' },
                grid: { color: 'rgba(255,255,255,.04)' } },
          y1: { position: 'right', title: { display: true, text: 'Units', color: '#4e5b70' },
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
          x: { grid: { color: 'rgba(255,255,255,.04)' },
               title: { display: true, text: 'Revenue ($)', color: '#4e5b70' } },
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
          y: { grid: { color: 'rgba(255,255,255,.04)' },
               title: { display: true, text: 'Revenue ($)', color: '#4e5b70' } },
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
          r: { grid: { color: 'rgba(255,255,255,.06)' }, ticks: { display: false } },
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
          y: { grid: { color: 'rgba(255,255,255,.04)' },
               title: { display: true, text: 'Revenue ($)', color: '#4e5b70' } },
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
