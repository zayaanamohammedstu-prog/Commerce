/* ── Global Chart.js defaults ─────────────────────────────── */
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(255,255,255,.08)';
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

const PALETTE = [
  '#38bdf8', '#818cf8', '#4ade80', '#fbbf24', '#f87171',
  '#a78bfa', '#34d399', '#fb923c', '#60a5fa', '#e879f9',
];

/* ── Utility: format currency ─────────────────────────────── */
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

/* ── ETL ──────────────────────────────────────────────────── */
const runEtlBtn = document.getElementById('runEtlBtn');
const etlBanner = document.getElementById('etlBanner');

function showBanner(msg, type) {
  etlBanner.textContent = msg;
  etlBanner.className = `banner ${type}`;
  etlBanner.classList.remove('hidden');
  if (type !== 'info') setTimeout(() => etlBanner.classList.add('hidden'), 5000);
}

runEtlBtn.addEventListener('click', async () => {
  runEtlBtn.disabled = true;
  showBanner('⏳ Running ETL pipeline…', 'info');
  try {
    const res = await fetch('/api/etl/run', { method: 'POST' });
    const data = await res.json();
    if (res.ok && data.status === 'success') {
      const r = data.rows_loaded;
      showBanner(
        `✅ ETL complete — Products: ${r.products}, Customers: ${r.customers}, Dates: ${r.dates}, Sales: ${r.sales}`,
        'success'
      );
      loadAll();
    } else {
      showBanner('❌ ETL error: ' + (data.message || 'unknown'), 'error');
    }
  } catch (e) {
    showBanner('❌ Network error: ' + e.message, 'error');
  } finally {
    runEtlBtn.disabled = false;
  }
});

/* ── Chart instances (kept so we can destroy on reload) ───── */
let salesChart, topProductsChart, categoryChart,
    regionChart, regionOrdersChart, forecastChart;

function destroyChart(ch) { if (ch) ch.destroy(); }

/* ── KPIs ─────────────────────────────────────────────────── */
async function loadKPIs() {
  try {
    const d = await fetch('/api/kpis').then(r => r.json());
    document.getElementById('kpiRevenue').textContent    = fmtCurrency(d.total_revenue);
    document.getElementById('kpiOrders').textContent     = fmtNumber(d.total_orders);
    document.getElementById('kpiUnits').textContent      = fmtNumber(d.total_units_sold);
    document.getElementById('kpiCustomers').textContent  = fmtNumber(d.unique_customers);
    document.getElementById('kpiAvgOrder').textContent   = fmtCurrency(d.avg_order_value);
    document.getElementById('kpiProducts').textContent   = fmtNumber(d.unique_products);
  } catch (_) {}
}

/* ── Sales Trend ──────────────────────────────────────────── */
async function loadSalesChart() {
  const gran = document.getElementById('granularity').value;
  try {
    const data = await fetch(`/api/sales/timeseries?granularity=${gran}`).then(r => r.json());
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
            backgroundColor: 'rgba(56,189,248,.7)',
            borderColor: '#38bdf8',
            borderWidth: 1,
            yAxisID: 'y',
            order: 2,
          },
          {
            label: 'Units Sold',
            data: units,
            type: 'line',
            borderColor: '#818cf8',
            backgroundColor: 'rgba(129,140,248,.15)',
            tension: .4,
            pointRadius: 3,
            yAxisID: 'y1',
            order: 1,
          },
        ],
      },
      options: {
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        scales: {
          y:  { position: 'left',  title: { display: true, text: 'Revenue ($)' } },
          y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Units' } },
        },
        plugins: { legend: { position: 'top' } },
      },
    });
  } catch (_) {}
}

document.getElementById('granularity').addEventListener('change', loadSalesChart);

/* ── Top Products ─────────────────────────────────────────── */
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
          backgroundColor: PALETTE,
          borderRadius: 6,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        plugins: { legend: { display: false } },
        scales: { x: { title: { display: true, text: 'Revenue ($)' } } },
      },
    });
  } catch (_) {}
}

/* ── Category Doughnut ────────────────────────────────────── */
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
          backgroundColor: PALETTE,
          hoverOffset: 12,
        }],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label: ctx => ` ${fmtCurrency(ctx.parsed)}`,
            },
          },
        },
      },
    });
  } catch (_) {}
}

/* ── Region Charts ────────────────────────────────────────── */
async function loadRegionCharts() {
  try {
    const data = await fetch('/api/sales/region').then(r => r.json());
    const labels = data.map(d => d.region);

    destroyChart(regionChart);
    regionChart = new Chart(document.getElementById('regionChart'), {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Revenue ($)',
          data: data.map(d => d.revenue),
          backgroundColor: PALETTE,
          borderRadius: 6,
        }],
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: { y: { title: { display: true, text: 'Revenue ($)' } } },
      },
    });

    destroyChart(regionOrdersChart);
    regionOrdersChart = new Chart(document.getElementById('regionOrdersChart'), {
      type: 'pie',
      data: {
        labels,
        datasets: [{
          data: data.map(d => d.order_count),
          backgroundColor: PALETTE,
          hoverOffset: 10,
        }],
      },
      options: {
        responsive: true,
        plugins: { legend: { position: 'bottom' } },
      },
    });
  } catch (_) {}
}

/* ── Forecast ─────────────────────────────────────────────── */
async function loadForecast() {
  const horizon = document.getElementById('forecastHorizon').value;
  try {
    const [forecastData, summary] = await Promise.all([
      fetch(`/api/forecast?horizon=${horizon}`).then(r => r.json()),
      fetch('/api/forecast/summary').then(r => r.json()),
    ]);

    // Model meta
    const meta = document.getElementById('forecastMeta');
    if (summary.status === 'ready') {
      meta.innerHTML =
        `<span><b>Algorithm:</b> ${summary.algorithm}</span>` +
        `<span><b>Training period:</b> ${summary.training_start} → ${summary.training_end}</span>` +
        `<span><b>Mean daily revenue:</b> ${fmtCurrency(summary.mean_daily_revenue)}</span>` +
        `<span><b>Std dev:</b> ${fmtCurrency(summary.std_daily_revenue)}</span>`;
    } else {
      meta.innerHTML = '<span>No training data available — run ETL first.</span>';
    }

    destroyChart(forecastChart);
    forecastChart = new Chart(document.getElementById('forecastChart'), {
      type: 'line',
      data: {
        labels: forecastData.map(d => d.date),
        datasets: [{
          label: 'Forecasted Revenue ($)',
          data: forecastData.map(d => d.forecast),
          borderColor: '#4ade80',
          backgroundColor: 'rgba(74,222,128,.12)',
          tension: .4,
          fill: true,
          pointRadius: 4,
          pointHoverRadius: 7,
        }],
      },
      options: {
        responsive: true,
        scales: {
          x: { ticks: { maxTicksLimit: 15 } },
          y: { title: { display: true, text: 'Revenue ($)' } },
        },
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: ctx => ` ${fmtCurrency(ctx.parsed.y)}`,
            },
          },
        },
      },
    });
  } catch (_) {}
}

document.getElementById('forecastHorizon').addEventListener('change', loadForecast);

/* ── Load everything ──────────────────────────────────────── */
function loadAll() {
  loadKPIs();
  loadSalesChart();
  loadTopProducts();
  loadCategoryChart();
  loadRegionCharts();
  loadForecast();
}

loadAll();
