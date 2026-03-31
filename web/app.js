const SOURCES = {
  realtor: {
    label: "Realtor.ca",
    base: "../data/derived/realtor_ca/",
    path: "data/derived/realtor_ca/",
    type: "realtor",
    region: "BC Interior / Kootenays",
    subtitle:
      "Active inventory, sold momentum, and pricing stress indicators from Realtor.ca snapshots.",
  },
  remax: {
    label: "RE/MAX",
    base: "../data/derived/remax_ca/kootenay_active/",
    path: "data/derived/remax_ca/kootenay_active/",
    type: "remax",
    region: "Kootenays (RE/MAX active)",
    subtitle: "Active-only inventory and listing trends from RE/MAX Canada.",
  },
};

const MACRO_BASE = "../data/derived/macro/";

const numberFormat = new Intl.NumberFormat("en-CA");
const currencyFormat = new Intl.NumberFormat("en-CA", {
  style: "currency",
  currency: "CAD",
  maximumFractionDigits: 0,
});
const ratioFormat = new Intl.NumberFormat("en-CA", {
  maximumFractionDigits: 2,
});

const toCurrency = (value) =>
  value === null || value === undefined ? "—" : currencyFormat.format(value);
const toNumber = (value) =>
  value === null || value === undefined ? "—" : numberFormat.format(value);
const toRatio = (value) =>
  value === null || value === undefined ? "—" : ratioFormat.format(value);
const toRate = (value) =>
  value === null || value === undefined ? "—" : `${Number(value).toFixed(2)}%`;
const toPercent = (value) =>
  value === null || value === undefined ? "—" : `${(value * 100).toFixed(1)}%`;

const fetchJson = async (path) => {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}`);
  }
  return response.json();
};

const safeFetchJson = async (path) => {
  try {
    return await fetchJson(path);
  } catch (error) {
    return null;
  }
};

const clampTop = (items, limit = 8) => items.slice(0, limit);

const renderBars = (container, items, limit = 8) => {
  container.innerHTML = "";
  const trimmed = clampTop(items, limit);
  const maxValue = Math.max(...trimmed.map((item) => item.count), 1);

  trimmed.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";

    const label = document.createElement("div");
    label.className = "bar-label";
    label.textContent = item.key || "Unknown";

    const value = document.createElement("div");
    value.className = "bar-value";
    value.textContent = toNumber(item.count);

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${(item.count / maxValue) * 100}%`;

    track.appendChild(fill);
    row.appendChild(label);
    row.appendChild(value);
    row.appendChild(track);
    container.appendChild(row);
  });
};

const renderMetrics = (container, metrics) => {
  container.innerHTML = "";
  metrics.forEach((metric) => {
    const card = document.createElement("div");
    card.className = "metric";
    card.innerHTML = `
      <div class="value">${metric.value}</div>
      <div class="label">${metric.label}</div>
    `;
    container.appendChild(card);
  });
};

const renderSeriesList = (container, items, labelKey, valueKey, formatter) => {
  container.innerHTML = "";
  items.slice(-12).forEach((item) => {
    const row = document.createElement("div");
    row.className = "series-item";
    row.innerHTML = `
      <span>${item[labelKey]}</span>
      <span>${formatter(item[valueKey])}</span>
    `;
    container.appendChild(row);
  });
};

const buildSparkline = (values, { width = 320, height = 120, padding = 8 } = {}) => {
  const clean = values.filter((value) => value !== null && value !== undefined);
  if (!clean.length) {
    return "";
  }
  const min = Math.min(...clean);
  const max = Math.max(...clean);
  const range = max - min || 1;
  const points = values.map((value, index) => {
    if (value === null || value === undefined) {
      return null;
    }
    const x = padding + (index / (values.length - 1 || 1)) * (width - padding * 2);
    const y = height - padding - ((value - min) / range) * (height - padding * 2);
    return `${x},${y}`;
  });
  return points.filter(Boolean).join(" ");
};

const renderSparkline = (container, values, color) => {
  const points = buildSparkline(values);
  if (!points) {
    container.innerHTML = "<p class=\"muted\">No data</p>";
    return;
  }
  container.innerHTML = `
    <svg width="100%" height="120" viewBox="0 0 320 120" preserveAspectRatio="none">
      <polyline fill="none" stroke="${color}" stroke-width="3" points="${points}" />
      <polyline fill="none" stroke="rgba(0,0,0,0.1)" stroke-width="1" points="${points}" />
    </svg>
  `;
};

const renderDualSparkline = (container, primary, secondary) => {
  const width = 320;
  const height = 120;
  const padding = 8;
  const combined = primary.concat(secondary).filter((value) => value !== null && value !== undefined);
  if (!combined.length) {
    container.innerHTML = "<p class=\"muted\">No data</p>";
    return;
  }
  const min = Math.min(...combined);
  const max = Math.max(...combined);
  const range = max - min || 1;

  const build = (values) =>
    values
      .map((value, index) => {
        if (value === null || value === undefined) {
          return null;
        }
        const x = padding + (index / (values.length - 1 || 1)) * (width - padding * 2);
        const y = height - padding - ((value - min) / range) * (height - padding * 2);
        return `${x},${y}`;
      })
      .filter(Boolean)
      .join(" ");

  const primaryPoints = build(primary);
  const secondaryPoints = build(secondary);

  container.innerHTML = `
    <svg width="100%" height="120" viewBox="0 0 320 120" preserveAspectRatio="none">
      <polyline fill="none" stroke="var(--brick)" stroke-width="3" points="${primaryPoints}" />
      <polyline fill="none" stroke="var(--sea)" stroke-width="3" points="${secondaryPoints}" />
    </svg>
  `;
};

const renderAbsorptionTable = (container, rows) => {
  container.innerHTML = "";
  const header = document.createElement("div");
  header.className = "table-row header";
  header.innerHTML = `
    <span>Type</span>
    <span>Price Band</span>
    <span>Active</span>
    <span>Sold 30d</span>
    <span>Ratio</span>
  `;
  container.appendChild(header);

  rows.slice(0, 12).forEach((row) => {
    const el = document.createElement("div");
    el.className = "table-row";
    el.innerHTML = `
      <span>${row.property_type}</span>
      <span>${row.price_band}</span>
      <span>${toNumber(row.active_count)}</span>
      <span>${toNumber(row.sold_30d)}</span>
      <span>${row.absorption_ratio ?? "—"}</span>
    `;
    container.appendChild(el);
  });
};

const renderBandMix = (container, bandShares) => {
  container.innerHTML = "";
  if (!bandShares.length) {
    container.innerHTML = "<p class=\"muted\">No band data</p>";
    return;
  }
  const latest = bandShares[bandShares.length - 1];
  const entries = Object.entries(latest.shares).sort((a, b) => b[1] - a[1]);
  entries.slice(0, 8).forEach(([band, share]) => {
    const el = document.createElement("div");
    el.className = "band-item";
    el.innerHTML = `
      <span>${band}</span>
      <span>${(share * 100).toFixed(1)}%</span>
    `;
    container.appendChild(el);
  });
};

const renderBandMixFromCounts = (container, counts) => {
  container.innerHTML = "";
  if (!counts.length) {
    container.innerHTML = "<p class=\\\"muted\\\">No band data</p>";
    return;
  }
  const total = counts.reduce((sum, item) => sum + item.count, 0);
  const entries = counts
    .map((item) => [item.key, total ? item.count / total : 0])
    .sort((a, b) => b[1] - a[1]);
  entries.slice(0, 8).forEach(([band, share]) => {
    const el = document.createElement("div");
    el.className = "band-item";
    el.innerHTML = `
      <span>${band}</span>
      <span>${(share * 100).toFixed(1)}%</span>
    `;
    container.appendChild(el);
  });
};

const renderTom = (container, summary, labels = {}) => {
  if (!summary) {
    container.innerHTML = "<p class=\"muted\">No data</p>";
    return;
  }
  const medianLabel = labels.median || "Median time on Realtor.ca";
  const averageLabel = labels.average || "Average time on Realtor.ca";
  const countLabel = labels.count || "Listings with time data";
  container.innerHTML = `
    <div class="metric">
      <div class="value">${summary.median_days ?? "—"} days</div>
      <div class="label">${medianLabel}</div>
    </div>
    <div class="metric">
      <div class="value">${summary.average_days ?? "—"} days</div>
      <div class="label">${averageLabel}</div>
    </div>
    <div class="metric">
      <div class="value">${summary.count ?? 0}</div>
      <div class="label">${countLabel}</div>
    </div>
  `;
};

const latestValue = (series) => {
  if (!series) return null;
  if (series.latest && series.latest.value !== undefined) {
    return series.latest.value;
  }
  const observations = series.observations || [];
  for (let idx = observations.length - 1; idx >= 0; idx -= 1) {
    const value = observations[idx].value;
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
};

const pickSeries = (rates, key) => {
  if (!rates || !rates.series) return null;
  return rates.series[key] || null;
};

const pickLatestRecord = (payload) => {
  if (!payload || !Array.isArray(payload.records)) {
    return null;
  }
  const records = payload.records.slice();
  if (!records.length) {
    return null;
  }
  records.sort((a, b) => (a.reference_month || "").localeCompare(b.reference_month || ""));
  return records[records.length - 1] || null;
};

const pickLatestInteriorStats = (payload) => {
  const latest = pickLatestRecord(payload);
  if (!latest) return null;
  return {
    month: latest.reference_month,
    moi: latest.moi,
    snlr: latest.snlr,
  };
};

const renderDiff = (container, diff) => {
  if (!diff) {
    container.innerHTML = "<p class=\"muted\">No snapshot diff found.</p>";
    return;
  }
  container.innerHTML = `
    <div class="metric">
      <div class="value">${toNumber(diff.added_count)}</div>
      <div class="label">New since last snapshot</div>
    </div>
    <div class="metric">
      <div class="value">${toNumber(diff.removed_count)}</div>
      <div class="label">Removed since last snapshot</div>
    </div>
  `;
};

const renderMarketState = (container, payload, officialStats) => {
  if (!payload) {
    container.innerHTML = "<p class=\"muted\">No market state data.</p>";
    return;
  }
  const state = payload.state || {};
  const metrics = payload.metrics || {};
  const thresholds = payload.thresholds || {};
  const officialMonth = officialStats?.month ? `IR ${officialStats.month}` : null;
  const snlrLabel = officialMonth ? `SNLR (${officialMonth})` : "SNLR 30d";
  const moiLabel = officialMonth ? `MOI (${officialMonth})` : "MOI (latest)";
  const snlrValue = officialStats?.snlr ?? metrics.snlr_30d;
  const moiValue = officialStats?.moi ?? metrics.moi_latest;
  const snlrLow = thresholds.snlr_low;
  const snlrHigh = thresholds.snlr_high;
  const moiMid = thresholds.moi_mid;
  const moiHigh = thresholds.moi_high;

  let scaleMarkup = "";
  if (
    snlrLow !== undefined &&
    snlrHigh !== undefined &&
    moiMid !== undefined &&
    moiHigh !== undefined
  ) {
    scaleMarkup = `
      <div class="scale-row">
        <div class="scale-label">SNLR</div>
        <div class="scale-bar snlr" aria-hidden="true"></div>
        <div class="scale-tags">
          <span>Buyer < ${snlrLow}</span>
          <span>Balanced ${snlrLow}–${snlrHigh}</span>
          <span>Seller > ${snlrHigh}</span>
        </div>
      </div>
      <div class="scale-row">
        <div class="scale-label">MOI</div>
        <div class="scale-bar moi" aria-hidden="true"></div>
        <div class="scale-tags">
          <span>Seller < ${moiMid}</span>
          <span>Balanced ${moiMid}–${moiHigh}</span>
          <span>Buyer > ${moiHigh}</span>
        </div>
      </div>
    `;
  }
  container.innerHTML = `
    <div class="state-chip ${state.key || "unknown"}">${state.label || "—"}</div>
    <p class="state-desc">${state.description || ""}</p>
    <div class="state-metrics">
      <div><span>${snlrLabel}</span><strong>${toRatio(snlrValue)}</strong></div>
      <div><span>${moiLabel}</span><strong>${toRatio(moiValue)}</strong></div>
      <div><span>DOM median</span><strong>${toNumber(metrics.dom_median_days)} days</strong></div>
      <div><span>Price-cut rate</span><strong>${toPercent(metrics.price_cut_rate)}</strong></div>
      <div><span>Net active change</span><strong>${toNumber(metrics.snapshot_net)}</strong></div>
    </div>
    ${scaleMarkup ? `<div class="state-scale">${scaleMarkup}</div>` : ""}
  `;
};

const renderTypeMetrics = (container, record) => {
  container.innerHTML = "";
  if (!record || !Array.isArray(record.property_types)) {
    container.innerHTML = "<p class=\"muted\">No IR stats yet.</p>";
    return;
  }
  const header = document.createElement("div");
  header.className = "type-row header";
  header.innerHTML = "<span>Type</span><span>Median</span><span>Days</span>";
  container.appendChild(header);

  record.property_types.forEach((item) => {
    const row = document.createElement("div");
    row.className = "type-row";
    row.innerHTML = `
      <span title="${item.property_type}">${item.property_type}</span>
      <span>${toCurrency(item.median_price)}</span>
      <span>${item.days_to_sell ?? "—"}</span>
    `;
    container.appendChild(row);
  });
};

const renderHeroStats = (metrics) => {
  const container = document.getElementById("hero-stats");
  renderMetrics(container, metrics);
};

const triggerAnimations = () => {
  const nodes = document.querySelectorAll("[data-animate]");
  nodes.forEach((node, index) => {
    node.style.animationDelay = `${0.08 * index}s`;
  });
  document.body.classList.add("ready");
};

const setVisible = (id, show) => {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = show ? "" : "none";
};

const setText = (id, value) => {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value;
  }
};

const loadRealtor = async (base) => {
  const [inventory, momentum, prices, absorption, tom, marketState, interiorStats, interiorTypeStats] =
    await Promise.all([
    fetchJson(`${base}active_inventory.json`),
    fetchJson(`${base}sold_momentum.json`),
    fetchJson(`${base}price_trends.json`),
    fetchJson(`${base}absorption.json`),
    fetchJson(`${base}time_on_market.json`),
    safeFetchJson(`${base}market_state.json`),
    safeFetchJson("../data/derived/interior_realtors/kootenay_market_stats.json"),
    safeFetchJson("../data/derived/interior_realtors/kootenay_monthly_stats.json"),
  ]);

  let marketBalance = null;
  try {
    marketBalance = await fetchJson(`${base}market_balance.json`);
  } catch (error) {
    marketBalance = null;
  }

  let diff = null;
  try {
    diff = await fetchJson(`${base}diffs/latest_diff.json`);
  } catch (error) {
    diff = null;
  }

  setText("as-of", `As of ${inventory.as_of?.slice(0, 10) || "—"}`);
  setText("generated-at", `Generated ${inventory.generated_at || "—"}`);

  const sold30d = momentum.recent_counts["30d"];
  const new30d = marketBalance?.recent_counts?.new_30d ?? null;
  const snlr30d = marketBalance?.recent_counts?.snlr_30d ?? null;

  renderHeroStats([
    { label: "Active listings", value: toNumber(inventory.total_listings) },
    { label: "Sold last 30d", value: toNumber(sold30d) },
    { label: "New last 30d", value: toNumber(new30d) },
    { label: "SNLR 30d", value: toRatio(snlr30d) },
  ]);

  setText("inventory-type-title", "By Property Type");
  setText("inventory-area-title", "Top Areas");
  renderBars(document.getElementById("inventory-by-type"), inventory.by_property_type);
  renderBars(document.getElementById("inventory-by-price"), inventory.by_price_band);
  renderBars(document.getElementById("inventory-by-area"), inventory.by_area, 6);
  renderDiff(document.getElementById("snapshot-diff"), diff);
  setVisible("market-state-card", true);
  renderMarketState(
    document.getElementById("market-state"),
    marketState,
    pickLatestInteriorStats(interiorStats)
  );

  const latestTypeStats = pickLatestRecord(interiorTypeStats);
  if (latestTypeStats) {
    setVisible("ir-stats-card", true);
    setText("ir-stats-title", `IR Price + DOM by Type (${latestTypeStats.reference_month})`);
    renderTypeMetrics(document.getElementById("ir-type-metrics"), latestTypeStats);
  } else {
    setVisible("ir-stats-card", false);
  }

  setText("momentum-title", "Sold Velocity");
  setText("momentum-subtitle", "Rolling sale counts to show demand pressure.");
  setText("recent-title", "Recent Sales");
  setText("momentum-spark-title", "Monthly Sold Trend");
  renderMetrics(document.getElementById("recent-sales"), [
    { label: "Last 7 days", value: toNumber(momentum.recent_counts["7d"]) },
    { label: "Last 30 days", value: toNumber(momentum.recent_counts["30d"]) },
    { label: "Last 90 days", value: toNumber(momentum.recent_counts["90d"]) },
  ]);
  const soldMonthlyCounts = momentum.by_month.map((item) => item.count);
  renderSparkline(document.getElementById("sold-sparkline"), soldMonthlyCounts, "var(--brick)");
  renderSeriesList(
    document.getElementById("sold-monthly-list"),
    momentum.by_month,
    "month",
    "count",
    toNumber
  );

  setText("pricing-title", "Price Direction");
  setText("pricing-subtitle", "Sold vs active list price trends by month.");
  setText("pricing-card-title", "Median Sold vs List");
  setVisible("price-legend", true);
  const soldMedian = prices.sold_by_month.map((item) => item.median);
  const activeMedian = prices.active_list_price_by_month.map((item) => item.median);
  const maxSeriesLength = Math.max(soldMedian.length, activeMedian.length);
  const paddedSold = Array.from({ length: maxSeriesLength }, (_, idx) => soldMedian[idx] ?? null);
  const paddedActive = Array.from({ length: maxSeriesLength }, (_, idx) => activeMedian[idx] ?? null);
  renderDualSparkline(document.getElementById("price-sparkline"), paddedSold, paddedActive);
  renderBandMix(document.getElementById("price-band-mix"), prices.sold_price_band_shares);

  setVisible("absorption", true);
  renderAbsorptionTable(document.getElementById("absorption-table"), absorption.rows);

  setVisible("tom-sold-card", true);
  setText("tom-title", "Speed of Sales");
  setText("tom-subtitle", "Approximate DOM from Realtor.ca metadata.");
  renderTom(document.getElementById("tom-active"), tom.active?.overall);
  renderTom(document.getElementById("tom-sold"), tom.sold?.overall);
};

const loadMacro = async () => {
  const rates = await safeFetchJson(`${MACRO_BASE}rates.json`);
  const unemployment = await safeFetchJson(`${MACRO_BASE}unemployment.json`);
  if (!rates && !unemployment) {
    setVisible("macro", false);
    return;
  }
  setVisible("macro", true);

  const policy = pickSeries(rates, "policy_target");
  const prime = pickSeries(rates, "prime");
  const mortgage5y = pickSeries(rates, "mortgage_5y");
  const mortgage3y = pickSeries(rates, "mortgage_3y");

  const rateMetrics = [];
  if (policy) rateMetrics.push({ label: "Policy target", value: toRate(latestValue(policy)) });
  if (prime) rateMetrics.push({ label: "Prime rate", value: toRate(latestValue(prime)) });
  if (mortgage5y) rateMetrics.push({ label: "Posted 5y", value: toRate(latestValue(mortgage5y)) });
  if (mortgage3y) rateMetrics.push({ label: "Posted 3y", value: toRate(latestValue(mortgage3y)) });
  if (!rateMetrics.length) {
    rateMetrics.push({ label: "Rates", value: "—" });
  }
  renderMetrics(document.getElementById("rates-metrics"), rateMetrics);

  const primaryRate = policy || prime || mortgage5y || mortgage3y;
  if (primaryRate) {
    const values = (primaryRate.observations || []).map((obs) => obs.value);
    renderSparkline(document.getElementById("rates-sparkline"), values, "var(--brick)");
    renderSeriesList(
      document.getElementById("rates-series"),
      primaryRate.observations || [],
      "date",
      "value",
      toRate
    );
  } else {
    renderSparkline(document.getElementById("rates-sparkline"), [], "var(--brick)");
    document.getElementById("rates-series").innerHTML = "<p class=\"muted\">No rate data</p>";
  }

  if (unemployment) {
    const observations = unemployment.observations || [];
    const latest = unemployment.latest?.value ?? latestValue({ observations });
    renderMetrics(document.getElementById("labour-metrics"), [
      { label: "Latest unemployment", value: toRate(latest) },
      { label: "Series points", value: toNumber(observations.length) },
    ]);
    renderSparkline(
      document.getElementById("labour-sparkline"),
      observations.map((row) => row.value),
      "var(--sage)"
    );
    renderSeriesList(
      document.getElementById("labour-series"),
      observations,
      "ref_period",
      "value",
      toRate
    );
  } else {
    renderMetrics(document.getElementById("labour-metrics"), [
      { label: "Latest unemployment", value: "—" },
    ]);
    document.getElementById("labour-sparkline").innerHTML = "<p class=\"muted\">No data</p>";
    document.getElementById("labour-series").innerHTML = "<p class=\"muted\">No data</p>";
  }
};

const loadRemax = async (base) => {
  const [inventory, listingTrend, tom] = await Promise.all([
    fetchJson(`${base}active_inventory.json`),
    fetchJson(`${base}listing_trend.json`),
    fetchJson(`${base}time_on_market.json`),
  ]);

  let diff = null;
  try {
    diff = await fetchJson(`${base}diffs/latest_diff.json`);
  } catch (error) {
    diff = null;
  }

  setText("as-of", `As of ${inventory.as_of?.slice(0, 10) || "—"}`);
  setText("generated-at", `Generated ${inventory.generated_at || "—"}`);

  const byMonth = listingTrend.by_month || [];
  const latest = byMonth[byMonth.length - 1];
  const last3 = byMonth.slice(-3).reduce((sum, item) => sum + item.count, 0);
  const last6 = byMonth.slice(-6).reduce((sum, item) => sum + item.count, 0);

  renderHeroStats([
    { label: "Active listings", value: toNumber(inventory.total_listings) },
    { label: "Latest month", value: toNumber(latest?.count ?? 0) },
    { label: "Last 6 months", value: toNumber(last6) },
  ]);

  setText("inventory-type-title", "By Status");
  setText("inventory-area-title", "Top Cities");
  renderBars(document.getElementById("inventory-by-type"), inventory.by_status);
  renderBars(document.getElementById("inventory-by-price"), inventory.by_price_band);
  renderBars(document.getElementById("inventory-by-area"), inventory.by_city, 6);
  renderDiff(document.getElementById("snapshot-diff"), diff);

  setText("momentum-title", "Listing Trend");
  setText("momentum-subtitle", "Active listings by month (RE/MAX).");
  setText("recent-title", "Recent Listings");
  setText("momentum-spark-title", "Monthly Listing Count");
  renderMetrics(document.getElementById("recent-sales"), [
    { label: "Latest month", value: toNumber(latest?.count ?? 0) },
    { label: "Last 3 months", value: toNumber(last3) },
    { label: "Months tracked", value: toNumber(byMonth.length) },
  ]);
  const counts = byMonth.map((item) => item.count);
  renderSparkline(document.getElementById("sold-sparkline"), counts, "var(--sea)");
  renderSeriesList(
    document.getElementById("sold-monthly-list"),
    byMonth,
    "month",
    "count",
    toNumber
  );

  setText("pricing-title", "List Price Trend");
  setText("pricing-subtitle", "Median list price based on listing dates.");
  setText("pricing-card-title", "Median List Price");
  setVisible("price-legend", false);
  const medians = byMonth.map((item) => item.median);
  renderSparkline(document.getElementById("price-sparkline"), medians, "var(--sea)");
  renderBandMixFromCounts(document.getElementById("price-band-mix"), inventory.by_price_band);

  setVisible("absorption", false);
  setVisible("tom-sold-card", false);
  setVisible("market-state-card", false);
  setVisible("ir-stats-card", false);
  setText("tom-title", "Listing Age");
  setText("tom-subtitle", "Approximate days since listing date (active only).");
  renderTom(document.getElementById("tom-active"), tom, {
    median: "Median days since listing date",
    average: "Average days since listing date",
    count: "Listings with listing date",
  });
};

const loadSource = async (key) => {
  const source = SOURCES[key];
  if (!source) return;
  setText("region-label", source.region);
  setText("hero-subtitle", source.subtitle);
  setText("footer-source", `Generated from \`${source.path || source.base}\`.`);
  if (source.type === "realtor") {
    await Promise.all([loadRealtor(source.base), loadMacro()]);
  } else {
    await Promise.all([loadRemax(source.base), loadMacro()]);
  }
};

const init = async () => {
  try {
    const select = document.getElementById("source-select");
    Object.entries(SOURCES).forEach(([key, source]) => {
      const option = document.createElement("option");
      option.value = key;
      option.textContent = source.label;
      select.appendChild(option);
    });

    const params = new URLSearchParams(window.location.search);
    const requested = params.get("source");
    const initial = SOURCES[requested] ? requested : "realtor";
    select.value = initial;
    await loadSource(initial);

    select.addEventListener("change", async (event) => {
      await loadSource(event.target.value);
    });

    triggerAnimations();
  } catch (error) {
    document.body.innerHTML = `
      <div style="padding: 4rem; font-family: 'Space Grotesk', sans-serif;">
        <h2>Data load failed</h2>
        <p>${error.message}</p>
        <p>Run a local server (e.g., <code>python -m http.server</code>) from the repo root.</p>
      </div>
    `;
    triggerAnimations();
  }
};

init();
