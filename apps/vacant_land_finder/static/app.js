const APP_BOOTSTRAP = window.APP_BOOTSTRAP || {};

const DEFAULTS = {
  lat: 44.32307163206222,
  lng: -78.32923679935357,
  radius_km: 50,
  include_realtor: true,
  include_remax: true,
  ...(APP_BOOTSTRAP.defaults || {}),
};

const SOURCE_LABELS = {
  realtor_ca: "Realtor.ca",
  remax_ca: "RE/MAX",
  ...(APP_BOOTSTRAP.sources || {}),
};

const currency = new Intl.NumberFormat("en-CA", {
  style: "currency",
  currency: "CAD",
  maximumFractionDigits: 0,
});

const number = new Intl.NumberFormat("en-CA", { maximumFractionDigits: 2 });

const form = document.getElementById("search-form");
const statusText = document.getElementById("status-text");
const meta = document.getElementById("meta");
const count = document.getElementById("result-count");
const list = document.getElementById("results-list");
const searchBtn = document.getElementById("search-btn");

function setDefaults() {
  Object.entries(DEFAULTS).forEach(([key, value]) => {
    const input = document.getElementById(key);
    if (!input) {
      return;
    }
    if (input.type === "checkbox") {
      input.checked = Boolean(value);
    } else {
      input.value = String(value);
    }
  });
}

function queryStringFromForm(formEl) {
  const data = new FormData(formEl);
  data.set("include_realtor", document.getElementById("include_realtor").checked ? "true" : "false");
  data.set("include_remax", document.getElementById("include_remax").checked ? "true" : "false");
  return new URLSearchParams(data).toString();
}

function fmtPrice(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  return currency.format(value);
}

function fmtDistance(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${number.format(value)} km`;
}

function fmtLandSize(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return "-";
  }
  return String(value);
}

function labelForSource(source) {
  return SOURCE_LABELS[source] || source || "Unknown";
}

function renderMeta(payload) {
  const sourceCounts = {};
  (payload.results || []).forEach((item) => {
    const source = item.source || "unknown";
    sourceCounts[source] = (sourceCounts[source] || 0) + 1;
  });

  const sourceText = Object.entries(sourceCounts)
    .map(([source, total]) => `${labelForSource(source)}: ${total}`)
    .join(" | ");

  const latInput = document.getElementById("lat");
  const lngInput = document.getElementById("lng");
  const radiusInput = document.getElementById("radius_km");

  const centerLat = Number(latInput && latInput.value ? latInput.value : DEFAULTS.lat);
  const centerLng = Number(lngInput && lngInput.value ? lngInput.value : DEFAULTS.lng);
  const radiusKm = Number(radiusInput && radiusInput.value ? radiusInput.value : DEFAULTS.radius_km);

  const parts = [
    `Center: ${centerLat.toFixed(6)}, ${centerLng.toFixed(6)}`,
    `Radius: ${number.format(radiusKm)} km`,
  ];

  if (sourceText) {
    parts.push(sourceText);
  }

  meta.textContent = parts.join(" | ");
}

function renderErrors(payload) {
  const errors = payload.errors || {};
  const keys = Object.keys(errors);
  if (!keys.length) {
    return;
  }

  const block = document.createElement("div");
  block.className = "error-block";
  block.textContent = keys.map((key) => `${labelForSource(key)}: ${errors[key]}`).join(" | ");
  list.prepend(block);
}

function appendListingCard(item, index) {
  const card = document.createElement("article");
  card.className = "item";

  const rank = document.createElement("p");
  rank.className = "rank";
  rank.textContent = `#${item.rank || index + 1}`;

  const title = document.createElement("h3");
  title.textContent = item.address || "Address unavailable";

  const locality = document.createElement("p");
  locality.className = "locality";
  locality.textContent = [item.city, item.province].filter(Boolean).join(", ") || "-";

  const details = document.createElement("p");
  details.className = "details";
  details.textContent = `${labelForSource(item.source)} | ${fmtDistance(item.distance_km)} | ${fmtPrice(item.price)} | Land: ${fmtLandSize(item.land_size)}`;

  const main = document.createElement("div");
  main.className = "main";
  main.append(title, locality, details);

  card.append(rank, main);

  if (item.listing_url) {
    const link = document.createElement("a");
    link.className = "visit";
    link.href = item.listing_url;
    link.target = "_blank";
    link.rel = "noreferrer noopener";
    link.textContent = "Open";
    card.appendChild(link);
  }

  list.appendChild(card);
}

function renderResults(payload) {
  list.innerHTML = "";
  const results = payload.results || [];
  count.textContent = `${results.length} results`;

  if (!results.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No listings found in this radius.";
    list.appendChild(empty);
    return;
  }

  results.forEach((item, index) => appendListingCard(item, index));
  renderErrors(payload);
}

async function runSearch() {
  statusText.textContent = "Searching...";
  searchBtn.disabled = true;

  try {
    const response = await fetch(`/api/search?${queryStringFromForm(form)}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || "Search failed");
    }

    renderMeta(payload);
    renderResults(payload);
    statusText.textContent = `Updated ${new Date(payload.generated_at).toLocaleString()}`;
  } catch (error) {
    list.innerHTML = "";
    count.textContent = "0 results";
    meta.textContent = "";
    statusText.textContent = `Error: ${error.message}`;
  } finally {
    searchBtn.disabled = false;
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  runSearch();
});

setDefaults();
runSearch();
