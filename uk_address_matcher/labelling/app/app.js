import { loadBrowserStore } from "./browser.js";

"use strict";
let browserStore = null;
const state = {
  page: 1,
  pageSize: 20,
  maximumPage: 1,
  total: 0,
  rows: [],
  bootstrap: null,
  sortBy: "unique_id",
  sortOrder: "asc",
};
const $ = (id) => document.getElementById(id);
const el = {
  body: $("records-body"),
  range: $("record-range"),
  rangeBottom: $("record-range-bottom"),
  top: $("pagination-top"),
  bottom: $("pagination-bottom"),
  save: $("save-status"),
  overlay: $("session-overlay"),
};
async function api(url, options = {}) {
  if (!browserStore) throw Error("Load a labelling bundle first.");
  return browserStore.request(url, options);
}
function stages() {
  return [...document.querySelectorAll('input[name="stage"]:checked')].map(
    (input) => input.value,
  );
}
function applyFilter() {
  state.page = 1;
  load();
}
function buildStageFilters(counts) {
  const container = $("stage-options");
  container.replaceChildren();
  Object.entries(counts).forEach(([stage, count]) => {
    const label = document.createElement("label"),
      input = document.createElement("input");
    label.className = "stage-option";
    input.type = "checkbox";
    input.name = "stage";
    input.value = stage;
    input.checked = true;
    input.onchange = applyFilter;
    label.append(input, document.createTextNode(` ${stage}`), text("b", count));
    container.append(label);
  });
}
function configureRange(prefix) {
  const low = $(`${prefix}-range-min`),
    high = $(`${prefix}-range-max`),
    min = $(`${prefix}-min`),
    max = $(`${prefix}-max`),
    scale = low.parentElement,
    paint = () => {
      const span = Number(low.max) - Number(low.min),
        start = ((Number(low.value) - Number(low.min)) / span) * 100,
        end = ((Number(high.value) - Number(low.min)) / span) * 100;
      scale.style.setProperty("--range-start", `${start}%`);
      scale.style.setProperty("--range-end", `${end}%`);
    },
    sync = () => {
      if (Number(low.value) > Number(high.value)) high.value = low.value;
      min.value = low.value;
      max.value = high.value;
      paint();
      min.dispatchEvent(new Event("input"));
    },
    syncFromInputs = (changed) => {
      const value = (input, slider, fallback) => {
        if (!input.value) return fallback;
        const number = Number(input.value);
        return Number.isFinite(number)
          ? Math.min(Math.max(number, Number(slider.min)), Number(slider.max))
          : Number(slider.value);
      };
      low.value = value(min, low, low.min);
      high.value = value(max, high, high.max);
      if (Number(low.value) > Number(high.value)) {
        if (changed === min) {
          high.value = low.value;
          max.value = high.value;
        } else {
          low.value = high.value;
          min.value = low.value;
        }
      }
      paint();
    };
  low.oninput = sync;
  high.oninput = sync;
  min.addEventListener("input", () => syncFromInputs(min));
  max.addEventListener("input", () => syncFromInputs(max));
  paint();
}
function resetRanges() {
  $("score-range-min").value = -10;
  $("score-range-max").value = 30;
  $("dist-range-min").value = 0;
  $("dist-range-max").value = 20;
  $("score-range-min").dispatchEvent(new Event("input"));
  $("dist-range-min").dispatchEvent(new Event("input"));
}
function query() {
  const p = new URLSearchParams({
    page: state.page,
    page_size: state.pageSize,
    show_labelled: $("show-labelled").checked,
    mismatches_only: $("mismatches-only").checked,
    sort_by: state.sortBy,
    sort_order: state.sortOrder,
  });
  [
    ["unique_id_query", "record-unique-id"],
    ["address_query", "record-address-query"],
  ].forEach(([key, id]) => {
    if ($(id).value.trim()) p.set(key, $(id).value.trim());
  });
  stages().forEach((stage) => p.append("stage", stage));
  [
    ["score_min", "score-min"],
    ["score_max", "score-max"],
    ["distinguishability_min", "dist-min"],
    ["distinguishability_max", "dist-max"],
  ].forEach(([key, id]) => {
    if ($(id).value) p.set(key, $(id).value);
  });
  return p;
}
function text(tag, value, className = "") {
  const node = document.createElement(tag);
  node.textContent = value;
  if (className) node.className = className;
  return node;
}
function matchWeightClass(value) {
  if (value == null) return "match-weight";
  const weight = Number(value);
  if (weight < 0) return "match-weight weight-negative";
  if (weight < 5) return "match-weight weight-red-orange";
  if (weight < 8) return "match-weight weight-orange-yellow";
  if (weight < 10) return "match-weight weight-yellow";
  if (weight < 12) return "match-weight weight-yellow-green";
  if (weight < 20) return "match-weight weight-green";
  return "match-weight weight-strong-green";
}
const sortDescriptions = {
  reranked_score: "Final score after reranking candidates",
  splink_score: "Raw Splink match weight before reranking",
  distinguishability: "Difference between this match and the next-best candidate",
};
function updateSortControls() {
  document.querySelectorAll(".sort-button").forEach((button) => {
    const active = button.dataset.sort === state.sortBy,
      direction = active ? state.sortOrder : "asc",
      arrow = button.querySelector(".sort-arrow");
    arrow.textContent = active ? (direction === "asc" ? "↑" : "↓") : "↕";
    button.setAttribute(
      "aria-label",
      `${sortDescriptions[button.dataset.sort]}. Sort ${direction === "asc" ? "descending" : "ascending"} next`,
    );
  });
}
let hoverCard = null;
function hideHoverCard() {
  if (hoverCard) {
    hoverCard.remove();
    hoverCard = null;
  }
}
document.addEventListener("scroll", hideHoverCard, true);
function addHoverDetails(cell, items, className) {
  if (!items.length) return;
  const card = document.createElement("div");
  card.className = `hover-card ${className}`;
  items.forEach((item) => card.append(item));
  cell.classList.add("has-hover-details");
  cell.addEventListener("pointerenter", () => {
    hideHoverCard();
    document.body.append(card);
    const bounds = cell.getBoundingClientRect(),
      left = Math.min(bounds.left, window.innerWidth - card.offsetWidth - 12);
    card.style.left = `${Math.max(12, left)}px`;
    card.style.top = `${Math.min(bounds.bottom + 6, window.innerHeight - card.offsetHeight - 12)}px`;
    hoverCard = card;
  });
  cell.addEventListener("pointerleave", hideHoverCard);
}
function candidateDetails(row) {
  return (Array.isArray(row.top_candidates) ? row.top_candidates : [])
    .filter((candidate) => candidate && candidate.canonical_address)
    .map((candidate) => {
      const score = Number(candidate.splink_match_weight),
        scoreText = Number.isFinite(score) ? score.toFixed(2) : "-",
        item = document.createElement("div");
      item.className = "candidate-detail";
      item.append(
        text("strong", `Address candidate ${candidate.rank ?? ""}`),
        text("span", candidate.canonical_address),
        text("span", candidate.canonical_postcode || ""),
        text("span", `Splink score: ${scoreText}`),
      );
      return item;
    });
}
function candidates(row) {
  const list = row.top_candidates || row.candidates;
  return Array.isArray(list)
    ? list.filter(
        (candidate) => candidate && candidate.label_id != null,
      )
    : [];
}
function currentDisplayLabel(record) {
  if (!record.current_label) return null;
  if (String(record.current_label) === String(record.resolved_label_id))
    return record.resolved_canonical_id || record.current_label;
  return (
    candidates(record).find(
      (candidate) => String(candidate.label_id) === String(record.current_label),
    )?.canonical_id || record.current_label
  );
}
function select(row) {
  const node = document.createElement("select");
  node.className = "label-select";
  const current =
    {
      accept_model: "model",
      use_existing: "existing",
      imported: "existing",
      no_match: "no_match",
      uncertain: "uncertain",
    }[row.current_decision] || "";
  node.append(new Option("Select label...", "", false, !current));
  const modelCandidate = candidates(row).find(
      (candidate) => candidate.is_model_selection,
    ),
    modelLabel = row.resolved_canonical_id ?? modelCandidate?.canonical_id;
  const seen = new Set();
  const add = (value, label, name) => {
    if (seen.has(value)) return;
    seen.add(value);
    node.append(new Option(name, value, false, current === value));
  };
  if (modelLabel != null && modelLabel !== "")
    add("model", String(modelLabel), `Predicted value - ${modelLabel}`);
  if (row.imported_label)
    add(
      "existing",
      String(row.imported_label),
      `Existing label - ${row.imported_label}`,
    );
  candidates(row).forEach((candidate, index) =>
    add(
      `candidate:${index}`,
      String(candidate.label_id),
      `Candidate ${candidate.rank ?? index + 1} - ${candidate.canonical_id || candidate.label_id}`,
    ),
  );
  node.append(
    new Option("No match", "no_match", false, current === "no_match"),
    new Option("Uncertain", "uncertain", false, current === "uncertain"),
  );
  if (row.is_labelled) node.append(new Option("Clear label", "clear"));
  node.onclick = (event) => event.stopPropagation();
  node.onchange = () => save(row, node.value);
  return node;
}
async function save(row, value) {
  if (!value) return;
  let payload = {
    unique_id: row.unique_id,
    decision: value,
    ukam_label: null,
    selected_candidate_rank: null,
  };
  if (value === "model") {
    payload.decision = "accept_model";
    payload.ukam_label = row.resolved_label_id;
    payload.selected_candidate_rank = 1;
  } else if (value === "existing") {
    payload.decision = "use_existing";
    payload.ukam_label = row.imported_label;
  } else if (value.startsWith("candidate:")) {
    const candidate = candidates(row)[Number(value.split(":")[1])];
    payload.decision = "select_candidate";
    payload.ukam_label = String(candidate.label_id);
    payload.selected_candidate_rank = candidate.rank ?? null;
  }
  el.save.textContent = "Saving...";
  try {
    await api("/api/labels", { method: "POST", body: JSON.stringify(payload) });
    state.bootstrap = await api("/api/bootstrap");
    $("label-progress").textContent =
      `${state.bootstrap.labelled_records} / ${state.bootstrap.total_records}`;
    el.save.textContent = "Autosaved";
    toast("Label saved");
    load();
  } catch (error) {
    el.save.textContent = "Save failed";
    toast(error.message);
  }
}
function review(id) {
  sessionStorage.setItem("ukam-review-filter-query", reviewFilterQuery());
  sessionStorage.setItem("ukam-last-review-id", id);
  location.hash = `review/${encodeURIComponent(id)}`;
  view("review");
}
function render() {
  el.body.replaceChildren();
  if (!state.rows.length) {
    const row = document.createElement("tr"),
      cell = text("td", "No records match the selected filters.");
    cell.colSpan = 10;
    row.append(cell);
    el.body.append(row);
    return;
  }
  state.rows.forEach((record) => {
    const row = document.createElement("tr");
    row.onclick = (event) => {
      if (!event.target.closest("button,select,input"))
        review(record.unique_id);
    };
    const messy = text(
        "td",
        record.messy_address || "Address unavailable",
        "messy",
      ),
      originalAddress = String(record.messy_address || "").trim(),
      cleanedAddress = String(record.messy_cleaned_address || "").trim(),
      messyPostcode = String(record.messy_postcode || "").trim(),
      canonicalPostcode = String(
        record.resolved_canonical_postcode || "",
      ).trim(),
      postcode = text("div", messyPostcode || "-", "secondary messy-postcode");
    if (
      canonicalPostcode &&
      messyPostcode &&
      canonicalPostcode !== messyPostcode
    )
      postcode.classList.add("postcode-mismatch");
    messy.append(postcode);
    if (cleanedAddress && cleanedAddress !== originalAddress)
      addHoverDetails(
        messy,
        [
          text("div", "Cleaned address", "hover-title"),
          text("div", cleanedAddress, "hover-address"),
        ],
        "cleaned-detail",
      );
    const suggestion = document.createElement("td");
    suggestion.className = "model-suggestion";
    if (record.resolved_label_id) {
      suggestion.append(
        text("div", record.resolved_canonical_id, "primary"),
        text(
          "div",
          record.resolved_canonical_address || "Address unavailable",
          "primary",
        ),
        text("div", record.resolved_canonical_postcode || "", "secondary"),
      );
      addHoverDetails(
        suggestion,
        candidateDetails(record),
        "candidate-details",
      );
    } else suggestion.textContent = "No accepted match";
    const currentLabel = document.createElement("td");
    currentLabel.className = "current-label";
    const currentLabelValue = currentDisplayLabel(record);
    if (currentLabelValue) {
      currentLabel.append(text("div", currentLabelValue, "primary"));
      if (record.current_label_clean_full_address)
        currentLabel.append(
          text("div", record.current_label_clean_full_address, "primary"),
        );
      if (record.current_label_postcode)
        currentLabel.append(text("div", record.current_label_postcode, "secondary"));
    } else currentLabel.textContent = "Not labelled";
    const stage = text(
      "span",
      {
        exact: "Exact",
        peeled: "Peeled",
        splink: "Splink",
        unique_trigram: "Unique trigram",
        unmatched: "Unmatched",
      }[record.match_stage] || record.match_stage,
      `stage stage-${record.match_stage}`,
    );
    const stageCell = document.createElement("td");
    stageCell.append(stage);
    const labelCell = document.createElement("td");
    labelCell.append(select(record));
    const action = document.createElement("td"),
      button = text("button", "Review", "review"),
      weightText =
        record.match_weight == null
          ? "-"
          : Number(record.match_weight).toFixed(2);
    button.onclick = (event) => {
      event.stopPropagation();
      review(record.unique_id);
    };
    action.append(button);
    row.append(
      text("td", record.unique_id, "primary"),
      messy,
      suggestion,
      currentLabel,
      stageCell,
      text("td", weightText, matchWeightClass(record.match_weight)),
      text(
        "td",
        record.splink_match_weight == null
          ? "-"
          : Number(record.splink_match_weight).toFixed(2),
        matchWeightClass(record.splink_match_weight),
      ),
      text(
        "td",
        record.distinguishability == null
          ? "-"
          : Number(record.distinguishability).toFixed(2),
      ),
      labelCell,
      action,
    );
    el.body.append(row);
  });
}
function paginationItems() {
  if (state.maximumPage <= 7)
    return Array.from({ length: state.maximumPage }, (_, index) => index + 1);
  const pages = [1];
  if (state.page > 3) pages.push("...");
  for (
    let page = Math.max(2, state.page - 1);
    page <= Math.min(state.maximumPage - 1, state.page + 1);
    page++
  )
    pages.push(page);
  if (state.page < state.maximumPage - 2) pages.push("...");
  pages.push(state.maximumPage);
  return pages;
}
function renderPagination(container) {
  container.replaceChildren();
  const add = (value, disabled = false, current = false) => {
    const button = text("button", value);
    button.disabled = disabled;
    button.className = current ? "current" : "";
    button.onclick = () => {
      state.page = Number(value);
      load();
    };
    container.append(button);
  };
  const previous = text("button", "Previous");
  previous.disabled = state.page === 1;
  previous.onclick = () => {
    state.page--;
    load();
  };
  container.append(previous);
  paginationItems().forEach((item) =>
    item === "..."
      ? container.append(text("span", "..."))
      : add(item, false, item === state.page),
  );
  const next = text("button", "Next");
  next.disabled = state.page === state.maximumPage;
  next.onclick = () => {
    state.page++;
    load();
  };
  container.append(next);
}
async function load() {
  try {
    const data = await api(`/api/records?${query()}`);
    Object.assign(state, {
      page: data.page,
      pageSize: data.page_size,
      maximumPage: data.maximum_page,
      total: data.total_filtered,
      rows: data.rows,
    });
    render();
    renderPagination(el.top);
    renderPagination(el.bottom);
    const first = state.total ? (state.page - 1) * state.pageSize + 1 : 0,
      last = Math.min(state.page * state.pageSize, state.total),
      value = state.total
        ? `Showing ${first}-${last} of ${state.total} records`
        : "Showing 0 records";
    el.range.textContent = value;
    el.rangeBottom.textContent = value;
  } catch (error) {
    expired(error);
  }
}
function view(name) {
  document
    .querySelectorAll(".view")
    .forEach((node) =>
      node.classList.toggle("active", node.id === `${name}-view`),
    );
  document
    .querySelectorAll(".tab")
    .forEach((node) =>
      node.classList.toggle("active", node.dataset.view === name),
    );
}
function toast(message) {
  const node = $("toast");
  node.textContent = message;
  node.hidden = false;
  setTimeout(() => (node.hidden = true), 2600);
}
function expired(error) {
  console.error(error);
  el.overlay.hidden = false;
}
async function initialise() {
  document.querySelectorAll(".tab").forEach(
    (button) =>
      (button.onclick = () => {
        location.hash = button.dataset.view;
        view(button.dataset.view);
        if (button.dataset.view === "overview") load();
      }),
  );
  addEventListener("hashchange", () =>
    view(
      location.hash.startsWith("#review")
        ? "review"
        : "overview",
    ),
  );
  $("reset-filters").onclick = () => {
    document
      .querySelectorAll('input[name="stage"]')
      .forEach((input) => (input.checked = true));
    ["score-min", "score-max", "dist-min", "dist-max"].forEach(
      (id) => ($(id).value = ""),
    );
    $("record-unique-id").value = "";
    $("record-address-query").value = "";
    resetRanges();
    $("show-labelled").checked = true;
    $("mismatches-only").checked = false;
    state.sortBy = "unique_id";
    state.sortOrder = "asc";
    updateSortControls();
    state.page = 1;
    load();
  };
  $("collapse-filters").onclick = () => {
    $("filters").hidden = true;
    $("layout").classList.add("collapsed");
    $("expand-filters").hidden = false;
  };
  $("expand-filters").onclick = () => {
    $("filters").hidden = false;
    $("layout").classList.remove("collapsed");
    $("expand-filters").hidden = true;
  };
  $("show-labelled").onchange = applyFilter;
  $("mismatches-only").onchange = applyFilter;
  ["record-unique-id", "record-address-query"].forEach((id) =>
    $(id).addEventListener("input", () => {
      state.page = 1;
      clearTimeout(window.searchTimer);
      window.searchTimer = setTimeout(load, 250);
    }),
  );
  document.querySelectorAll(".sort-button").forEach((button) => {
    button.onclick = () => {
      if (state.sortBy === button.dataset.sort)
        state.sortOrder = state.sortOrder === "asc" ? "desc" : "asc";
      else {
        state.sortBy = button.dataset.sort;
        state.sortOrder = "desc";
      }
      state.page = 1;
      updateSortControls();
      load();
    };
  });
  updateSortControls();
  $("page-size").onchange = () => {
    state.page = 1;
    state.pageSize = Number($("page-size").value);
    load();
  };
  ["score-min", "score-max", "dist-min", "dist-max"].forEach(
    (id) =>
      ($(id).oninput = () => {
        state.page = 1;
        clearTimeout(window.filterTimer);
        window.filterTimer = setTimeout(load, 300);
      }),
  );
  configureRange("score");
  configureRange("dist");
  $("scroll-top").onclick = () => scrollTo({ top: 0, behavior: "smooth" });
  $("scroll-bottom").onclick = () =>
    scrollTo({
      top: document.documentElement.scrollHeight,
      behavior: "smooth",
    });
  $("download-updates").onclick = () => browserStore.downloadUpdates();
  view(location.hash.startsWith("#review") ? "review" : "overview");
  try {
    state.bootstrap = await api("/api/bootstrap");
    $("bundle-name").textContent = state.bootstrap.bundle_name;
    $("label-progress").textContent =
      `${state.bootstrap.labelled_records} / ${state.bootstrap.total_records}`;
    buildStageFilters(state.bootstrap.stage_counts);
    const savedToBundle = browserStore.remoteEventsUrl;
    $("session-countdown").textContent = savedToBundle
      ? "Saved to bundle/labelled_review_data.parquet"
      : "Saved in browser";
    $("session-countdown").title = savedToBundle
      ? `Events: ${browserStore.labelledReviewPath?.replace(/labelled_review_data\.parquet$/, "labelling_updates.json")}\nLabelled review: ${browserStore.labelledReviewPath}`
      : "Download updates to keep a portable copy outside this browser.";
    await load();
  } catch (error) {
    expired(error);
  }
}
async function loadDataset(manifest, review, canonical, options = {}) {
  const status = $("dataset-loader-status"),
    button = $("load-dataset");
  button.disabled = true;
  status.textContent = "Starting DuckDB-WASM. Browser memory is limited; large files may fail to load.";
  const previousStore = browserStore;
  browserStore = null;
  if (previousStore) await previousStore.close();
  try {
    browserStore = await loadBrowserStore(manifest, review, canonical, options);
    $("dataset-loader").hidden = true;
    $("labelling-app").hidden = false;
    if (!browserStore.remoteEventsUrl)
      $("session-countdown").textContent = "Saved in browser";
    await initialise();
  } catch (error) {
    browserStore = null;
    $("dataset-loader").hidden = false;
    $("labelling-app").hidden = true;
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}
async function loadSelectedDataset() {
  const bundleFiles = [...$("bundle-directory").files];
  const manifest = bundleFiles.find((file) => file.name === "manifest.json");
  if (!manifest) {
    $("dataset-loader-status").textContent =
      "Select the folder containing manifest.json and the review data file.";
    return;
  }
  let manifestPayload;
  try {
    manifestPayload = JSON.parse(await manifest.text());
  } catch {
    $("dataset-loader-status").textContent = "Bundle manifest is not valid JSON.";
    return;
  }
  const reviewName = String(manifestPayload.data_file || "review_data.parquet");
  const review = bundleFiles.find((file) => file.name === reviewName);
  if (!review) {
    $("dataset-loader-status").textContent =
      `The selected bundle folder is missing ${reviewName}.`;
    return;
  }
  const canonicalName = manifestPayload.canonical_data_file;
  const bundleCanonical = canonicalName
    ? bundleFiles.find((file) => file.name === canonicalName)
    : null;
  if (canonicalName && !bundleCanonical) {
    $("dataset-loader-status").textContent =
      `The selected bundle folder is missing ${canonicalName}.`;
    return;
  }
  await loadDataset(
    manifest,
    review,
    [
      ...(bundleCanonical ? [bundleCanonical] : []),
      ...$("canonical-data-files").files,
    ],
  );
}
async function fileFromUrl(url, name) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Could not load local file ${name}.`);
  const blob = await response.blob();
  return new File([blob], name, { type: blob.type });
}
function lazyFileFromUrl(url, name) {
  return {
    name,
    url: new URL(url, location.href).href,
    async arrayBuffer() {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Could not load local file ${name}.`);
      return response.arrayBuffer();
    },
  };
}
async function loadConfiguredDataset() {
  try {
    const response = await fetch("/api/local-config");
    if (!response.ok || !response.headers.get("content-type")?.includes("json")) {
      $("dataset-loader").hidden = false;
      return;
    }
    const config = await response.json();
    if (!config.bundle) {
      $("dataset-loader").hidden = false;
      return;
    }
    $("dataset-loader").hidden = true;
    $("labelling-app").hidden = false;
    const manifest = await fileFromUrl(
      config.bundle.manifest_url,
      config.bundle.manifest_name,
    );
    const review = await fileFromUrl(
      config.bundle.review_url,
      config.bundle.review_name,
    );
    const canonical = await Promise.all(
      (config.canonical_urls || []).map((item) =>
        lazyFileFromUrl(item.url, item.name),
      ),
    );
    await loadDataset(manifest, review, canonical, {
      remoteEventsUrl: config.events_url,
      nativeCanonicalSearchUrl: config.canonical_search_url,
      labelledReviewPath: config.labelled_review_path,
    });
  } catch (error) {
    $("dataset-loader").hidden = false;
    $("labelling-app").hidden = true;
    $("dataset-loader-status").textContent = error.message;
  }
}
$("load-dataset").onclick = loadSelectedDataset;
state.review = {
  record: null,
  navigation: null,
  selectedCandidateLabel: null,
  loading: false,
  pendingDecision: null,
  saveFailed: false,
};
loadConfiguredDataset();
const reviewElements = {
  empty: $("review-empty"),
  content: $("review-content"),
  complete: $("review-complete"),
  previous: $("review-previous"),
  next: $("review-next"),
  position: $("review-position"),
  candidateBody: $("review-candidate-body"),
  candidates: $("review-candidates"),
  undo: $("review-undo"),
  accept: $("review-accept"),
};
function reviewFilterQuery() {
  const parameters = query();
  parameters.delete("page");
  parameters.delete("page_size");
  return parameters.toString();
}
function reviewId() {
  const hash = location.hash.replace(/^#/, "");
  return hash.startsWith("review/") ? decodeURIComponent(hash.slice(7)) : null;
}
function display(value, fallback = "Not available") {
  return value == null || value === "" ? fallback : String(value);
}
function canonicalColumnLabel(column) {
  const key = String(column).toLowerCase();
  if (key === "classificationcode" || key === "classification_code")
    return "OS classification";
  if (key === "floorlevel" || key === "floor_level") return "Floor level";
  return String(column)
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}
function renderCanonicalAdditionalFields(container, values) {
  const fields = Object.entries(values || {}).filter(
    ([, value]) => value != null && value !== "",
  );
  container.replaceChildren(
    ...fields.map(([column, value]) => {
      const field = document.createElement("div");
      field.className = "review-canonical-additional-field";
      field.append(
        text("span", canonicalColumnLabel(column), "review-canonical-field-label"),
        text("span", value, "review-canonical-field-value"),
      );
      return field;
    }),
  );
  container.hidden = !container.hasChildNodes();
}
function metric(value, applicable = true) {
  if (!applicable) return "Not applicable";
  const number = Number(value);
  return value == null || !Number.isFinite(number)
    ? "Not available"
    : number.toFixed(2);
}
function reviewCandidates(record) {
  return Array.isArray(record.candidates)
    ? [...record.candidates].sort(
        (left, right) =>
          Number(left.rank ?? Number.MAX_SAFE_INTEGER) -
          Number(right.rank ?? Number.MAX_SAFE_INTEGER),
      )
    : [];
}
function initialCandidate(record) {
  const list = reviewCandidates(record),
    current = list.find(
      (item) => String(item.label_id) === String(record.current_label),
    );
  return (
    String(
      (current || list.find((item) => item.is_model_selection) || list[0] || {})
        .label_id || "",
    ) || null
  );
}
function selectedReviewCandidate() {
  return (
    reviewCandidates(state.review.record || {}).find(
      (item) =>
        String(item.label_id) === String(state.review.selectedCandidateLabel),
    ) || null
  );
}
function showReviewEmpty() {
  state.review.record = null;
  state.review.pendingDecision = null;
  state.review.saveFailed = false;
  reviewElements.content.hidden = true;
  reviewElements.complete.hidden = true;
  reviewElements.empty.hidden = false;
}
function renderReview() {
  const record = state.review.record,
    navigation = state.review.navigation;
  if (!record || !navigation) return showReviewEmpty();
  reviewElements.empty.hidden = true;
  reviewElements.complete.hidden = true;
  reviewElements.content.hidden = false;
  reviewElements.position.textContent = Number.isInteger(navigation.position)
    ? `Record ${navigation.position} of ${navigation.total}`
    : "Preparing review navigation...";
  reviewElements.previous.disabled = !navigation.previous_unique_id;
  reviewElements.next.disabled = !navigation.next_unique_id;
  $("review-messy-id").textContent = display(record.unique_id);
  const messyAddress = String(record.messy_address || "").trim(),
    messyPostcode = String(record.messy_postcode || "").trim(),
    mapQuery = [messyAddress, messyPostcode].filter(Boolean).join(", ");
  $("review-messy-address").textContent = display(messyAddress);
  const mapLink = $("review-open-map");
  mapLink.hidden = !messyAddress;
  mapLink.href = `https://www.google.com/maps/search/?${new URLSearchParams({
    api: "1",
    query: mapQuery,
  })}`;
  $("review-messy-cleaned").textContent = display(record.messy_cleaned_address);
  $("review-messy-postcode").textContent = display(record.messy_postcode);
  const matched = Boolean(
    record.resolved_label_id || record.resolved_canonical_id,
  );
  $("review-canonical-card").classList.toggle("empty-card", !matched);
  $("review-canonical-fields").hidden = !matched;
  $("review-no-canonical").hidden = matched;
  if (matched) {
    $("review-canonical-label").textContent = display(
      record.resolved_canonical_id,
    );
    $("review-canonical-address").textContent = display(
      record.resolved_canonical_address,
    );
    $("review-canonical-postcode").textContent = display(
      record.resolved_canonical_postcode,
    );
  }
  $("review-sticky-messy-address").textContent = display(
    record.messy_cleaned_address || messyAddress,
  );
  $("review-sticky-messy-postcode").textContent = display(messyPostcode, "");
  $("review-sticky-canonical-address").textContent = matched
    ? display(record.resolved_canonical_address)
    : "No accepted canonical match";
  $("review-sticky-canonical-postcode").textContent = matched
    ? display(record.resolved_canonical_postcode, "")
    : "";
  renderCanonicalAdditionalFields(
    $("review-canonical-additional-fields"),
    matched ? record.resolved_canonical_additional_columns : {},
  );
  const splink = record.match_stage === "splink",
    score = $("review-score");
  $("review-stage").replaceChildren(
    text(
      "span",
      record.match_stage || "Not available",
      `stage stage-${record.match_stage}`,
    ),
  );
  $("review-reason").textContent = display(record.match_reason);
  score.textContent = metric(record.match_weight, splink);
  score.className =
    splink && record.match_weight != null
      ? matchWeightClass(record.match_weight)
      : "";
  $("review-distinguishability").textContent = metric(
    record.distinguishability,
    splink,
  );
  const list = reviewCandidates(record);
  $("review-count").textContent = splink
    ? String(list.length)
    : "Not applicable";
  reviewElements.candidates.hidden = !splink;
  if (splink) {
    $("review-candidates-heading").textContent =
      `Candidates considered by reranker (${list.length})`;
    $("review-one-candidate").hidden = list.length !== 1;
    $("review-no-candidates").hidden = list.length !== 0;
    reviewElements.candidateBody.replaceChildren(
      ...list.map((candidate) => candidateRow(candidate)),
    );
  }
  renderCurrentDecision();
  updateReviewAccept();
}
function decisionPresentation(record) {
  const pending = state.review.pendingDecision;
  const decision = pending?.decision || record.current_decision;
  const details = pending || {
    label: currentDisplayLabel(record),
    address: record.current_label_address,
    postcode: record.current_label_postcode,
  };
  const presentations = {
    accept_model: ["accepted", "Model match accepted"],
    select_candidate: ["accepted", "Candidate match selected"],
    select_canonical: ["accepted", "Canonical match selected"],
    imported: ["accepted", "Model match accepted"],
    use_existing: ["neutral", "Existing label retained"],
    no_match: ["no-match", "No match"],
    uncertain: ["uncertain", "Marked uncertain"],
  };
  const [type, title] = presentations[decision] || ["neutral", "Not yet labelled"];
  return { type, title, ...details };
}
function renderCurrentDecision() {
  const record = state.review.record;
  if (!record) return;
  const decision = decisionPresentation(record);
  const panel = $("review-current-decision");
  panel.className = `current-decision current-decision-${decision.type}`;
  $("review-current-decision-icon").textContent =
    decision.type === "accepted"
      ? "\u2713"
      : decision.type === "uncertain"
        ? "?"
        : decision.type === "no-match"
          ? "\u00d7"
          : "";
  $("review-current-decision-title").textContent = decision.title;
  const label = $("review-current-decision-id");
  label.textContent = decision.label || "";
  label.parentElement.hidden = !decision.label;
  const address = [decision.address, decision.postcode].filter(Boolean).join(" \u00b7 ");
  const addressNode = $("review-current-decision-address");
  addressNode.textContent = address;
  addressNode.title = address;
  addressNode.hidden = !address;
  const persistence = $("review-current-decision-persistence");
  persistence.textContent = state.review.saving
    ? "Saving..."
    : state.review.saveFailed
      ? "Save failed"
      : decision.type === "neutral" && !decision.label
        ? ""
        : "\u2713 Saved";
  persistence.className = state.review.saveFailed
    ? "current-decision-save-failed"
    : "current-decision-persistence";
}
function candidateRow(candidate) {
  const row = document.createElement("tr"),
    radio = document.createElement("input"),
    selectCell = document.createElement("td");
  radio.type = "radio";
  radio.name = "review-candidate";
  radio.value = String(candidate.label_id);
  radio.checked = radio.value === state.review.selectedCandidateLabel;
  radio.setAttribute("aria-label", `Select candidate ${candidate.rank}`);
  radio.onchange = () => {
    state.review.selectedCandidateLabel = radio.value;
    renderReview();
  };
  if (candidate.is_model_selection) row.classList.add("candidate-model");
  if (radio.checked) row.classList.add("candidate-selected");
  const values = [
    candidate.rank,
    candidate.canonical_id || candidate.label_id,
    candidate.canonical_address,
    candidate.canonical_postcode,
    metric(candidate.splink_match_weight),
    metric(candidate.splink_match_probability),
    metric(candidate.rerank_adjustment),
    metric(candidate.match_weight),
    metric(candidate.distinguishability),
    candidate.is_model_selection ? "Selected" : "Not selected",
  ];
  selectCell.append(radio);
  row.append(selectCell);
  values.forEach((value, index) =>
    row.append(
      text(
        "td",
        display(value),
        index === 7 && candidate.match_weight != null
          ? matchWeightClass(candidate.match_weight)
          : "",
      ),
    ),
  );
  row.onclick = (event) => {
    if (event.target !== radio) {
      radio.checked = true;
      radio.onchange();
    }
  };
  return row;
}
function updateReviewAccept() {
  const record = state.review.record,
    candidate = selectedReviewCandidate(),
    canAccept =
      record &&
      (record.match_stage !== "splink" ? record.resolved_label_id : candidate);
  reviewElements.accept.hidden = !canAccept;
  const currentDecision = state.review.pendingDecision?.decision || record.current_decision;
  [["review-no-match", "no_match", "No match"], ["review-uncertain", "uncertain", "Uncertain"]].forEach(
    ([id, decision, label]) => {
      const button = $(id);
      const isSelected = currentDecision === decision;
      button.classList.toggle("is-selected", isSelected);
      button.setAttribute("aria-pressed", String(isSelected));
      button.textContent = isSelected ? `\u2713 ${label}` : label;
    },
  );
  if (!canAccept) return;
  const model = record.match_stage !== "splink" || candidate.is_model_selection;
  const selected = model
    ? currentDecision === "accept_model"
    : currentDecision === "select_candidate";
  reviewElements.accept.classList.toggle("is-selected", selected);
  reviewElements.accept.setAttribute("aria-pressed", String(selected));
  reviewElements.accept.textContent = selected
    ? model
      ? "\u2713 Model match accepted"
      : "\u2713 Candidate match selected"
    : model
      ? "Accept model match"
      : "Use selected candidate";
}
function prefetchNextReview(navigation, parameters) {
  if (!navigation.next_unique_id) return;
  const prefetchParameters = new URLSearchParams(parameters);
  prefetchParameters.set("unique_id", navigation.next_unique_id);
  api(`/api/review-record?${prefetchParameters}`).catch(() => {});
}
async function loadReview(
  id = reviewId() || sessionStorage.getItem("ukam-last-review-id"),
  includeCurrent = false,
) {
  if (!id) return showReviewEmpty();
  try {
    const parameters = new URLSearchParams(
      sessionStorage.getItem("ukam-review-filter-query") || reviewFilterQuery(),
    );
    parameters.set("unique_id", id);
    if (includeCurrent) parameters.set("include_current", "true");
    const payload = await api(`/api/review-record?${parameters}`);
    state.review.record = payload.record;
    state.review.navigation = payload.navigation;
    state.review.selectedCandidateLabel = initialCandidate(payload.record);
    state.review.pendingDecision = null;
    state.review.saveFailed = false;
    sessionStorage.setItem("ukam-last-review-id", payload.record.unique_id);
    renderReview();
    api(`/api/review-navigation?${parameters}`).then((navigation) => {
      if (state.review.record?.unique_id !== payload.record.unique_id) return;
      state.review.navigation = navigation;
      renderReview();
      prefetchNextReview(navigation, parameters);
    }).catch((error) => toast(error.message));
  } catch (error) {
    toast(error.message);
    showReviewEmpty();
  }
}
async function refreshSavedReview() {
  state.bootstrap = await api("/api/bootstrap");
  $("label-progress").textContent =
    `${state.bootstrap.labelled_records} / ${state.bootstrap.total_records}`;
  await load();
}
function showReviewComplete() {
  reviewElements.content.hidden = true;
  reviewElements.complete.hidden = false;
  resetReviewScroll();
}
let pendingReviewSaves = Promise.resolve();

function queueReviewSave(payload, displayLabel = null) {
  state.review.pendingDecision = {
    decision: payload.decision,
    label: displayLabel || payload.ukam_label,
    address: payload.clean_full_address,
    postcode: payload.postcode,
  };
  state.review.saveFailed = false;
  renderCurrentDecision();
  updateReviewAccept();
  el.save.textContent = "Saving...";
  const save = pendingReviewSaves
    .catch(() => {})
    .then(() =>
      api("/api/labels", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    );
  pendingReviewSaves = save;
  return save.then(
    () => {
      el.save.textContent = "Autosaved";
    },
    (error) => {
      el.save.textContent = "Save failed";
      state.review.saveFailed = true;
      renderCurrentDecision();
      toast(error.message);
      throw error;
    },
  );
}
function setReviewSaving(saving) {
  state.review.saving = saving;
  renderCurrentDecision();
  [
    reviewElements.accept,
    $("review-no-match"),
    $("review-uncertain"),
    $("review-clear"),
    $("review-use-existing"),
    reviewElements.undo,
  ].forEach((button) => {
    if (button) button.disabled = saving;
  });
}

function resetReviewScroll() {
  window.scrollTo({ top: 0, behavior: "auto" });
}
function advanceAfterReviewSave(nextId) {
  if (nextId) {
    location.hash = `review/${encodeURIComponent(nextId)}`;
  } else {
    showReviewComplete();
  }
  if (nextId) resetReviewScroll();
}
async function saveCanonicalSelection(canonicalRecord) {
  const record = state.review.record;
  if (!record || state.review.saving) {
    toast("Open a record in Review before selecting a canonical result.");
    return;
  }
  const nextId = state.review.navigation?.next_unique_id;
  const payload = {
    unique_id: record.unique_id,
    decision: "select_canonical",
    ukam_label: canonicalRecord.canonical_id,
    clean_full_address: canonicalRecord.cleaned_address || canonicalRecord.canonical_address,
    postcode: canonicalRecord.canonical_postcode,
    selected_candidate_rank: null,
    next_unique_id: nextId,
    review_query: reviewFilterQuery(),
  };
  setReviewSaving(true);
  try {
    await queueReviewSave(payload, canonicalRecord.canonical_unique_id);
    advanceAfterReviewSave(nextId);
  } catch {
    return;
  } finally {
    setReviewSaving(false);
  }
}
function navigateReview(direction) {
  const id =
    state.review.navigation?.[
      direction === "previous" ? "previous_unique_id" : "next_unique_id"
    ];
    if (id) {
      resetReviewScroll();
      location.hash = `review/${encodeURIComponent(id)}`;
    }
}
async function saveReviewDecision(decision) {
  const record = state.review.record,
    candidate = selectedReviewCandidate();
  if (!record || state.review.saving) return;
  const nextId = state.review.navigation?.next_unique_id;
  let payload = {
    unique_id: record.unique_id,
    decision,
    ukam_label: null,
    clean_full_address: null,
    postcode: null,
    selected_candidate_rank: null,
    next_unique_id: nextId,
    review_query: reviewFilterQuery(),
  };
  if (decision === "accept") {
    if (record.match_stage === "splink") {
      if (!candidate) return;
      payload.decision = candidate.is_model_selection
        ? "accept_model"
        : "select_candidate";
      payload.ukam_label = String(candidate.label_id);
      payload.clean_full_address = candidate.canonical_address;
      payload.postcode = candidate.canonical_postcode;
      payload.selected_candidate_rank = candidate.rank ?? null;
    } else {
      payload.decision = "accept_model";
      payload.ukam_label = record.resolved_label_id;
      payload.clean_full_address = record.resolved_canonical_address;
      payload.postcode = record.resolved_canonical_postcode;
      payload.selected_candidate_rank = 1;
    }
  }
  setReviewSaving(true);
  try {
    await queueReviewSave(
      payload,
      decision === "accept"
        ? record.match_stage === "splink"
          ? candidate?.canonical_id
          : record.resolved_canonical_id
        : null,
    );
    advanceAfterReviewSave(nextId);
  } catch {
    return;
  } finally {
    setReviewSaving(false);
  }
}
async function undoReviewDecision() {
  try {
    const payload = await api("/api/undo", {
      method: "POST",
      body: "{}",
    });
    state.bootstrap = await api("/api/bootstrap");
    $("label-progress").textContent =
      `${state.bootstrap.labelled_records} / ${state.bootstrap.total_records}`;
    await load();
    const reviewHash = `#review/${encodeURIComponent(payload.unique_id)}`;
    if (location.hash === reviewHash) await loadReview(payload.unique_id);
    else location.hash = reviewHash;
    toast("Last label action undone");
  } catch (error) {
    toast(error.message);
  }
}
function reviewHashChange() {
  if (location.hash.startsWith("#review")) {
    view("review");
    loadReview();
  }
}
addEventListener("hashchange", reviewHashChange);
$("review-overview").onclick = () => (location.hash = "overview");
reviewElements.previous.onclick = () => navigateReview("previous");
reviewElements.next.onclick = () => navigateReview("next");
reviewElements.undo.onclick = () => undoReviewDecision();
reviewElements.accept.onclick = () => saveReviewDecision("accept");
$("review-no-match").onclick = () => saveReviewDecision("no_match");
$("review-uncertain").onclick = () => saveReviewDecision("uncertain");
$("review-skip").onclick = () => {
  if (state.review.navigation?.next_unique_id) navigateReview("next");
  else showReviewComplete();
};
$("review-complete-previous").onclick = () => navigateReview("previous");
$("review-complete-overview").onclick = () => (location.hash = "overview");
if (location.hash.startsWith("#review")) loadReview();
(() => {
  const c = {
    unavailable: $("canonical-unavailable"),
    content: $("canonical-content"),
    uniqueId: $("canonical-unique-id"),
    postcode: $("canonical-postcode"),
    address: $("canonical-address-query"),
    usePostcode: $("canonical-use-review-postcode"),
    search: $("canonical-search"),
    clear: $("canonical-clear"),
    status: $("canonical-results-status"),
    page: $("canonical-page-number"),
    message: $("canonical-results-message"),
    table: $("canonical-results-table-shell"),
    headerRow: $("canonical-results-header-row"),
    actionHeading: $("canonical-action-heading"),
    body: $("canonical-results-body"),
    previous: $("canonical-previous"),
    next: $("canonical-next"),
    summary: $("canonical-pagination-summary"),
    pagination: $("canonical-pagination"),
  };
  const requiredCanonicalElements = [
    c.unavailable,
    c.content,
    c.uniqueId,
    c.postcode,
    c.address,
    c.usePostcode,
    c.search,
    c.clear,
    c.status,
    c.page,
    c.message,
    c.table,
    c.headerRow,
    c.actionHeading,
    c.body,
    c.previous,
    c.next,
    c.summary,
    c.pagination,
  ];
  if (requiredCanonicalElements.some((element) => !element)) return;
  state.canonical = {
    available: false,
    page: 1,
    rows: [],
    hasPrevious: false,
    hasNext: false,
    loading: false,
    initialisedReviewId: null,
    additionalColumns: [],
  };
  const format = (value) => Number(value).toLocaleString("en-GB");
  const reset = () => {
    c.body.replaceChildren();
    c.table.hidden = true;
    c.message.hidden = false;
    c.message.textContent = "Search results will appear here.";
    c.status.textContent =
      "Enter a unique ID, postcode, or address value to begin searching.";
    c.page.textContent = "Page 1";
    c.summary.textContent = "Page 1";
    c.previous.disabled = true;
    c.next.disabled = true;
  };
  const updatePagination = () => {
    c.previous.disabled =
      state.canonical.loading || !state.canonical.hasPrevious;
    c.next.disabled = state.canonical.loading || !state.canonical.hasNext;
    c.pagination.hidden =
      !state.canonical.hasPrevious && !state.canonical.hasNext;
  };
  const renderAdditionalHeadings = () => {
    c.headerRow
      .querySelectorAll("[data-canonical-additional-column]")
      .forEach((heading) => heading.remove());
    state.canonical.additionalColumns.forEach((column) => {
      const heading = text("th", canonicalColumnLabel(column));
      heading.dataset.canonicalAdditionalColumn = column;
      c.headerRow.insertBefore(heading, c.actionHeading);
    });
  };
  const appendHighlight = (node, value, needle) => {
    node.replaceChildren();
    const source = value == null ? "" : String(value),
      query = needle == null ? "" : String(needle);
    if (!query) {
      node.textContent = source || "Not available";
      return;
    }
    const lower = source.toLocaleLowerCase("en-GB"),
      term = query.toLocaleLowerCase("en-GB");
    let position = 0,
      index = lower.indexOf(term);
    while (index !== -1) {
      if (index > position)
        node.append(document.createTextNode(source.slice(position, index)));
      const mark = document.createElement("mark");
      mark.className = "search-highlight";
      mark.textContent = source.slice(index, index + query.length);
      node.append(mark);
      position = index + query.length;
      index = lower.indexOf(term, position);
    }
    if (position < source.length)
      node.append(document.createTextNode(source.slice(position)));
    if (!node.hasChildNodes()) node.textContent = source || "Not available";
  };
  const selectResult = (record) => {
    saveCanonicalSelection(record);
  };
  const renderRows = () => {
    c.body.replaceChildren();
    c.page.textContent = `Page ${format(state.canonical.page)}`;
    c.summary.textContent = c.page.textContent;
    if (!state.canonical.rows.length) {
      c.table.hidden = true;
      c.message.hidden = false;
      c.message.textContent = "No canonical records matched the search.";
      c.status.textContent = "No matching records";
      updatePagination();
      return;
    }
    c.message.hidden = true;
    c.table.hidden = false;
    const first = (state.canonical.page - 1) * 100 + 1,
      last = first + state.canonical.rows.length - 1;
    c.status.textContent = `Showing canonical records ${format(first)}-${format(last)}`;
    state.canonical.rows.forEach((record) => {
      const row = document.createElement("tr"),
        id = text(
          "td",
          record.canonical_unique_id || record.canonical_id,
          "primary",
        ),
        cleaned = document.createElement("td"),
        postcode = text("td", record.canonical_postcode || "-"),
        action = document.createElement("td"),
        button = text("button", "Use this record", "use-canonical-button");
      const additional = state.canonical.additionalColumns.map((column) =>
        text("td", record[column] || "-"),
      );
      appendHighlight(
        cleaned,
        record.cleaned_address,
        state.canonical.addressQuery,
      );
      button.type = "button";
      button.onclick = () => selectResult(record);
      action.append(button);
      row.append(id, cleaned, postcode, ...additional, action);
      row.ondblclick = () => selectResult(record);
      c.body.append(row);
    });
    updatePagination();
  };
  const load = async () => {
    if (!state.canonical.available) return;
    const uniqueIdQuery = c.uniqueId.value.trim(),
      postcode = c.postcode.value.trim(),
      addressQuery = c.address.value.trim();
    if (!uniqueIdQuery && !postcode && !addressQuery) {
      toast("Enter a unique ID, postcode, or address value.");
      return;
    }
    state.canonical.loading = true;
    c.search.disabled = true;
    c.message.hidden = false;
    c.table.hidden = true;
    c.message.textContent = "Searching canonical data...";
    updatePagination();
    try {
      const parameters = new URLSearchParams({
        page: String(state.canonical.page),
      });
      if (uniqueIdQuery) parameters.set("unique_id_query", uniqueIdQuery);
      if (postcode) parameters.set("postcode", postcode);
      if (addressQuery) parameters.set("address_query", addressQuery);
      const payload = await api(`/api/canonical-search?${parameters}`);
      state.canonical.page = payload.page;
      state.canonical.rows = payload.rows;
      state.canonical.hasPrevious = payload.has_previous;
      state.canonical.hasNext = payload.has_next;
      state.canonical.addressQuery = payload.address_query || "";
      state.canonical.additionalColumns = Array.isArray(
        payload.additional_canonical_columns,
      )
        ? payload.additional_canonical_columns
        : state.canonical.additionalColumns;
      renderAdditionalHeadings();
      renderRows();
    } catch (error) {
      c.message.hidden = false;
      c.table.hidden = true;
      c.message.textContent = error.message;
      toast(error.message);
    } finally {
      state.canonical.loading = false;
      c.search.disabled = false;
      updatePagination();
    }
  };
  const prepare = () => {
    if (!state.canonical.available) return;
    const record = state.review.record,
      reviewId = record?.unique_id || null,
      postcode = record?.messy_postcode || "";
    c.usePostcode.hidden = !postcode;
    if (reviewId && reviewId !== state.canonical.initialisedReviewId) {
      state.canonical.initialisedReviewId = reviewId;
      state.canonical.page = 1;
      state.canonical.rows = [];
      state.canonical.hasPrevious = false;
      state.canonical.hasNext = false;
      c.address.value = "";
      c.postcode.value = postcode;
      reset();
    }
  };
  const oldRender = renderReview;
  renderReview = () => {
    oldRender();
    prepare();
  };
  c.search.onclick = () => {
    state.canonical.page = 1;
    load();
  };
  c.clear.onclick = () => {
    c.postcode.value = "";
    c.address.value = "";
    state.canonical.page = 1;
    state.canonical.rows = [];
    state.canonical.hasPrevious = false;
    state.canonical.hasNext = false;
    reset();
  };
  c.usePostcode.onclick = () => {
    c.postcode.value = state.review.record?.messy_postcode || "";
  };
  c.previous.onclick = async () => {
    if (state.canonical.hasPrevious && !state.canonical.loading) {
      state.canonical.page--;
      await load();
      scrollTo({ top: 0, behavior: "smooth" });
    }
  };
  c.next.onclick = async () => {
    if (state.canonical.hasNext && !state.canonical.loading) {
      state.canonical.page++;
      await load();
      scrollTo({ top: 0, behavior: "smooth" });
    }
  };
  [c.uniqueId, c.postcode, c.address].forEach(
    (input) =>
      (input.onkeydown = (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          state.canonical.page = 1;
          load();
        }
      }),
  );
  const initialise = () => {
    if (!state.bootstrap) return setTimeout(initialise, 25);
    const config = state.bootstrap.canonical_search || {};
    state.canonical.available = Boolean(config.available);
    state.canonical.additionalColumns = Array.isArray(
      config.additional_canonical_columns,
    )
      ? config.additional_canonical_columns
      : [];
    renderAdditionalHeadings();
    c.unavailable.hidden = state.canonical.available;
    c.content.hidden = !state.canonical.available;
    prepare();
  };
  initialise();
})();
