"use strict";
const token = new URLSearchParams(location.search).get("token");
const state = {
  page: 1,
  pageSize: 20,
  maximumPage: 1,
  total: 0,
  rows: [],
  bootstrap: null,
  deadline: 0,
  lastActivity: 0,
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
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      "X-UKAM-Session-Token": token,
      ...options.headers,
    },
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw Error(data.error || `Request failed (${response.status})`);
  }
  return response.status === 204 ? null : response.json();
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
    };
  low.oninput = sync;
  high.oninput = sync;
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
  return Array.isArray(row.top_candidates)
    ? row.top_candidates.filter(
        (candidate) => candidate && candidate.label_id != null,
      )
    : [];
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
  const seen = new Set();
  const add = (value, label, name) => {
    if (seen.has(label)) return;
    seen.add(label);
    node.append(new Option(name, value, false, current === value));
  };
  if (row.imported_label)
    add(
      "existing",
      String(row.imported_label),
      `Existing label - ${row.imported_label}`,
    );
  if (row.resolved_label_id)
    add(
      "model",
      String(row.resolved_label_id),
      `Accept model - ${row.resolved_label_id}`,
    );
  candidates(row).forEach((candidate, index) =>
    add(
      `candidate:${index}`,
      String(candidate.label_id),
      `Candidate ${candidate.rank ?? index + 1} - ${candidate.label_id}`,
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
  location.hash = `review/${encodeURIComponent(id)}`;
  view("review");
}
function render() {
  el.body.replaceChildren();
  if (!state.rows.length) {
    const row = document.createElement("tr"),
      cell = text("td", "No records match the selected filters.");
    cell.colSpan = 8;
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
    suggestion.className = "canonical";
    if (record.resolved_label_id) {
      suggestion.append(
        text("div", record.resolved_label_id, "primary"),
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
      stageCell,
      text("td", weightText, matchWeightClass(record.match_weight)),
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
function activity() {
  if (!state.bootstrap) return;
  state.deadline = Date.now() + state.bootstrap.idle_timeout_seconds * 1000;
  if (Date.now() - state.lastActivity > 15000) {
    state.lastActivity = Date.now();
    api("/api/activity", { method: "POST", body: "{}" }).catch(expired);
  }
}
async function initialise() {
  if (!token) return expired();
  document.querySelectorAll(".tab").forEach(
    (button) =>
      (button.onclick = () => {
        location.hash = button.dataset.view;
        view(button.dataset.view);
      }),
  );
  addEventListener("hashchange", () =>
    view(
      location.hash.startsWith("#review")
        ? "review"
        : location.hash.startsWith("#canonical")
          ? "canonical"
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
    resetRanges();
    $("show-labelled").checked = true;
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
  ["pointerdown", "keydown", "change", "scroll"].forEach((name) =>
    addEventListener(name, activity, { passive: true }),
  );
  view(location.hash.startsWith("#review") ? "review" : "overview");
  try {
    state.bootstrap = await api("/api/bootstrap");
    $("bundle-name").textContent = state.bootstrap.bundle_name;
    $("label-progress").textContent =
      `${state.bootstrap.labelled_records} / ${state.bootstrap.total_records}`;
    buildStageFilters(state.bootstrap.stage_counts);
    activity();
    await load();
    setInterval(() => {
      const left = Math.ceil((state.deadline - Date.now()) / 1000);
      if (left <= 0) return expired();
      $("session-countdown").textContent =
        `${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")} remaining`;
    }, 1000);
  } catch (error) {
    expired(error);
  }
}
initialise();
state.review = {
  record: null,
  navigation: null,
  selectedCandidateLabel: null,
  loading: false,
};
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
  reviewElements.position.textContent = `Record ${navigation.position} of ${navigation.total}`;
  reviewElements.previous.disabled = !navigation.previous_unique_id;
  reviewElements.next.disabled = !navigation.next_unique_id;
  $("review-messy-id").textContent = display(record.unique_id);
  $("review-messy-address").textContent = display(record.messy_address);
  $("review-messy-cleaned").textContent = display(record.messy_cleaned_address);
  $("review-messy-postcode").textContent = display(record.messy_postcode);
  const matched = Boolean(
    record.resolved_label_id || record.resolved_canonical_id,
  );
  $("review-canonical-card").classList.toggle("empty-card", !matched);
  $("review-canonical-fields").hidden = !matched;
  $("review-no-canonical").hidden = matched;
  if (matched) {
    $("review-canonical-label").textContent = display(record.resolved_label_id);
    const showId =
      record.resolved_canonical_id &&
      String(record.resolved_canonical_id) !== String(record.resolved_label_id);
    $("review-canonical-id-term").hidden = !showId;
    $("review-canonical-id").hidden = !showId;
    $("review-canonical-id").textContent = display(
      record.resolved_canonical_id,
    );
    $("review-canonical-address").textContent = display(
      record.resolved_canonical_address,
    );
    $("review-canonical-postcode").textContent = display(
      record.resolved_canonical_postcode,
    );
  }
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
  $("review-current-label").textContent = record.current_label
    ? `Current label: ${record.current_label}`
    : record.is_labelled
      ? "A decision has been recorded."
      : "This record has not been labelled.";
  updateReviewAccept();
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
    candidate.label_id,
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
  if (!canAccept) return;
  const model = record.match_stage !== "splink" || candidate.is_model_selection;
  reviewElements.accept.textContent = model
    ? "Accept model match"
    : "Use selected candidate";
}
async function loadReview(
  id = reviewId() || sessionStorage.getItem("ukam-last-review-id"),
) {
  if (!id) return showReviewEmpty();
  try {
    const parameters = new URLSearchParams(
      sessionStorage.getItem("ukam-review-filter-query") || reviewFilterQuery(),
    );
    parameters.set("unique_id", id);
    const payload = await api(`/api/review-record?${parameters}`);
    state.review.record = payload.record;
    state.review.navigation = payload.navigation;
    state.review.selectedCandidateLabel = initialCandidate(payload.record);
    sessionStorage.setItem("ukam-last-review-id", payload.record.unique_id);
    renderReview();
  } catch (error) {
    toast(error.message);
    showReviewEmpty();
  }
}
function openReview(id) {
  sessionStorage.setItem("ukam-review-filter-query", reviewFilterQuery());
  sessionStorage.setItem("ukam-last-review-id", id);
  location.hash = `review/${encodeURIComponent(id)}`;
}
function navigateReview(direction) {
  const id =
    state.review.navigation?.[
      direction === "previous" ? "previous_unique_id" : "next_unique_id"
    ];
  if (id) location.hash = `review/${encodeURIComponent(id)}`;
}
async function saveReviewDecision(decision) {
  const record = state.review.record,
    candidate = selectedReviewCandidate();
  if (!record) return;
  let payload = {
    unique_id: record.unique_id,
    decision,
    ukam_label: null,
    selected_candidate_rank: null,
  };
  if (decision === "accept") {
    if (record.match_stage === "splink") {
      if (!candidate) return;
      payload.decision = candidate.is_model_selection
        ? "accept_model"
        : "select_candidate";
      payload.ukam_label = String(candidate.label_id);
      payload.selected_candidate_rank = candidate.rank ?? null;
    } else {
      payload.decision = "accept_model";
      payload.ukam_label = record.resolved_label_id;
      payload.selected_candidate_rank = 1;
    }
  }
  try {
    await api("/api/labels", { method: "POST", body: JSON.stringify(payload) });
    toast("Label saved");
    if (state.review.navigation.next_unique_id) navigateReview("next");
    else {
      reviewElements.content.hidden = true;
      reviewElements.complete.hidden = false;
    }
  } catch (error) {
    toast(error.message);
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
$("review-skip").onclick = () =>
  state.review.navigation?.next_unique_id
    ? navigateReview("next")
    : ((reviewElements.content.hidden = true),
      (reviewElements.complete.hidden = false));
$("review-complete-previous").onclick = () => navigateReview("previous");
$("review-complete-overview").onclick = () => (location.hash = "overview");
const existingReview = review;
if (typeof existingReview === "function") review = (id) => openReview(id);
if (location.hash.startsWith("#review")) loadReview();
(() => {
  const c = {
    unavailable: $("canonical-unavailable"),
    content: $("canonical-content"),
    reviewContext: $("canonical-record-context"),
    reviewMessyAddress: $("canonical-review-messy-address"),
    reviewMessyCleaned: $("canonical-review-messy-cleaned"),
    reviewMessyPostcode: $("canonical-review-messy-postcode"),
    back: $("canonical-back-to-review"),
    postcode: $("canonical-postcode"),
    address: $("canonical-address-query"),
    usePostcode: $("canonical-use-review-postcode"),
    search: $("canonical-search"),
    clear: $("canonical-clear"),
    status: $("canonical-results-status"),
    page: $("canonical-page-number"),
    message: $("canonical-results-message"),
    table: $("canonical-results-table-shell"),
    body: $("canonical-results-body"),
    previous: $("canonical-previous"),
    next: $("canonical-next"),
    summary: $("canonical-pagination-summary"),
    pagination: $("canonical-pagination"),
    reviewSearch: $("review-search-canonical"),
    selection: $("review-canonical-search-selection"),
    selectionId: $("review-search-selection-id"),
    selectionAddress: $("review-search-selection-address"),
    clearSelection: $("review-clear-search-selection"),
  };
  state.canonical = {
    available: false,
    page: 1,
    rows: [],
    hasPrevious: false,
    hasNext: false,
    loading: false,
    initialisedReviewId: null,
    pendingSelection: null,
  };
  const format = (value) => Number(value).toLocaleString("en-GB");
  const pending = (record) => {
    let selection = state.canonical.pendingSelection;
    if (!selection) {
      try {
        selection = JSON.parse(
          sessionStorage.getItem("ukam-pending-canonical-selection") || "null",
        );
      } catch {
        sessionStorage.removeItem("ukam-pending-canonical-selection");
      }
      if (selection) state.canonical.pendingSelection = selection;
    }
    return selection &&
      record &&
      String(selection.messy_unique_id) === String(record.unique_id)
      ? selection
      : null;
  };
  const clearPending = () => {
    state.canonical.pendingSelection = null;
    sessionStorage.removeItem("ukam-pending-canonical-selection");
    c.selection.hidden = true;
  };
  const reset = () => {
    c.body.replaceChildren();
    c.table.hidden = true;
    c.message.hidden = false;
    c.message.textContent = "Search results will appear here.";
    c.status.textContent =
      "Enter a postcode or address value to begin searching.";
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
    const reviewRecord = state.review.record;
    if (!reviewRecord) {
      toast("Open a record in Review before selecting a canonical result.");
      return;
    }
    const selection = {
      messy_unique_id: String(reviewRecord.unique_id),
      canonical_id: String(record.canonical_id),
      canonical_address: record.canonical_address,
      cleaned_address: record.cleaned_address,
      canonical_postcode: record.canonical_postcode,
    };
    state.canonical.pendingSelection = selection;
    sessionStorage.setItem(
      "ukam-pending-canonical-selection",
      JSON.stringify(selection),
    );
    toast("Canonical record selected");
    location.hash = `review/${encodeURIComponent(reviewRecord.unique_id)}`;
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
    c.status.textContent = `Showing unique canonical records ${format(first)}-${format(last)}`;
    state.canonical.rows.forEach((record) => {
      const row = document.createElement("tr"),
        id = text("td", record.canonical_id, "primary"),
        address = document.createElement("td"),
        cleaned = document.createElement("td"),
        postcode = text("td", record.canonical_postcode || "-"),
        action = document.createElement("td"),
        button = text("button", "Use this record", "use-canonical-button");
      appendHighlight(
        address,
        record.canonical_address,
        state.canonical.addressQuery,
      );
      appendHighlight(
        cleaned,
        record.cleaned_address,
        state.canonical.addressQuery,
      );
      button.type = "button";
      button.onclick = () => selectResult(record);
      action.append(button);
      row.append(id, address, cleaned, postcode, action);
      row.ondblclick = () => selectResult(record);
      c.body.append(row);
    });
    updatePagination();
  };
  const load = async () => {
    if (!state.canonical.available) return;
    const postcode = c.postcode.value.trim(),
      addressQuery = c.address.value.trim();
    if (!postcode && !addressQuery) {
      toast("Enter a postcode or address value.");
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
      if (postcode) parameters.set("postcode", postcode);
      if (addressQuery) parameters.set("address_query", addressQuery);
      const payload = await api(`/api/canonical-search?${parameters}`);
      state.canonical.page = payload.page;
      state.canonical.rows = payload.rows;
      state.canonical.hasPrevious = payload.has_previous;
      state.canonical.hasNext = payload.has_next;
      state.canonical.addressQuery = payload.address_query || "";
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
    c.reviewContext.hidden = !reviewId;
    c.usePostcode.hidden = !postcode;
    if (reviewId) {
      c.reviewMessyAddress.textContent = display(record.messy_address);
      c.reviewMessyCleaned.textContent = display(record.messy_cleaned_address);
      c.reviewMessyPostcode.textContent = display(record.messy_postcode);
    }
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
  const renderSelection = (record) => {
    const selection = pending(record);
    c.selection.hidden = !selection;
    if (!selection) return;
    c.selectionId.textContent = selection.canonical_id;
    c.selectionAddress.textContent = [
      selection.canonical_address,
      selection.canonical_postcode,
    ]
      .filter(Boolean)
      .join(" - ");
  };
  const oldRender = renderReview;
  renderReview = () => {
    oldRender();
    renderSelection(state.review.record);
  };
  const oldAccept = updateReviewAccept;
  updateReviewAccept = () => {
    const selection = pending(state.review.record);
    if (selection) {
      reviewElements.accept.hidden = false;
      reviewElements.accept.textContent = "Use canonical search selection";
      return;
    }
    oldAccept();
  };
  const oldSave = saveReviewDecision;
  saveReviewDecision = async (decision) => {
    const selection = pending(state.review.record);
    if (decision !== "accept" || !selection) return oldSave(decision);
    const record = state.review.record;
    try {
      await api("/api/labels", {
        method: "POST",
        body: JSON.stringify({
          unique_id: record.unique_id,
          decision: "select_canonical",
          ukam_label: selection.canonical_id,
          selected_candidate_rank: null,
        }),
      });
      clearPending();
      toast("Label saved");
      if (state.review.navigation.next_unique_id) navigateReview("next");
      else {
        reviewElements.content.hidden = true;
        reviewElements.complete.hidden = false;
      }
    } catch (error) {
      toast(error.message);
    }
  };
  $("review-candidate-body").addEventListener(
    "change",
    () => {
      if (pending(state.review.record)) {
        clearPending();
        updateReviewAccept();
      }
    },
    true,
  );
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
  c.back.onclick = () => {
    if (state.review.record)
      location.hash = `review/${encodeURIComponent(state.review.record.unique_id)}`;
  };
  c.reviewSearch.onclick = () => (location.hash = "canonical");
  c.clearSelection.onclick = () => {
    clearPending();
    updateReviewAccept();
  };
  [c.postcode, c.address].forEach(
    (input) =>
      (input.onkeydown = (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          state.canonical.page = 1;
          load();
        }
      }),
  );
  addEventListener("hashchange", () => {
    if (location.hash.startsWith("#canonical")) prepare();
  });
  const initialise = () => {
    if (!state.bootstrap) return setTimeout(initialise, 25);
    const config = state.bootstrap.canonical_search || {};
    state.canonical.available = Boolean(config.available);
    c.unavailable.hidden = state.canonical.available;
    c.content.hidden = !state.canonical.available;
    if (location.hash.startsWith("#canonical")) prepare();
  };
  initialise();
})();
