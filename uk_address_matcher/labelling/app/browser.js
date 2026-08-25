import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";

const PAGE_SIZE = 100;
const PREPARED_CANONICAL_FILE_NAMES = (file) => {
  const name = String(file.name || "").toLowerCase();
  return (
    name === "ukam_canonical_addresses.parquet" ||
    (name.startsWith("canonical_addresses_chunk_") && name.endsWith(".parquet"))
  );
};
const ALLOWED_DECISIONS = new Set([
  "accept_model",
  "select_candidate",
  "select_canonical",
  "use_existing",
  "no_match",
  "uncertain",
  "clear",
]);
const REQUIRED_REVIEW_COLUMNS = new Set([
  "bundle_id",
  "uk_address_matcher_version",
  "created_at_utc",
  "unique_id",
  "messy_address",
  "messy_postcode",
  "ukam_label",
  "has_existing_label",
  "resolved_canonical_id",
  "resolved_label_id",
  "resolved_canonical_address",
  "resolved_canonical_postcode",
  "match_reason",
  "match_stage",
  "is_matched",
  "match_weight",
  "distinguishability",
  "candidate_count",
  "top_candidates",
]);
const STAGES = new Set(["exact", "peeled", "splink", "unique_trigram", "unmatched"]);
const SORT_COLUMNS = {
  unique_id: "unique_id",
  reranked_score: "match_weight",
  splink_score: "splink_match_weight",
  distinguishability: "distinguishability",
};

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function sqlIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function fileExtension(file) {
  const match = String(file.name).toLowerCase().match(/\.[^.]+$/);
  return match ? match[0] : "";
}

function sourceSql(name, extension) {
  if (extension === ".parquet") return `read_parquet(${sqlString(name)})`;
  if (extension === ".csv") return `read_csv_auto(${sqlString(name)})`;
  throw new Error("Selected data must be a CSV or Parquet file.");
}

function canonicalSourceSql(names) {
  return `read_parquet([${names.map(sqlString).join(", ")}])`;
}

function normaliseValue(value) {
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(normaliseValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, normaliseValue(item)]),
    );
  }
  return value;
}

function normaliseCandidates(value) {
  if (value && typeof value.toJSON === "function") value = value.toJSON();
  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(value) && value && typeof value[Symbol.iterator] === "function")
    value = [...value];
  return Array.isArray(value)
    ? value.filter((item) => item && typeof item === "object")
    : [];
}

function rowsFromResult(result) {
  return result.toArray().map((row) => {
    const value = typeof row.toJSON === "function" ? row.toJSON() : row;
    return normaliseValue(value);
  });
}

function openEventsDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("ukam-labelling-events", 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("events", { keyPath: "event_id" });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB could not be opened"));
  });
}

function idbRequest(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB request failed"));
  });
}

class EventStore {
  async load(bundleId) {
    const database = await openEventsDatabase();
    const transaction = database.transaction("events", "readonly");
    const events = await idbRequest(transaction.objectStore("events").getAll());
    database.close();
    return events.filter((event) => event.bundle_id === bundleId);
  }

  async put(event) {
    const database = await openEventsDatabase();
    const transaction = database.transaction("events", "readwrite");
    await idbRequest(transaction.objectStore("events").put(event));
    database.close();
  }

  async delete(eventId) {
    const database = await openEventsDatabase();
    const transaction = database.transaction("events", "readwrite");
    await idbRequest(transaction.objectStore("events").delete(eventId));
    database.close();
  }
}

function bool(value) {
  return value === true || value === 1 || value === "true";
}

function parseBoolean(value, fallback) {
  if (value == null) return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function normalisePostcode(value) {
  const compact = String(value || "").replaceAll(/\s/g, "").toUpperCase();
  if (!compact) return null;
  if (compact.length > 16) throw new Error("Postcode search is too long.");
  return compact.length <= 3 ? compact : `${compact.slice(0, -3)} ${compact.slice(-3)}`;
}

function validPostcode(value) {
  return /^(?:GIR 0AA|[A-Z][A-HJ-Y]?\d[A-Z\d]? \d[A-Z]{2})$/i.test(
    String(value || "").trim(),
  );
}

function idEquals(left, right) {
  return left != null && right != null && String(left) === String(right);
}

function latestEvents(events) {
  const latest = new Map();
  events.forEach((event) => {
    const current = latest.get(event.unique_id);
    if (
      !current ||
      `${event.created_at_utc}|${event.event_id}` >
        `${current.created_at_utc}|${current.event_id}`
    )
      latest.set(event.unique_id, event);
  });
  return latest;
}

export class BrowserLabellingStore {
  constructor(manifest, reviewFile, canonicalFiles, options = {}) {
    this.manifest = manifest;
    this.reviewFile = reviewFile;
    this.canonicalFiles = canonicalFiles;
    this.remoteEventsUrl = options.remoteEventsUrl || null;
    this.nativeCanonicalSearchUrl = options.nativeCanonicalSearchUrl || null;
    this.eventsStore = new EventStore();
    this.events = [];
    this.db = null;
    this.connection = null;
    this.reviewSource = null;
    this.canonicalSource = null;
    this.canonical = null;
    this.canonicalLoading = null;
    this.canonicalColumns = [];
    this.reviewNavigationCache = new Map();
  }

  async initialise() {
    const extension = fileExtension(this.reviewFile);
    if (![".csv", ".parquet"].includes(extension))
      throw new Error("Review data must be a CSV or Parquet file.");
    const worker = new Worker(duckdbWorker);
    this.db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await this.db.instantiate(duckdbWasm);
    this.connection = await this.db.connect();
    const reviewName = `review_data${extension}`;
    await this.db.registerFileBuffer(
      reviewName,
      new Uint8Array(await this.reviewFile.arrayBuffer()),
    );
    this.reviewSource = sourceSql(reviewName, extension);
    const reviewColumns = await this.columns(this.reviewSource);
    const missing = [...REQUIRED_REVIEW_COLUMNS].filter(
      (column) => !reviewColumns.includes(column),
    );
    if (missing.length)
      throw new Error(`The labelling bundle is missing required columns: ${missing.join(", ")}`);
    const invalidBundleRows = await this.queryRows(
      `SELECT COUNT(*) AS count FROM ${this.reviewSource} WHERE CAST(bundle_id AS VARCHAR) <> ${sqlString(this.manifest.bundle_id)}`,
    );
    if (Number(invalidBundleRows[0]?.count || 0))
      throw new Error("The selected review data does not belong to this bundle manifest.");
    this.events = await this.eventsStore.load(this.manifest.bundle_id);
    if (this.remoteEventsUrl) {
      const response = await fetch(this.remoteEventsUrl);
      if (!response.ok) throw new Error("The local labelling event store could not be loaded.");
      const payload = await response.json();
      const remoteEvents = Array.isArray(payload.events) ? payload.events : [];
      const eventsById = new Map(
        [...this.events, ...remoteEvents].map((event) => [event.event_id, event]),
      );
      this.events = [...eventsById.values()];
      for (const event of this.events) await this.eventsStore.put(event);
    }
    await this.syncEvents();
    return this;
  }

  async columns(source) {
    const rows = await this.queryRows(`DESCRIBE SELECT * FROM ${source}`);
    return rows.map((row) => String(row.column_name));
  }

  async queryRows(query) {
    return rowsFromResult(await this.connection.query(query));
  }

  async syncEvents() {
    await this.queryRows("DROP TABLE IF EXISTS label_events");
    await this.queryRows(
      "CREATE TEMP TABLE label_events (event_id VARCHAR, bundle_id VARCHAR, unique_id VARCHAR, decision VARCHAR, ukam_label VARCHAR, selected_candidate_rank BIGINT, created_at_utc TIMESTAMPTZ)",
    );
    for (const event of this.events) {
      const rank = event.selected_candidate_rank == null ? "NULL" : String(event.selected_candidate_rank);
      const label = event.ukam_label == null ? "NULL" : sqlString(event.ukam_label);
      await this.queryRows(
        `INSERT INTO label_events VALUES (${sqlString(event.event_id)}, ${sqlString(event.bundle_id)}, ${sqlString(event.unique_id)}, ${sqlString(event.decision)}, ${label}, ${rank}, CAST(${sqlString(event.created_at_utc)} AS TIMESTAMPTZ))`,
      );
    }
  }

  async loadCanonicalData() {
    if (!this.canonicalFiles.length) return;
    const names = [];
    for (const [index, file] of this.canonicalFiles.entries()) {
      if (fileExtension(file) !== ".parquet")
        throw new Error("Canonical data must be supplied as Parquet files.");
      const name = `canonical-${index}.parquet`;
      names.push(name);
      if (file.url) {
        await this.db.registerFileURL(
          name,
          file.url,
          duckdb.DuckDBDataProtocol.HTTP,
          false,
        );
      } else if (typeof file.slice === "function" && typeof file.size === "number") {
        await this.db.registerFileHandle(
          name,
          file,
          duckdb.DuckDBDataProtocol.BROWSER_FILEREADER,
          true,
        );
      } else {
        await this.db.registerFileBuffer(
          name,
          new Uint8Array(await file.arrayBuffer()),
        );
      }
    }
    this.canonicalSource = canonicalSourceSql(names);
    this.canonicalColumns = await this.columns(this.canonicalSource);
    const lower = new Map(this.canonicalColumns.map((column) => [column.toLowerCase(), column]));
    if (!lower.has("unique_id") || !lower.has("postcode"))
      throw new Error("Canonical data is missing required unique_id or postcode columns.");
    const cleaned = ["clean_full_address", "cleaned_full_address"].find((column) =>
      lower.has(column),
    );
    if (!cleaned)
      throw new Error("Canonical data must contain clean_full_address or cleaned_full_address.");
    this.canonical = {
      uniqueId: lower.get("unique_id"),
      postcode: lower.get("postcode"),
      cleanedAddress: lower.get(cleaned),
      displayAddress: [
        "original_address_concat",
        "address_concat",
        "clean_full_address",
        "cleaned_full_address",
      ].find((column) => lower.has(column))
        ? lower.get(
            [
              "original_address_concat",
              "address_concat",
              "clean_full_address",
              "cleaned_full_address",
            ].find((column) => lower.has(column)),
          )
        : lower.get(cleaned),
      additional: ["classificationcode", "floorlevel"].filter((column) =>
        lower.has(column),
      ).map((column) => lower.get(column)),
      prepared: this.canonicalFiles.length > 0 &&
        this.canonicalFiles.every(PREPARED_CANONICAL_FILE_NAMES),
    };
  }

  async ensureCanonicalData() {
    if (this.canonicalSource || !this.canonicalFiles.length) return;
    if (!this.canonicalLoading) this.canonicalLoading = this.loadCanonicalData();
    await this.canonicalLoading;
  }

  baseReviewCte() {
    return `
      WITH latest_labels AS (
        SELECT event_id, unique_id, decision, ukam_label, selected_candidate_rank
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY unique_id ORDER BY created_at_utc DESC, event_id DESC
          ) AS event_rank FROM label_events
        ) WHERE event_rank = 1
      ), base AS (
        SELECT CAST(r.unique_id AS VARCHAR) AS unique_id, r.messy_address,
          r.messy_cleaned_address, r.messy_postcode,
          CAST(r.ukam_label AS VARCHAR) AS imported_label,
          COALESCE(r.has_existing_label, FALSE) AS has_existing_label,
          CAST(r.resolved_canonical_id AS VARCHAR) AS resolved_canonical_id,
          CAST(r.resolved_label_id AS VARCHAR) AS resolved_label_id,
          r.resolved_canonical_address, r.resolved_canonical_postcode,
          r.match_reason, r.match_stage, r.is_matched, r.match_weight,
          r.distinguishability,
          TRY_CAST(json_extract_string(CAST(r.top_candidates AS JSON), '$[0].splink_match_weight') AS DOUBLE) AS splink_match_weight,
          r.candidate_count, r.top_candidates,
          l.decision AS saved_decision, l.selected_candidate_rank,
          CASE WHEN l.decision = 'clear' THEN FALSE
               WHEN l.decision IS NOT NULL THEN TRUE
               ELSE COALESCE(r.has_existing_label, FALSE) END AS is_labelled,
          CASE WHEN l.decision = 'clear' THEN NULL
               WHEN l.decision IS NOT NULL THEN l.decision
               WHEN COALESCE(r.has_existing_label, FALSE) THEN 'imported' END AS current_decision,
          CASE WHEN l.decision IN ('clear', 'no_match', 'uncertain') THEN NULL
               WHEN l.ukam_label IS NOT NULL THEN l.ukam_label
               WHEN COALESCE(r.has_existing_label, FALSE) THEN CAST(r.ukam_label AS VARCHAR) END AS current_label
        FROM ${this.reviewSource} AS r LEFT JOIN latest_labels AS l
          ON CAST(r.unique_id AS VARCHAR) = l.unique_id
      )`;
  }

  filterSql(parameters) {
    const conditions = [];
    const add = (condition) => conditions.push(condition);
    const uniqueId = parameters.get("unique_id_query")?.trim() || "";
    const address = parameters.get("address_query")?.trim() || "";
    if (uniqueId) add(`contains(upper(unique_id), upper(${sqlString(uniqueId)}))`);
    if (address) {
      const value = sqlString(address);
      add(`(contains(upper(COALESCE(CAST(messy_address AS VARCHAR), '')), upper(${value})) OR contains(upper(COALESCE(CAST(messy_cleaned_address AS VARCHAR), '')), upper(${value})) OR contains(upper(COALESCE(CAST(messy_postcode AS VARCHAR), '')), upper(${value})))`);
    }
    const stages = parameters.getAll("stage");
    if (stages.length) {
      stages.forEach((stage) => {
        if (!STAGES.has(stage)) throw new Error("Unsupported match stage");
      });
      add(`match_stage IN (${stages.map(sqlString).join(", ")})`);
    }
    for (const [column, key, operator] of [
      ["match_weight", "score_min", ">="],
      ["match_weight", "score_max", "<="],
      ["distinguishability", "distinguishability_min", ">="],
      ["distinguishability", "distinguishability_max", "<="],
    ]) {
      const value = parameters.get(key);
      if (value) {
        if (!Number.isFinite(Number(value))) throw new Error(`${key} must be numeric`);
        add(`(match_stage != 'splink' OR ${column} ${operator} ${Number(value)})`);
      }
    }
    const showLabelled = parseBoolean(parameters.get("show_labelled"), true);
    const mismatchesOnly = parseBoolean(parameters.get("mismatches_only"), false);
    if (!showLabelled && !mismatchesOnly) add("is_labelled = FALSE");
    if (mismatchesOnly)
      add("match_stage <> 'unmatched' AND has_existing_label AND is_matched AND imported_label IS NOT NULL AND resolved_label_id IS NOT NULL AND resolved_label_id IS DISTINCT FROM imported_label");
    return conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  }

  orderSql(parameters) {
    const sort = parameters.get("sort_by") || "unique_id";
    const direction = (parameters.get("sort_order") || "asc").toLowerCase();
    if (!SORT_COLUMNS[sort] || !["asc", "desc"].includes(direction))
      throw new Error("Unsupported record sort");
    return `${SORT_COLUMNS[sort]} ${direction.toUpperCase()} NULLS LAST, unique_id ASC`;
  }

  async bootstrap() {
    const summary = await this.queryRows(`${this.baseReviewCte()} SELECT COUNT(*) AS total_records, COUNT(*) FILTER (WHERE is_labelled) AS labelled_records, MIN(match_weight) AS minimum_score, MAX(match_weight) AS maximum_score, MIN(distinguishability) AS minimum_distinguishability, MAX(distinguishability) AS maximum_distinguishability FROM base`);
    const stages = await this.queryRows(`SELECT match_stage, COUNT(*) AS count FROM ${this.reviewSource} GROUP BY match_stage`);
    return {
      bundle_name: this.manifest.bundle_id,
      bundle_id: this.manifest.bundle_id,
      idle_timeout_seconds: 0,
      total_records: Number(summary[0].total_records),
      labelled_records: Number(summary[0].labelled_records),
      stage_counts: Object.fromEntries(
        stages.filter((row) => STAGES.has(String(row.match_stage))).map((row) => [String(row.match_stage), Number(row.count)]),
      ),
      score_bounds: { minimum: summary[0].minimum_score, maximum: summary[0].maximum_score },
      distinguishability_bounds: { minimum: summary[0].minimum_distinguishability, maximum: summary[0].maximum_distinguishability },
      canonical_search: {
        available: Boolean(
          this.nativeCanonicalSearchUrl || this.canonicalFiles.length || this.canonicalSource,
        ),
        source_name: this.canonicalFiles[0]?.name || null,
        page_size: PAGE_SIZE,
        additional_canonical_columns: this.canonical?.additional || [],
        warning: this.nativeCanonicalSearchUrl || this.canonicalFiles.length || this.canonicalSource
          ? null
          : "Select canonical Parquet files when loading the bundle to enable canonical search.",
      },
    };
  }

  async records(parameters) {
    const pageSize = Number(parameters.get("page_size") || 20);
    if (![10, 20, 50, 100].includes(pageSize)) throw new Error("Unsupported page size");
    const where = this.filterSql(parameters);
    const totalRows = await this.queryRows(`${this.baseReviewCte()} SELECT COUNT(*) AS count FROM base ${where}`);
    const total = Number(totalRows[0].count);
    const maximumPage = Math.max(1, Math.ceil(total / pageSize));
    const page = Math.min(Math.max(1, Number(parameters.get("page") || 1)), maximumPage);
    const rows = await this.queryRows(`${this.baseReviewCte()} SELECT unique_id, messy_address, messy_cleaned_address, messy_postcode, imported_label, has_existing_label, resolved_label_id, resolved_canonical_address, resolved_canonical_postcode, match_reason, match_stage, is_matched, match_weight, distinguishability, splink_match_weight, candidate_count, CAST(top_candidates AS JSON) AS top_candidates, current_decision, current_label, selected_candidate_rank, is_labelled FROM base ${where} ORDER BY ${this.orderSql(parameters)} LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`);
    rows.forEach((row) => (row.top_candidates = normaliseCandidates(row.top_candidates)));
    return { page, page_size: pageSize, maximum_page: maximumPage, total_filtered: total, rows };
  }

  async canonicalRecord(label) {
    if (!label) return null;
    if (this.nativeCanonicalSearchUrl) {
      const url = new URL(this.nativeCanonicalSearchUrl, location.href);
      url.searchParams.set("unique_id_query", label);
      url.searchParams.set("page", "1");
      const response = await fetch(url);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Canonical search failed");
      return (
        (payload.rows || []).find((row) => idEquals(row.canonical_id, label)) ||
        null
      );
    }
    if (!this.canonicalSource) return null;
    const [row] = await this.queryRows(`SELECT CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR) AS canonical_id, CAST(${sqlIdentifier(this.canonical.displayAddress)} AS VARCHAR) AS canonical_address, CAST(${sqlIdentifier(this.canonical.cleanedAddress)} AS VARCHAR) AS cleaned_address, CAST(${sqlIdentifier(this.canonical.postcode)} AS VARCHAR) AS canonical_postcode ${this.canonical.additional.map((column) => `, CAST(${sqlIdentifier(column)} AS VARCHAR) AS ${sqlIdentifier(column)}`).join("")} FROM ${this.canonicalSource} WHERE CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR) = ${sqlString(label)} LIMIT 1`);
    return row || null;
  }

  additionalCanonicalValues(record) {
    if (!record || !this.canonical) return {};
    return Object.fromEntries(this.canonical.additional.filter((column) => record[column] != null && record[column] !== "").map((column) => [column, record[column]]));
  }

  async reviewRecord(parameters) {
    const uniqueId = parameters.get("unique_id")?.trim();
    if (!uniqueId) throw new Error("unique_id is required");
    const [row] = await this.queryRows(`${this.baseReviewCte()} SELECT unique_id, messy_address, messy_cleaned_address, messy_postcode, imported_label, current_decision, current_label, is_labelled, resolved_canonical_id, resolved_label_id, resolved_canonical_address, resolved_canonical_postcode, match_reason, match_stage, is_matched, match_weight, distinguishability, candidate_count, CAST(top_candidates AS JSON) AS candidates FROM base WHERE unique_id = ${sqlString(uniqueId)} LIMIT 1`);
    if (!row) throw new Error("The requested record does not exist");
    const candidates = normaliseCandidates(row.candidates);
    const currentDetails = idEquals(row.current_label, row.resolved_label_id)
      ? {
          canonical_address: row.resolved_canonical_address,
          canonical_postcode: row.resolved_canonical_postcode,
        }
      : candidates.find((candidate) => idEquals(candidate.label_id, row.current_label));
    row.current_label_address = currentDetails?.canonical_address || null;
    row.current_label_postcode = currentDetails?.canonical_postcode || null;
    row.current_label_additional_columns = {};
    row.resolved_canonical_additional_columns = {};
    row.candidates = candidates;
    return {
      record: row,
      navigation: {
        position: null,
        total: null,
        previous_unique_id: null,
        next_unique_id: null,
      },
    };
  }

  async reviewNavigation(parameters) {
    const uniqueId = parameters.get("unique_id")?.trim();
    if (!uniqueId) throw new Error("unique_id is required");
    const filter = this.filterSql(parameters);
    const includeCurrent = parseBoolean(parameters.get("include_current"), false);
    const navigationParameters = new URLSearchParams(parameters);
    navigationParameters.delete("unique_id");
    const navigationKey = `${navigationParameters}|${includeCurrent ? uniqueId : ""}`;
    let navigationIds = this.reviewNavigationCache.get(navigationKey);
    if (!navigationIds) {
      const navigationFilter = includeCurrent && filter
        ? `WHERE (${filter.replace(/^WHERE /, "")}) OR unique_id = ${sqlString(uniqueId)}`
        : filter;
      const navigationRows = await this.queryRows(
        `${this.baseReviewCte()} SELECT unique_id FROM base ${navigationFilter} ORDER BY ${this.orderSql(parameters)}`,
      );
      navigationIds = navigationRows.map((row) => String(row.unique_id));
      this.reviewNavigationCache.set(navigationKey, navigationIds);
    }
    const reviewIndex = navigationIds.indexOf(uniqueId);
    if (reviewIndex === -1)
      throw new Error("The requested record does not exist in the current filtered review set");
    return {
      position: reviewIndex + 1,
      total: navigationIds.length,
      previous_unique_id: navigationIds[reviewIndex - 1] || null,
      next_unique_id: navigationIds[reviewIndex + 1] || null,
    };
  }

  async recordForValidation(uniqueId) {
    const [row] = await this.queryRows(`SELECT CAST(resolved_label_id AS VARCHAR) AS resolved_label_id, CAST(ukam_label AS VARCHAR) AS imported_label, CAST(top_candidates AS JSON) AS top_candidates FROM ${this.reviewSource} WHERE CAST(unique_id AS VARCHAR) = ${sqlString(uniqueId)} LIMIT 1`);
    if (!row) throw new Error(`Unknown messy unique_id: ${uniqueId}`);
    return row;
  }

  async saveLabel(payload) {
    const uniqueId = String(payload.unique_id || "").trim();
    const decision = String(payload.decision || "").trim();
    let label = payload.ukam_label == null ? null : String(payload.ukam_label);
    let rank = payload.selected_candidate_rank == null ? null : Number(payload.selected_candidate_rank);
    if (!uniqueId) throw new Error("unique_id is required");
    if (!ALLOWED_DECISIONS.has(decision)) throw new Error(`Unsupported decision: ${decision}`);
    const row = await this.recordForValidation(uniqueId);
    const candidates = normaliseCandidates(row.top_candidates);
    const candidateRanks = new Map(candidates.filter((item) => item.label_id != null).map((item) => [String(item.label_id), item.rank]));
    if (decision === "accept_model" && (!row.resolved_label_id || label !== row.resolved_label_id)) throw new Error("The submitted label does not match the model-selected label");
    if (decision === "select_candidate") {
      if (!candidateRanks.has(label)) throw new Error("The submitted label is not one of the exported candidates");
      if (rank == null) rank = candidateRanks.get(label);
      if (rank !== candidateRanks.get(label)) throw new Error("The submitted candidate rank does not match the candidate");
    }
    if (decision === "select_canonical") {
      if (!this.canonicalSource && !this.nativeCanonicalSearchUrl)
        throw new Error("Canonical data is required to select a canonical-search result");
      if (!label || !(await this.canonicalRecord(label))) throw new Error("The selected canonical ID does not exist in the configured canonical data");
      rank = null;
    }
    if (decision === "use_existing" && (!row.imported_label || label !== row.imported_label)) throw new Error("The submitted label does not match the imported label");
    if (["no_match", "uncertain", "clear"].includes(decision)) {
      label = null;
      rank = null;
    }
    if (rank != null && !Number.isInteger(rank)) throw new Error("selected_candidate_rank must be an integer");
    const event = {
      event_id: crypto.randomUUID(),
      bundle_id: this.manifest.bundle_id,
      unique_id: uniqueId,
      decision,
      ukam_label: label,
      selected_candidate_rank: rank,
      created_at_utc: new Date().toISOString(),
    };
    this.events.push(event);
    await this.eventsStore.put(event);
    try {
      if (this.remoteEventsUrl) {
        const response = await fetch(this.remoteEventsUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(event),
        });
        if (!response.ok) throw new Error("The local labelling event could not be saved.");
      }
      await this.syncEvents();
      this.reviewNavigationCache.clear();
    } catch (error) {
      this.events = this.events.filter((item) => item.event_id !== event.event_id);
      await this.eventsStore.delete(event.event_id);
      await this.syncEvents();
      this.reviewNavigationCache.clear();
      throw error;
    }
    return event;
  }

  async undo() {
    if (!this.events.length) throw new Error("There are no label actions to undo");
    const event = [...this.events].sort((left, right) => `${right.created_at_utc}|${right.event_id}`.localeCompare(`${left.created_at_utc}|${left.event_id}`))[0];
    this.events = this.events.filter((item) => item.event_id !== event.event_id);
    await this.eventsStore.delete(event.event_id);
    try {
      if (this.remoteEventsUrl) {
        const response = await fetch(`${this.remoteEventsUrl}?event_id=${encodeURIComponent(event.event_id)}`, {
          method: "DELETE",
        });
        if (!response.ok) throw new Error("The local labelling event could not be removed.");
      }
      await this.syncEvents();
    } catch (error) {
      this.events.push(event);
      await this.eventsStore.put(event);
      await this.syncEvents();
      throw error;
    }
    const [row] = await this.queryRows(`${this.baseReviewCte()} SELECT current_label FROM base WHERE unique_id = ${sqlString(event.unique_id)}`);
    return { undone_event_id: event.event_id, unique_id: event.unique_id, ukam_label: row?.current_label || null };
  }

  async request(url, options = {}) {
    const parsed = new URL(url, location.href);
    if (parsed.pathname === "/api/bootstrap") return this.bootstrap();
    if (parsed.pathname === "/api/records") return this.records(parsed.searchParams);
    if (parsed.pathname === "/api/review-record") return this.reviewRecord(parsed.searchParams);
    if (parsed.pathname === "/api/review-navigation") return this.reviewNavigation(parsed.searchParams);
    if (parsed.pathname === "/api/canonical-search") {
      if (!this.nativeCanonicalSearchUrl) return this.searchCanonical(parsed.searchParams);
      const response = await fetch(`${this.nativeCanonicalSearchUrl}?${parsed.searchParams}`);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Canonical search failed");
      return payload;
    }
    if (parsed.pathname === "/api/activity") return null;
    if (parsed.pathname === "/api/labels") return this.saveLabel(JSON.parse(options.body || "{}"));
    if (parsed.pathname === "/api/undo") return this.undo();
    throw new Error(`Unsupported browser request: ${parsed.pathname}`);
  }

  async searchCanonical(parameters) {
    await this.ensureCanonicalData();
    if (!this.canonicalSource) throw new Error("Canonical data is not available");
    const uniqueId = parameters.get("unique_id_query")?.trim() || "";
    const postcode = normalisePostcode(parameters.get("postcode"));
    let address = parameters.get("address_query")?.trim() || "";
    if (uniqueId.length > 100 || address.length > 100) throw new Error("Search values must contain no more than 100 characters");
    if (!uniqueId && !postcode && !address) throw new Error("Enter a unique ID, postcode, or address value before searching.");
    const conditions = [`${sqlIdentifier(this.canonical.uniqueId)} IS NOT NULL`];
    if (uniqueId) conditions.push(`contains(upper(CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR)), upper(${sqlString(uniqueId)}))`);
    if (postcode) {
      const compact = postcode.replaceAll(" ", "");
      if (this.canonical.prepared) {
        const expression = sqlIdentifier(this.canonical.postcode);
        conditions.push(
          validPostcode(postcode)
            ? `${expression} = ${sqlString(postcode)}`
            : `contains(replace(${expression}, ' ', ''), ${sqlString(compact)})`,
        );
      } else {
        const expression = `upper(replace(CAST(${sqlIdentifier(this.canonical.postcode)} AS VARCHAR), ' ', ''))`;
        conditions.push(
          validPostcode(postcode)
            ? `${expression} = upper(${sqlString(compact)})`
            : `contains(${expression}, upper(${sqlString(compact)}))`,
        );
      }
    }
    address = address.split(/\s+/).filter(Boolean).join(" ");
    address.split(" ").filter(Boolean).forEach((token) => {
      conditions.push(
        this.canonical.prepared
          ? `contains(${sqlIdentifier(this.canonical.cleanedAddress)}, ${sqlString(token.toUpperCase())})`
          : `contains(upper(CAST(${sqlIdentifier(this.canonical.cleanedAddress)} AS VARCHAR)), upper(${sqlString(token)}))`,
      );
    });
    const page = Math.max(1, Number(parameters.get("page") || 1));
    const rows = await this.queryRows(`SELECT CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR) AS canonical_id, CAST(${sqlIdentifier(this.canonical.displayAddress)} AS VARCHAR) AS canonical_address, CAST(${sqlIdentifier(this.canonical.cleanedAddress)} AS VARCHAR) AS cleaned_address, CAST(${sqlIdentifier(this.canonical.postcode)} AS VARCHAR) AS canonical_postcode ${this.canonical.additional.map((column) => `, CAST(${sqlIdentifier(column)} AS VARCHAR) AS ${sqlIdentifier(column)}`).join("")} FROM ${this.canonicalSource} WHERE ${conditions.join(" AND ")} ORDER BY canonical_postcode, cleaned_address, canonical_address, canonical_id LIMIT ${PAGE_SIZE + 1} OFFSET ${(page - 1) * PAGE_SIZE}`);
    return { page, page_size: PAGE_SIZE, has_previous: page > 1, has_next: rows.length > PAGE_SIZE, unique_id_query: uniqueId, postcode, address_query: address, additional_canonical_columns: this.canonical.additional, rows: rows.slice(0, PAGE_SIZE) };
  }

  downloadUpdates() {
    const payload = {
      schema_version: 1,
      bundle_id: this.manifest.bundle_id,
      exported_at_utc: new Date().toISOString(),
      events: this.events,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2) + "\n"], { type: "application/json" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${this.manifest.bundle_id}-labelling-updates.json`;
    link.click();
    URL.revokeObjectURL(link.href);
  }
}

export async function loadBrowserStore(
  manifestFile,
  reviewFile,
  canonicalFiles,
  options = {},
) {
  if (!manifestFile || !reviewFile) throw new Error("Select both a bundle manifest and review data file.");
  let manifest;
  try {
    manifest = JSON.parse(await manifestFile.text());
  } catch {
    throw new Error("Bundle manifest is not valid JSON.");
  }
  if (!manifest || typeof manifest.bundle_id !== "string" || !manifest.bundle_id.trim()) throw new Error("Bundle manifest is missing bundle_id.");
  return new BrowserLabellingStore(
    manifest,
    reviewFile,
    canonicalFiles,
    options,
  ).initialise();
}
