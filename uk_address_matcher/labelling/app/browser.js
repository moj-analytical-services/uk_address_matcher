import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";

const PAGE_SIZE = 100;
const REVIEW_RECORD_CACHE_SIZE = 2;
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

function uploadPath(file) {
  return String(file.webkitRelativePath || file.name || "").replaceAll("\\", "/");
}

function relativeUploadPath(file, root) {
  const path = uploadPath(file);
  return path.startsWith(`${root}/`) ? path.slice(root.length + 1) : path;
}

function safeManifestPath(path) {
  return typeof path === "string" && path.length > 0 &&
    !path.startsWith("/") && !path.split("/").includes("..") &&
    !path.split("/").includes("");
}

function uploadedFileMap(files, manifestName) {
  const manifestFile = files.find((file) => uploadPath(file).endsWith(`/${manifestName}`) || uploadPath(file) === manifestName);
  if (!manifestFile) throw new Error(`Selected folder must contain a root ${manifestName}.`);
  const path = uploadPath(manifestFile);
  const root = path.includes("/") ? path.split("/")[0] : "";
  if (root && path !== `${root}/${manifestName}`)
    throw new Error(`${manifestName} must be at the selected folder root.`);
  const byPath = new Map(files.map((file) => [relativeUploadPath(file, root), file]));
  return { manifestFile, byPath };
}

async function readManifest(files, manifestName, description) {
  const { manifestFile, byPath } = uploadedFileMap(files, manifestName);
  let manifest;
  try {
    manifest = JSON.parse(await manifestFile.text());
  } catch {
    throw new Error(`${description} manifest is not valid JSON.`);
  }
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest))
    throw new Error(`${description} manifest must contain a JSON object.`);
  return { manifestFile, manifest, byPath };
}

function bundleUpload(files) {
  return readManifest(files, "manifest.json", "Labelling bundle").then(({ manifestFile, manifest, byPath }) => {
    if (typeof manifest.bundle_id !== "string" || !manifest.bundle_id.trim())
      throw new Error("Labelling bundle manifest is missing bundle_id.");
    if (typeof manifest.uk_address_matcher_version !== "string" || !manifest.uk_address_matcher_version.trim())
      throw new Error("Labelling bundle manifest is missing uk_address_matcher_version.");
    const dataFiles = manifest.data_files ?? (manifest.data_file ? [manifest.data_file] : null);
    if (!Array.isArray(dataFiles) || !dataFiles.length || dataFiles.some((path) => !safeManifestPath(path)))
      throw new Error("Labelling bundle manifest must list review data files using safe relative paths.");
    const reviewFiles = dataFiles.map((path) => {
      const file = byPath.get(path);
      if (!file) throw new Error(`Labelling bundle is missing ${path}.`);
      if (![".csv", ".parquet"].includes(fileExtension(file)))
        throw new Error("Labelling bundle review data must be CSV or Parquet files.");
      return file;
    });
    const extensions = new Set(reviewFiles.map(fileExtension));
    if (extensions.size !== 1)
      throw new Error("All labelling bundle review data files must use the same format.");
    return { manifestFile, manifest, reviewFiles };
  });
}

function canonicalUpload(files) {
  return readManifest(files, "ukam_manifest.json", "Prepared canonical data").then(({ manifest, byPath }) => {
    if (typeof manifest.ukam_version !== "string" || !manifest.ukam_version.trim() ||
      typeof manifest.created_at !== "string" ||
      typeof manifest.created_with_duckdb_version !== "string")
      throw new Error("Prepared canonical manifest is missing required provenance fields.");
    if (!manifest.row_counts || typeof manifest.row_counts !== "object" ||
      !Number.isInteger(manifest.row_counts.canonical_addresses) ||
      !Number.isInteger(manifest.row_counts.canonical_output_chunks))
      throw new Error("Prepared canonical manifest has invalid row_counts.");
    if (!manifest.preparation_options || typeof manifest.preparation_options !== "object")
      throw new Error("Prepared canonical manifest is missing preparation_options.");
    if (!manifest.files || typeof manifest.files !== "object" || Array.isArray(manifest.files))
      throw new Error("Prepared canonical manifest is missing file metadata.");
    const manifestPaths = Object.entries(manifest.files).map(([path, metadata]) => {
      if (!safeManifestPath(path) || !metadata || typeof metadata !== "object" ||
        !Number.isInteger(metadata.size_bytes) && metadata.size_bytes !== null ||
        !(typeof metadata.sha256 === "string" || metadata.sha256 === null) ||
        !Array.isArray(metadata.columns))
        throw new Error(`Prepared canonical manifest has invalid metadata for ${path}.`);
      if (!byPath.has(path)) throw new Error(`Prepared canonical data is missing ${path}.`);
      return path;
    });
    const addressPaths = manifestPaths.filter((path) =>
      path === "ukam_canonical_addresses.parquet" ||
      (path.startsWith("ukam_canonical_addresses_chunks/") && path.endsWith(".parquet"))
    );
    if (!["ukam_term_frequencies.parquet", "ukam_inverted_index.parquet"].every((path) => manifestPaths.includes(path)))
      throw new Error("Prepared canonical manifest is missing required index files.");
    if (addressPaths.length !== manifest.row_counts.canonical_output_chunks || !addressPaths.length)
      throw new Error("Prepared canonical manifest does not describe the expected canonical address files.");
    return {
      manifestFile: files.find((file) => uploadPath(file).endsWith("/ukam_manifest.json") || uploadPath(file) === "ukam_manifest.json"),
      manifest,
      canonicalFiles: addressPaths.sort().map((path) => byPath.get(path)),
    };
  });
}

export async function validateUploadedFolders(bundleFiles, canonicalFiles) {
  if (!bundleFiles?.length) throw new Error("Select a labelling bundle folder.");
  const bundle = await bundleUpload([...bundleFiles]);
  const canonical = canonicalFiles?.length
    ? await canonicalUpload([...canonicalFiles])
    : { canonicalFiles: [] };
  return {
    manifestFile: bundle.manifestFile,
    manifest: bundle.manifest,
    reviewFiles: bundle.reviewFiles,
    canonicalFiles: canonical.canonicalFiles,
  };
}

function sourceSql(names, extension) {
  const files = Array.isArray(names) ? names : [names];
  const source = files.length === 1
    ? sqlString(files[0])
    : `[${files.map(sqlString).join(", ")}]`;
  if (extension === ".parquet") return `read_parquet(${source})`;
  if (extension === ".csv") return `read_csv_auto(${source})`;
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

export class BrowserLabellingStore {
  constructor(manifest, reviewFiles, canonicalFiles, options = {}) {
    this.manifest = manifest;
    this.reviewFiles = Array.isArray(reviewFiles) ? reviewFiles : [reviewFiles];
    this.canonicalFiles = canonicalFiles;
    this.eventsUrl = options.eventsUrl || null;
    this.canonicalSearchUrl = options.canonicalSearchUrl || null;
    this.events = [];
    this.db = null;
    this.connection = null;
    this.worker = null;
    this.reviewSource = null;
    this.canonicalSource = null;
    this.canonical = null;
    this.canonicalLoading = null;
    this.canonicalColumns = [];
    this.reviewColumns = [];
    this.reviewNavigationCache = new Map();
    this.reviewRecordCache = new Map();
    this.reviewRecordLoads = new Map();
  }

  async initialise() {
    if (!this.reviewFiles.length)
      throw new Error("Select at least one bundle review data file.");
    const extensions = new Set(this.reviewFiles.map(fileExtension));
    if (extensions.size !== 1 || ![".csv", ".parquet"].includes([...extensions][0]))
      throw new Error("Review data must be a CSV or Parquet file.");
    const extension = [...extensions][0];
    this.worker = new Worker(duckdbWorker);
    this.db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), this.worker);
    try {
      await this.db.instantiate(duckdbWasm);
      this.connection = await this.db.connect();
      const reviewNames = [];
      for (const [index, file] of this.reviewFiles.entries()) {
        const reviewName = `review_data_${index}${extension}`;
        reviewNames.push(reviewName);
        await this.db.registerFileBuffer(
          reviewName,
          new Uint8Array(await file.arrayBuffer()),
        );
      }
      this.reviewSource = sourceSql(reviewNames, extension);
      const reviewColumns = await this.columns(this.reviewSource);
      this.reviewColumns = reviewColumns;
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
      if (this.eventsUrl) {
        const response = await fetch(this.eventsUrl);
        if (!response.ok) throw new Error("Could not load saved labelling events.");
        const payload = await response.json();
        if (!Array.isArray(payload.events))
          throw new Error("Saved labelling events are not valid.");
        this.events = payload.events;
      }
      await this.syncEvents();
      return this;
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  async close() {
    const connection = this.connection;
    const worker = this.worker;
    this.connection = null;
    this.db = null;
    this.worker = null;
    this.reviewSource = null;
    this.canonicalSource = null;
    this.canonicalLoading = null;
    try {
      if (connection) await connection.close();
    } finally {
      if (worker) worker.terminate();
    }
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
      "CREATE TEMP TABLE label_events (event_id VARCHAR, bundle_id VARCHAR, unique_id VARCHAR, decision VARCHAR, ukam_label VARCHAR, clean_full_address VARCHAR, postcode VARCHAR, selected_candidate_rank BIGINT, created_at_utc TIMESTAMPTZ)",
    );
    for (const event of this.events) {
      const rank = event.selected_candidate_rank == null ? "NULL" : String(event.selected_candidate_rank);
      const label = event.ukam_label == null ? "NULL" : sqlString(event.ukam_label);
      const cleanAddress = event.clean_full_address == null ? "NULL" : sqlString(event.clean_full_address);
      const postcode = event.postcode == null ? "NULL" : sqlString(event.postcode);
      await this.queryRows(
        `INSERT INTO label_events VALUES (${sqlString(event.event_id)}, ${sqlString(event.bundle_id)}, ${sqlString(event.unique_id)}, ${sqlString(event.decision)}, ${label}, ${cleanAddress}, ${postcode}, ${rank}, CAST(${sqlString(event.created_at_utc)} AS TIMESTAMPTZ))`,
      );
    }
  }

  async insertEvent(event) {
    const rank = event.selected_candidate_rank == null
      ? "NULL"
      : String(event.selected_candidate_rank);
    const label = event.ukam_label == null ? "NULL" : sqlString(event.ukam_label);
    const cleanAddress = event.clean_full_address == null ? "NULL" : sqlString(event.clean_full_address);
    const postcode = event.postcode == null ? "NULL" : sqlString(event.postcode);
    await this.queryRows(
      `INSERT INTO label_events VALUES (${sqlString(event.event_id)}, ${sqlString(event.bundle_id)}, ${sqlString(event.unique_id)}, ${sqlString(event.decision)}, ${label}, ${cleanAddress}, ${postcode}, ${rank}, CAST(${sqlString(event.created_at_utc)} AS TIMESTAMPTZ))`,
    );
  }

  async deleteEvent(eventId) {
    await this.queryRows(
      `DELETE FROM label_events WHERE event_id = ${sqlString(eventId)}`,
    );
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
    const labelId = lower.get(this.manifest.canonical_label_column) || lower.get("unique_id");
    if (!labelId)
      throw new Error(`Canonical data is missing ${this.manifest.canonical_label_column}.`);
    this.canonical = {
      labelId,
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
    const existingLabelAddress = this.reviewColumns.includes(
      "ukam_label_clean_full_address",
    )
      ? "r.ukam_label_clean_full_address"
      : "NULL::VARCHAR";
    const existingLabelPostcode = this.reviewColumns.includes(
      "ukam_label_postcode",
    )
      ? "r.ukam_label_postcode"
      : "NULL::VARCHAR";
    return `
      WITH latest_labels AS (
        SELECT event_id, unique_id, decision, ukam_label, clean_full_address, postcode,
          selected_candidate_rank
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
           ${existingLabelAddress} AS imported_label_clean_full_address,
           ${existingLabelPostcode} AS imported_label_postcode,
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
             WHEN COALESCE(r.has_existing_label, FALSE) THEN CAST(r.ukam_label AS VARCHAR) END AS current_label,
           CASE WHEN l.decision IN ('clear', 'no_match', 'uncertain') THEN NULL
              WHEN l.ukam_label IS NOT NULL THEN l.clean_full_address
              WHEN COALESCE(r.has_existing_label, FALSE) THEN ${existingLabelAddress}
              END AS current_label_clean_full_address,
           CASE WHEN l.decision IN ('clear', 'no_match', 'uncertain') THEN NULL
              WHEN l.ukam_label IS NOT NULL THEN l.postcode
              WHEN COALESCE(r.has_existing_label, FALSE) THEN ${existingLabelPostcode}
              END AS current_label_postcode
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
      add("current_label IS NOT NULL AND resolved_canonical_id IS NOT NULL AND current_label IS DISTINCT FROM resolved_canonical_id");
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
      total_records: Number(summary[0].total_records),
      labelled_records: Number(summary[0].labelled_records),
      stage_counts: Object.fromEntries(
        stages.filter((row) => STAGES.has(String(row.match_stage))).map((row) => [String(row.match_stage), Number(row.count)]),
      ),
      score_bounds: { minimum: summary[0].minimum_score, maximum: summary[0].maximum_score },
      distinguishability_bounds: { minimum: summary[0].minimum_distinguishability, maximum: summary[0].maximum_distinguishability },
      canonical_search: {
        available: Boolean(
          this.canonicalFiles.length || this.canonicalSource || this.canonicalSearchUrl,
        ),
        source_name: this.canonicalFiles[0]?.name || null,
        page_size: PAGE_SIZE,
        additional_canonical_columns: this.canonical?.additional || [],
        warning: this.canonicalFiles.length || this.canonicalSource || this.canonicalSearchUrl
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
    const rows = await this.queryRows(`${this.baseReviewCte()} SELECT unique_id, messy_address, messy_cleaned_address, messy_postcode, imported_label, has_existing_label, resolved_canonical_id, resolved_label_id, resolved_canonical_address, resolved_canonical_postcode, match_reason, match_stage, is_matched, match_weight, distinguishability, splink_match_weight, candidate_count, CAST(top_candidates AS JSON) AS top_candidates, current_decision, current_label, current_label_clean_full_address, current_label_postcode, selected_candidate_rank, is_labelled FROM base ${where} ORDER BY ${this.orderSql(parameters)} LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`);
    rows.forEach((row) => (row.top_candidates = normaliseCandidates(row.top_candidates)));
    return { page, page_size: pageSize, maximum_page: maximumPage, total_filtered: total, rows };
  }

  async canonicalRecord(label) {
    if (!label) return null;
    if (this.canonicalSearchUrl) {
      const result = await this.searchCanonical(
        new URLSearchParams({ unique_id_query: label, page: "1" }),
      );
      return result.rows.find((row) => String(row.canonical_id) === label) || null;
    }
    if (!this.canonicalSource) return null;
    const [row] = await this.queryRows(`SELECT CAST(${sqlIdentifier(this.canonical.labelId)} AS VARCHAR) AS canonical_id, CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR) AS canonical_unique_id, CAST(${sqlIdentifier(this.canonical.displayAddress)} AS VARCHAR) AS canonical_address, CAST(${sqlIdentifier(this.canonical.cleanedAddress)} AS VARCHAR) AS cleaned_address, CAST(${sqlIdentifier(this.canonical.postcode)} AS VARCHAR) AS canonical_postcode ${this.canonical.additional.map((column) => `, CAST(${sqlIdentifier(column)} AS VARCHAR) AS ${sqlIdentifier(column)}`).join("")} FROM ${this.canonicalSource} WHERE CAST(${sqlIdentifier(this.canonical.labelId)} AS VARCHAR) = ${sqlString(label)} LIMIT 1`);
    return row || null;
  }

  additionalCanonicalValues(record) {
    if (!record || !this.canonical) return {};
    return Object.fromEntries(this.canonical.additional.filter((column) => record[column] != null && record[column] !== "").map((column) => [column, record[column]]));
  }

  async cachedReviewRecord(uniqueId) {
    const cached = this.reviewRecordCache.get(uniqueId);
    if (cached) {
      this.reviewRecordCache.delete(uniqueId);
      this.reviewRecordCache.set(uniqueId, cached);
      return cached;
    }
    let loading = this.reviewRecordLoads.get(uniqueId);
    if (!loading) {
      loading = this.loadReviewRecord(uniqueId);
      this.reviewRecordLoads.set(uniqueId, loading);
    }
    try {
      const record = await loading;
      this.reviewRecordCache.set(uniqueId, record);
      while (this.reviewRecordCache.size > REVIEW_RECORD_CACHE_SIZE)
        this.reviewRecordCache.delete(this.reviewRecordCache.keys().next().value);
      return record;
    } finally {
      this.reviewRecordLoads.delete(uniqueId);
    }
  }

  async loadReviewRecord(uniqueId) {
    const [row] = await this.queryRows(`${this.baseReviewCte()} SELECT unique_id, messy_address, messy_cleaned_address, messy_postcode, imported_label, current_decision, current_label, current_label_clean_full_address, current_label_postcode, is_labelled, resolved_canonical_id, resolved_label_id, resolved_canonical_address, resolved_canonical_postcode, match_reason, match_stage, is_matched, match_weight, distinguishability, candidate_count, CAST(top_candidates AS JSON) AS candidates FROM base WHERE unique_id = ${sqlString(uniqueId)} LIMIT 1`);
    if (!row) throw new Error("The requested record does not exist");
    const candidates = normaliseCandidates(row.candidates);
    const currentDetails = idEquals(row.current_label, row.resolved_label_id)
      ? {
          canonical_address: row.resolved_canonical_address,
          canonical_postcode: row.resolved_canonical_postcode,
        }
      : candidates.find((candidate) => idEquals(candidate.label_id, row.current_label));
    row.current_label_address = row.current_label_clean_full_address || currentDetails?.canonical_address || null;
    row.current_label_postcode = row.current_label_postcode || currentDetails?.canonical_postcode || null;
    row.current_label_additional_columns = {};
    row.resolved_canonical_additional_columns = {};
    row.candidates = candidates;
    return row;
  }

  async reviewRecord(parameters) {
    const uniqueId = parameters.get("unique_id")?.trim();
    if (!uniqueId) throw new Error("unique_id is required");
    const row = await this.cachedReviewRecord(uniqueId);
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
    const [row] = await this.queryRows(`SELECT CAST(resolved_label_id AS VARCHAR) AS resolved_label_id, CAST(resolved_canonical_address AS VARCHAR) AS resolved_canonical_address, CAST(resolved_canonical_postcode AS VARCHAR) AS resolved_canonical_postcode, CAST(ukam_label AS VARCHAR) AS imported_label, CAST(top_candidates AS JSON) AS top_candidates FROM ${this.reviewSource} WHERE CAST(unique_id AS VARCHAR) = ${sqlString(uniqueId)} LIMIT 1`);
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
    let selectedCanonical = null;
    if (decision === "select_canonical") {
      if (!this.canonicalSource && !this.canonicalSearchUrl)
        throw new Error("Canonical data is required to select a canonical-search result");
      selectedCanonical = label ? await this.canonicalRecord(label) : null;
      if (!selectedCanonical) throw new Error("The selected canonical ID does not exist in the configured canonical data");
      rank = null;
    }
    if (decision === "use_existing" && (!row.imported_label || label !== row.imported_label)) throw new Error("The submitted label does not match the imported label");
    if (["no_match", "uncertain", "clear"].includes(decision)) {
      label = null;
      rank = null;
    }
    if (rank != null && !Number.isInteger(rank)) throw new Error("selected_candidate_rank must be an integer");
    const selectedCandidate = label === row.resolved_label_id
      ? {
          canonical_address: row.resolved_canonical_address,
          canonical_postcode: row.resolved_canonical_postcode,
        }
      : candidates.find((candidate) => String(candidate.label_id) === label);
    const event = {
      event_id: crypto.randomUUID(),
      bundle_id: this.manifest.bundle_id,
      unique_id: uniqueId,
      decision,
      ukam_label: label,
      clean_full_address: selectedCanonical?.cleaned_address || selectedCandidate?.canonical_address || null,
      postcode: selectedCanonical?.canonical_postcode || selectedCandidate?.canonical_postcode || null,
      selected_candidate_rank: rank,
      created_at_utc: new Date().toISOString(),
    };
    this.events.push(event);
    try {
      await this.insertEvent(event);
      if (this.eventsUrl) await this.persistEvent(event);
      this.reviewNavigationCache.clear();
      this.reviewRecordCache.delete(uniqueId);
    } catch (error) {
      this.events = this.events.filter((item) => item.event_id !== event.event_id);
      await this.deleteEvent(event.event_id);
      this.reviewNavigationCache.clear();
      throw error;
    }
    return event;
  }

  async undo() {
    if (!this.events.length) throw new Error("There are no label actions to undo");
    const event = [...this.events].sort((left, right) => `${right.created_at_utc}|${right.event_id}`.localeCompare(`${left.created_at_utc}|${left.event_id}`))[0];
    this.events = this.events.filter((item) => item.event_id !== event.event_id);
    try {
      await this.deleteEvent(event.event_id);
      if (this.eventsUrl) {
        const response = await fetch(
          `${this.eventsUrl}?event_id=${encodeURIComponent(event.event_id)}`,
          { method: "DELETE" },
        );
        if (!response.ok)
          throw new Error("Could not persist the undone labelling event.");
      }
      this.reviewNavigationCache.clear();
      this.reviewRecordCache.delete(event.unique_id);
    } catch (error) {
      this.events.push(event);
      await this.insertEvent(event);
      throw error;
    }
    const [row] = await this.queryRows(`${this.baseReviewCte()} SELECT current_label FROM base WHERE unique_id = ${sqlString(event.unique_id)}`);
    return { undone_event_id: event.event_id, unique_id: event.unique_id, ukam_label: row?.current_label || null };
  }

  async persistEvent(event) {
    const response = await fetch(this.eventsUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
    });
    if (!response.ok) throw new Error("Could not save the labelling event.");
  }

  async request(url, options = {}) {
    const parsed = new URL(url, location.href);
    if (parsed.pathname === "/api/bootstrap") return this.bootstrap();
    if (parsed.pathname === "/api/records") return this.records(parsed.searchParams);
    if (parsed.pathname === "/api/review-record") return this.reviewRecord(parsed.searchParams);
    if (parsed.pathname === "/api/review-navigation") return this.reviewNavigation(parsed.searchParams);
    if (parsed.pathname === "/api/canonical-search") return this.searchCanonical(parsed.searchParams);
    if (parsed.pathname === "/api/activity") return null;
    if (parsed.pathname === "/api/labels") return this.saveLabel(JSON.parse(options.body || "{}"));
    if (parsed.pathname === "/api/undo") return this.undo();
    throw new Error(`Unsupported browser request: ${parsed.pathname}`);
  }

  async searchCanonical(parameters) {
    if (this.canonicalSearchUrl) {
      const url = new URL(this.canonicalSearchUrl, location.href);
      url.search = parameters.toString();
      const response = await fetch(url);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Canonical search failed.");
      return payload;
    }
    await this.ensureCanonicalData();
    if (!this.canonicalSource) throw new Error("Canonical data is not available");
    const uniqueId = parameters.get("unique_id_query")?.trim() || "";
    const postcode = normalisePostcode(parameters.get("postcode"));
    let address = parameters.get("address_query")?.trim() || "";
    if (uniqueId.length > 100 || address.length > 100) throw new Error("Search values must contain no more than 100 characters");
    if (!uniqueId && !postcode && !address) throw new Error("Enter a unique ID, postcode, or address value before searching.");
    const conditions = [`${sqlIdentifier(this.canonical.labelId)} IS NOT NULL`];
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
    const rows = await this.queryRows(`SELECT CAST(${sqlIdentifier(this.canonical.labelId)} AS VARCHAR) AS canonical_id, CAST(${sqlIdentifier(this.canonical.uniqueId)} AS VARCHAR) AS canonical_unique_id, CAST(${sqlIdentifier(this.canonical.displayAddress)} AS VARCHAR) AS canonical_address, CAST(${sqlIdentifier(this.canonical.cleanedAddress)} AS VARCHAR) AS cleaned_address, CAST(${sqlIdentifier(this.canonical.postcode)} AS VARCHAR) AS canonical_postcode ${this.canonical.additional.map((column) => `, CAST(${sqlIdentifier(column)} AS VARCHAR) AS ${sqlIdentifier(column)}`).join("")} FROM ${this.canonicalSource} WHERE ${conditions.join(" AND ")} ORDER BY canonical_postcode, cleaned_address, canonical_address, canonical_id LIMIT ${PAGE_SIZE + 1} OFFSET ${(page - 1) * PAGE_SIZE}`);
    return { page, page_size: PAGE_SIZE, has_previous: page > 1, has_next: rows.length > PAGE_SIZE, unique_id_query: uniqueId, postcode, address_query: address, additional_canonical_columns: this.canonical.additional, rows: rows.slice(0, PAGE_SIZE) };
  }

  async downloadLabels() {
    const rows = await this.queryRows(
      `${this.baseReviewCte()} SELECT unique_id, CAST(messy_address AS VARCHAR) AS address_name, CAST(current_label AS VARCHAR) AS ukam_label, current_label IS NOT NULL AS label_available FROM base ORDER BY unique_id`,
    );
    const csvValue = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const lines = [
      ["unique_id", "address_name", "ukam_label", "label_available"],
      ...rows.map((row) => [
        row.unique_id,
        row.address_name,
        row.ukam_label,
        row.label_available ? "TRUE" : "FALSE",
      ]),
    ].map((row) => row.map(csvValue).join(","));
    const blob = new Blob([lines.join("\r\n") + "\r\n"], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${this.manifest.bundle_id}-labels.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  }
}

export async function loadBrowserStore(
  manifestFile,
  reviewFiles,
  canonicalFiles,
  options = {},
) {
  const files = Array.isArray(reviewFiles) ? reviewFiles : [reviewFiles];
  if (!manifestFile || !files[0])
    throw new Error("Select a bundle manifest and at least one review data file.");
  let manifest;
  try {
    manifest = JSON.parse(await manifestFile.text());
  } catch {
    throw new Error("Bundle manifest is not valid JSON.");
  }
  if (!manifest || typeof manifest.bundle_id !== "string" || !manifest.bundle_id.trim()) throw new Error("Bundle manifest is missing bundle_id.");
  return new BrowserLabellingStore(
    manifest,
    files,
    canonicalFiles,
    options,
  ).initialise();
}
