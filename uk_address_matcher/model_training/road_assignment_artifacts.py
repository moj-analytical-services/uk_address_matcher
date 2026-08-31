"""Build and query deployable road-phrase artifacts from prepared canonical data."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import duckdb

from uk_address_matcher.cleaning.steps.road_resources import (
    facility_clause_removal_sql,
    sql_text,
    suffix_peel_regex_sql_literal,
    token_pattern,
    token_policy,
)

from .additive_pairwise_road_assignment import (
    ADDITIVE_FEATURES,
    additive_feature_sql,
)
from .all_sector_model import FEATURE_COLUMNS
from .linear_road_assignment import FoldedLogisticModel, logistic_logit_sql

AMBIGUOUS_ADDRESS_PATTERN = (
    "CARAVAN|HOUSE BOAT|HOUSEBOAT|BEACH HUT|TENNIS.*(FROM|UNNAMED ROAD)|"
    "(^|[^A-Z])REAR( OF)?([^A-Z]|$)"
)
BLOCK_CANDIDATE_PATTERN = "(^| )BLOCK( |$)"
FACILITY_CANDIDATE_PATTERN = "SHOPPING CENTRE|INDUSTRIAL ESTATE|INDUSTRIAL PARK"
DEFAULT_EXCLUDED_CLASSIFICATION_PREFIXES = (
    "L",
    "M",
    "O",
    "P",
    "U",
    "X",
    "Z",
    "RG",
    "RB",
    "RC",
)
ARTIFACT_VERSION = 1


@dataclass(frozen=True)
class FoldedRankerArtifact:
    model: FoldedLogisticModel
    feature_definitions: tuple[tuple[str, str], ...]
    model_type: str


def _tree_rule_feature_definitions(
    rules: object,
) -> tuple[tuple[str, str], ...]:
    if not isinstance(rules, list):
        raise ValueError("The tree-rule ranker artifact is missing rules")
    definitions: list[tuple[str, str]] = []
    for index, rule in enumerate(rules, start=1):
        if not isinstance(rule, dict) or not isinstance(rule.get("conditions"), list):
            raise ValueError("The tree-rule ranker artifact has an invalid rule")
        conditions: list[str] = []
        for condition in rule["conditions"]:
            if not isinstance(condition, dict):
                raise ValueError("The tree-rule ranker artifact has an invalid condition")
            feature = condition.get("feature")
            operator = condition.get("operator")
            if feature not in FEATURE_COLUMNS or operator not in {"<=", ">"}:
                raise ValueError("The tree-rule ranker artifact has an invalid condition")
            conditions.append(
                f"{{alias}}.{feature}::DOUBLE {operator} "
                f"{float(condition['threshold'])!r}"
            )
        if not conditions:
            raise ValueError("The tree-rule ranker artifact has an empty rule")
        definitions.append(
            (
                f"tree_rule_{index:03d}",
                "CASE WHEN " + " AND ".join(conditions) + " THEN 1.0 ELSE 0.0 END",
            )
        )
    return tuple(definitions)


def load_folded_ranker_model(model_path: Path) -> FoldedRankerArtifact:
    """Load the JSON scorecard used by the DuckDB-native serving path."""
    artifact: dict[str, Any] = json.loads(model_path.read_text(encoding="utf-8"))
    model_type = artifact.get("model_type")
    if model_type == "additive_pairwise_logistic_candidate_ranker":
        feature_definitions = ADDITIVE_FEATURES
    elif model_type == "tree_rule_distilled_additive_pairwise_ranker":
        feature_definitions = ADDITIVE_FEATURES + _tree_rule_feature_definitions(
            artifact.get("rules")
        )
    else:
        raise ValueError("The folded ranker artifact has an unsupported model type")
    feature_columns = tuple(name for name, _ in feature_definitions)
    if artifact.get("feature_columns") != list(feature_columns):
        raise ValueError("The folded ranker feature contract does not match this runtime")
    coefficients = artifact.get("coefficients")
    if not isinstance(coefficients, dict):
        raise ValueError("The folded ranker artifact is missing coefficients")
    return FoldedRankerArtifact(
        model=FoldedLogisticModel(
            intercept=float(artifact["intercept"]),
            coefficients={
                feature: float(coefficients[feature]) for feature in feature_columns
            },
        ),
        feature_definitions=feature_definitions,
        model_type=model_type,
    )


def canonical_candidate_sql(
    source_relation: str,
    *,
    classification_code_column: str | None = None,
    excluded_classification_prefixes: tuple[str, ...] = (),
) -> str:
    """Return candidate SQL for canonical rows with precomputed numeric tokens."""
    suffix_pattern = suffix_peel_regex_sql_literal()
    raw_text_pattern = sql_text(AMBIGUOUS_ADDRESS_PATTERN)
    block_candidate_pattern = sql_text(BLOCK_CANDIDATE_PATTERN)
    facility_candidate_pattern = sql_text(FACILITY_CANDIDATE_PATTERN)
    road_terminal_pattern = sql_text(
        token_pattern(tuple(token_policy()["road_syntax_terminal_tokens"]))
    )
    classification_filter = ""
    if classification_code_column is not None and excluded_classification_prefixes:
        excluded_prefixes = ", ".join(
            sql_text(prefix) for prefix in excluded_classification_prefixes
        )
        classification_filter = f"""
              AND NOT list_has_any(
                  [{excluded_prefixes}],
                  list_transform(
                      range(1, length({classification_code_column}) + 1),
                      position -> left({classification_code_column}, position)
                  )
              )
        """
    return f"""
        WITH source_rows AS (
            SELECT
                CAST(unique_id AS VARCHAR) AS address_id,
                trim(regexp_replace(
                    {facility_clause_removal_sql("upper(clean_full_address)")},
                    '{suffix_pattern}', ''
                )) AS peeled_address,
                regexp_replace(
                    upper(coalesce(postcode, '')), '[^A-Z0-9]', '', 'g'
                ) AS full_postcode,
                regexp_extract(
                    regexp_replace(upper(coalesce(postcode, '')), '[^A-Z0-9]', '', 'g'),
                    '^([A-Z]{{1,2}}[0-9][A-Z0-9]?)',
                    1
                ) AS postcode_district,
                numeric_tokens
            FROM {source_relation}
            WHERE clean_full_address IS NOT NULL
              AND clean_full_address != ''
                            AND NOT regexp_matches(
                                    upper(clean_full_address), {raw_text_pattern}
                            )
              AND array_length(numeric_tokens) > 0
                            {classification_filter}
        ), addresses AS (
            SELECT
                *,
                string_split(peeled_address, ' ') AS address_tokens
            FROM source_rows
            WHERE peeled_address != ''
        ), anchors AS (
            SELECT
                addresses.*,
                list_max(list_transform(
                    range(1, array_length(address_tokens) + 1),
                    position -> CASE
                        WHEN list_contains(
                            numeric_tokens, list_extract(address_tokens, position)
                        )
                        THEN position
                    END
                )) AS numeric_anchor
            FROM addresses
        ), candidate_windows AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                list_extract(address_tokens, numeric_anchor) AS rightmost_numeric_value,
                numeric_anchor,
                array_length(address_tokens) - numeric_anchor AS tail_length,
                starts.start_position AS candidate_start_position,
                widths.width AS candidate_width,
                starts.start_position + widths.width - 1 AS candidate_end_position,
                array_to_string(
                    list_slice(
                        address_tokens,
                        starts.start_position,
                        starts.start_position + widths.width - 1
                    ),
                    ' '
                ) AS candidate_phrase,
                list_extract(
                    address_tokens, starts.start_position + widths.width - 1
                ) AS terminal_token
            FROM anchors
            CROSS JOIN range(
                numeric_anchor + 1, array_length(address_tokens) + 1
            ) AS starts(start_position)
            CROSS JOIN (VALUES (2), (3)) AS widths(width)
            WHERE numeric_anchor IS NOT NULL
              AND starts.start_position + widths.width - 1
                    <= array_length(address_tokens)
        ), candidate_terminals AS (
            SELECT
                *,
                regexp_matches(terminal_token, {road_terminal_pattern})
                    AS road_syntax_terminal,
                max(regexp_matches(terminal_token, {road_terminal_pattern})::INTEGER)
                    OVER (PARTITION BY address_id) AS has_road_syntax_terminal
            FROM candidate_windows
            WHERE NOT regexp_matches(candidate_phrase, {facility_candidate_pattern})
        )
        SELECT
            address_id,
            full_postcode,
            postcode_district,
            rightmost_numeric_value,
            numeric_anchor,
            tail_length,
            candidate_start_position,
            candidate_width,
            candidate_end_position,
            candidate_phrase,
            terminal_token
        FROM candidate_terminals
        WHERE (road_syntax_terminal OR has_road_syntax_terminal = 0)
          AND (
              NOT regexp_matches(candidate_phrase, {block_candidate_pattern})
              OR road_syntax_terminal
          )
    """


def prepared_candidate_sql(source_relation: str) -> str:
    """Return candidate SQL that reuses prepared tokens and numeric anchors."""
    suffix_pattern = suffix_peel_regex_sql_literal()
    raw_text_pattern = sql_text(AMBIGUOUS_ADDRESS_PATTERN)
    block_candidate_pattern = sql_text(BLOCK_CANDIDATE_PATTERN)
    facility_candidate_pattern = sql_text(FACILITY_CANDIDATE_PATTERN)
    road_terminal_pattern = sql_text(
        token_pattern(tuple(token_policy()["road_syntax_terminal_tokens"]))
    )
    return f"""
        WITH source_rows AS (
            SELECT
                CAST(unique_id AS VARCHAR) AS address_id,
                clean_full_address,
                regexp_replace(
                    upper(coalesce(postcode, '')), '[^A-Z0-9]', '', 'g'
                ) AS full_postcode,
                postcode_district,
                rightmost_numeric_value,
                rightmost_numeric_position AS numeric_anchor,
                peeled_tokens AS address_tokens,
                numeric_tokens,
                regexp_matches(
                    upper(clean_full_address), {facility_candidate_pattern}
                ) AS has_facility_clause
            FROM {source_relation}
            WHERE clean_full_address IS NOT NULL
              AND clean_full_address != ''
              AND NOT regexp_matches(
                  upper(clean_full_address), {raw_text_pattern}
              )
              AND rightmost_numeric_position IS NOT NULL
        ), prepared_candidate_windows AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                rightmost_numeric_value,
                numeric_anchor,
                array_length(address_tokens) - numeric_anchor AS tail_length,
                starts.start_position AS candidate_start_position,
                widths.width AS candidate_width,
                starts.start_position + widths.width - 1 AS candidate_end_position,
                array_to_string(
                    list_slice(
                        address_tokens,
                        starts.start_position,
                        starts.start_position + widths.width - 1
                    ),
                    ' '
                ) AS candidate_phrase,
                list_extract(
                    address_tokens, starts.start_position + widths.width - 1
                ) AS terminal_token
            FROM source_rows
            CROSS JOIN range(
                numeric_anchor + 1, array_length(address_tokens) + 1
            ) AS starts(start_position)
            CROSS JOIN (VALUES (2), (3)) AS widths(width)
            WHERE NOT has_facility_clause
              AND starts.start_position + widths.width - 1
                    <= array_length(address_tokens)
        ), facility_addresses AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                numeric_tokens,
                string_split(
                    trim(regexp_replace(
                        {facility_clause_removal_sql("upper(clean_full_address)")},
                        '{suffix_pattern}', ''
                    )),
                    ' '
                ) AS address_tokens
            FROM source_rows
            WHERE has_facility_clause
        ), facility_anchors AS (
            SELECT
                *,
                list_max(list_transform(
                    range(1, array_length(address_tokens) + 1),
                    position -> CASE
                        WHEN list_contains(
                            numeric_tokens, list_extract(address_tokens, position)
                        ) THEN position
                    END
                )) AS numeric_anchor
            FROM facility_addresses
        ), facility_candidate_windows AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                list_extract(address_tokens, numeric_anchor) AS rightmost_numeric_value,
                numeric_anchor,
                array_length(address_tokens) - numeric_anchor AS tail_length,
                starts.start_position AS candidate_start_position,
                widths.width AS candidate_width,
                starts.start_position + widths.width - 1 AS candidate_end_position,
                array_to_string(
                    list_slice(
                        address_tokens,
                        starts.start_position,
                        starts.start_position + widths.width - 1
                    ),
                    ' '
                ) AS candidate_phrase,
                list_extract(
                    address_tokens, starts.start_position + widths.width - 1
                ) AS terminal_token
            FROM facility_anchors
            CROSS JOIN range(
                numeric_anchor + 1, array_length(address_tokens) + 1
            ) AS starts(start_position)
            CROSS JOIN (VALUES (2), (3)) AS widths(width)
            WHERE numeric_anchor IS NOT NULL
              AND starts.start_position + widths.width - 1
                    <= array_length(address_tokens)
        ), candidate_windows AS (
            SELECT * FROM prepared_candidate_windows
            UNION ALL
            SELECT * FROM facility_candidate_windows
        ), candidate_terminals AS (
            SELECT
                *,
                regexp_matches(terminal_token, {road_terminal_pattern})
                    AS road_syntax_terminal,
                max(regexp_matches(terminal_token, {road_terminal_pattern})::INTEGER)
                    OVER (PARTITION BY address_id) AS has_road_syntax_terminal
            FROM candidate_windows
            WHERE NOT regexp_matches(candidate_phrase, {facility_candidate_pattern})
        )
        SELECT
            address_id,
            full_postcode,
            postcode_district,
            rightmost_numeric_value,
            numeric_anchor,
            tail_length,
            candidate_start_position,
            candidate_width,
            candidate_end_position,
            candidate_phrase,
            terminal_token
        FROM candidate_terminals
        WHERE (road_syntax_terminal OR has_road_syntax_terminal = 0)
          AND (
              NOT regexp_matches(candidate_phrase, {block_candidate_pattern})
              OR road_syntax_terminal
          )
    """


def create_candidate_table(
    con: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    candidate_table: str = "road_assignment_candidates",
    temporary: bool = False,
    analyze: bool = True,
    use_prepared_fields: bool = False,
    classification_code_column: str | None = None,
    excluded_classification_prefixes: tuple[str, ...] = (),
) -> None:
    """Materialize canonical candidates once for cataloguing and scoring."""
    if use_prepared_fields:
        if classification_code_column is not None or excluded_classification_prefixes:
            raise ValueError("Prepared candidate SQL does not support class exclusions")
        candidates = prepared_candidate_sql(source_relation)
    else:
        candidates = canonical_candidate_sql(
            source_relation,
            classification_code_column=classification_code_column,
            excluded_classification_prefixes=excluded_classification_prefixes,
        )
    table_kind = "TEMPORARY " if temporary else ""
    con.execute(f"CREATE OR REPLACE {table_kind}TABLE {candidate_table} AS {candidates}")
    if analyze:
        con.execute(f"ANALYZE {candidate_table}")


def create_phrase_catalog_from_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    candidate_relation: str,
    catalog_table: str,
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {catalog_table} AS
        WITH candidate_stats AS (
            SELECT
                candidate_phrase,
                terminal_token,
                count(*) AS phrase_support,
                count(DISTINCT address_id) AS phrase_addresses,
                approx_count_distinct(rightmost_numeric_value) AS distinct_numbers,
                approx_count_distinct(full_postcode) AS distinct_postcodes,
                approx_count_distinct(postcode_district) AS distinct_districts
            FROM {candidate_relation}
            GROUP BY candidate_phrase, terminal_token
        )
        SELECT
            candidate_phrase,
            terminal_token,
            phrase_support,
            phrase_addresses,
            distinct_numbers,
            distinct_postcodes,
            distinct_districts,
            sum(phrase_support) OVER (PARTITION BY terminal_token) AS terminal_support,
            count(*) OVER (PARTITION BY terminal_token) AS terminal_distinct_phrases
        FROM candidate_stats
        """
    )
    con.execute(f"ANALYZE {catalog_table}")


def create_phrase_catalog(
    con: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    catalog_table: str = "road_assignment_phrase_catalog",
    classification_code_column: str | None = None,
    excluded_classification_prefixes: tuple[str, ...] = (),
) -> None:
    """Materialize the phrase and terminal recurrence values required at inference."""
    candidates = canonical_candidate_sql(
        source_relation,
        classification_code_column=classification_code_column,
        excluded_classification_prefixes=excluded_classification_prefixes,
    )
    create_phrase_catalog_from_candidates(
        con,
        candidate_relation=f"({candidates})",
        catalog_table=catalog_table,
    )


def online_feature_sql(
    input_relation: str,
    *,
    catalog_table: str = "road_assignment_phrase_catalog",
    include_candidate_count: bool = False,
    catalog_only: bool = False,
    classification_code_column: str | None = None,
    excluded_classification_prefixes: tuple[str, ...] = (),
) -> str:
    """Return inference features for prepared input rows without reading an oracle."""
    candidates = canonical_candidate_sql(
        input_relation,
        classification_code_column=classification_code_column,
        excluded_classification_prefixes=excluded_classification_prefixes,
    )
    return candidate_feature_sql(
        f"({candidates})",
        catalog_table=catalog_table,
        include_candidate_count=include_candidate_count,
        catalog_only=catalog_only,
    )


def candidate_feature_sql(
    candidate_relation: str,
    *,
    catalog_table: str = "road_assignment_phrase_catalog",
    include_candidate_count: bool = False,
    catalog_only: bool = False,
) -> str:
    """Return deployable features from already-materialized candidate rows."""
    policy = token_policy()
    residence_pattern = sql_text(
        token_pattern(tuple(policy["residence_or_non_road_any_token"]))
    )
    road_pattern = sql_text(token_pattern(tuple(policy["road_syntax_terminal_tokens"])))
    business_pattern = sql_text("(^| )(BUSINESS|ESTATE|PARK)( |$)")
    candidate_counts_cte = ""
    candidate_counts_select = ""
    candidate_counts_join = ""
    if include_candidate_count:
        candidate_counts_cte = """, candidate_counts AS (
            SELECT address_id, count(*) AS candidate_count
            FROM candidates
            GROUP BY address_id
        )"""
        candidate_counts_select = "candidate_counts.candidate_count,"
        candidate_counts_join = "JOIN candidate_counts USING (address_id)"
    catalog_join = "JOIN" if catalog_only else "LEFT JOIN"
    return f"""
        WITH candidates AS (SELECT * FROM {candidate_relation}){candidate_counts_cte}
        SELECT
            candidates.address_id,
            candidates.candidate_phrase,
            candidates.candidate_start_position,
            candidates.candidate_end_position,
            candidates.candidate_width,
            candidates.tail_length,
            candidates.terminal_token,
            {candidate_counts_select}
            regexp_matches(candidates.candidate_phrase, {residence_pattern})::DOUBLE
                AS contains_residence_token,
            regexp_matches(candidates.candidate_phrase, {business_pattern})::DOUBLE
                AS contains_business_token,
            regexp_matches(candidates.terminal_token, {road_pattern})::DOUBLE
                AS road_syntax_terminal,
            (candidates.candidate_start_position - candidates.numeric_anchor)::DOUBLE
                / greatest(candidates.tail_length, 1) AS start_tail_fraction,
            (candidates.candidate_end_position - candidates.numeric_anchor)::DOUBLE
                / greatest(candidates.tail_length, 1) AS end_tail_fraction,
            candidates.candidate_width::DOUBLE / greatest(candidates.tail_length, 1)
                AS width_tail_fraction,
            CAST(
                candidates.candidate_end_position
                    = candidates.numeric_anchor + candidates.tail_length
                AS DOUBLE
            ) AS ends_at_tail,
            ln(1 + coalesce(catalog.phrase_support, 0)) AS log_phrase_support,
            coalesce(catalog.distinct_numbers, 0)::DOUBLE
                / greatest(
                    coalesce(catalog.phrase_addresses, 0), 1
                ) AS number_diversity_ratio,
            ln(1 + coalesce(catalog.distinct_postcodes, 0)) AS log_postcode_support,
            coalesce(catalog.distinct_districts, 0)::DOUBLE AS district_count,
            ln(1 + coalesce(catalog.terminal_support, 0)) AS log_terminal_support,
            coalesce(catalog.terminal_distinct_phrases, 0)::DOUBLE
                AS terminal_right_context_diversity
        FROM candidates
        {candidate_counts_join}
        {catalog_join} {catalog_table} AS catalog
            USING (candidate_phrase, terminal_token)
    """


def score_candidate_relation_sql(
    con: duckdb.DuckDBPyConnection,
    *,
    model: FoldedLogisticModel,
    candidate_relation: str,
    output_table: str = "road_assignment_additive_scores",
    catalog_table: str = "road_assignment_phrase_catalog",
    catalog_only: bool = False,
    temporary: bool = False,
    feature_definitions: Sequence[tuple[str, str]] = ADDITIVE_FEATURES,
) -> int:
    """Score materialized candidates using the folded additive model in DuckDB."""
    features = candidate_feature_sql(
        candidate_relation,
        catalog_table=catalog_table,
        catalog_only=catalog_only,
    )
    scorecard_features = additive_feature_sql(
        "candidate_features", feature_definitions=feature_definitions
    )
    table_kind = "TEMPORARY " if temporary else ""
    con.execute(
        f"""
        CREATE OR REPLACE {table_kind}TABLE {output_table} AS
        WITH candidate_features AS ({features}), scorecard_features AS (
            SELECT
                address_id,
                candidate_phrase,
                candidate_start_position,
                candidate_end_position,
                candidate_width,
                tail_length,
                {scorecard_features}
            FROM candidate_features
        )
        SELECT
            address_id,
            candidate_phrase,
            candidate_start_position,
            candidate_end_position,
            candidate_width,
            tail_length,
            {logistic_logit_sql(model)} AS ranker_logit
        FROM scorecard_features
        """
    )
    return int(con.execute(f"SELECT count(*) FROM {output_table}").fetchone()[0])


def create_ranker_winners(
    con: duckdb.DuckDBPyConnection,
    *,
    score_table: str = "road_assignment_scores",
    winner_table: str = "road_assignment_ranker_winners",
    score_column: str = "ranker_probability",
    temporary: bool = False,
) -> None:
    """Select a deterministic top-one ranker candidate for every scored address."""
    table_kind = "TEMPORARY " if temporary else ""
    con.execute(
        f"""
        CREATE OR REPLACE {table_kind}TABLE {winner_table} AS
        SELECT * EXCLUDE (candidate_rank)
        FROM (
            SELECT
                scores.*,
                row_number() OVER (
                    PARTITION BY address_id
                    ORDER BY
                        {score_column} DESC,
                        candidate_phrase,
                        candidate_start_position
                ) AS candidate_rank
            FROM {score_table} AS scores
        )
        WHERE candidate_rank = 1
        """
    )


def build_catalog(
    *,
    canonical_path: Path,
    output_database: Path,
    threads: int | None,
    explain: bool,
    input_path: Path | None,
    folded_ranker_path: Path | None,
    assign_canonical: bool,
    classification_code_column: str | None,
    excluded_classification_prefixes: tuple[str, ...],
) -> None:
    output_database.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output_database))
    if threads is not None:
        con.execute(f"SET threads TO {int(threads)}")
    source_relation = f"read_parquet({sql_text(str(canonical_path.resolve()))})"
    source_columns = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM {source_relation}").fetchall()
    }
    source_classification_column = (
        classification_code_column
        if classification_code_column in source_columns
        else None
    )
    if explain:
        plan = con.execute(
            f"EXPLAIN {canonical_candidate_sql(source_relation)}"
        ).fetchone()[1]
        sys.stdout.write(f"{plan}\n")
        return
    if input_path is None and not assign_canonical:
        started = time.perf_counter()
        create_phrase_catalog(
            con,
            source_relation=source_relation,
            classification_code_column=source_classification_column,
            excluded_classification_prefixes=excluded_classification_prefixes,
        )
        rows = con.execute(
            "SELECT count(*) FROM road_assignment_phrase_catalog"
        ).fetchone()[0]
        sys.stdout.write(
            f"catalog rows={rows:,} wall_seconds={time.perf_counter() - started:.3f}\n"
        )
        return
    if input_path is None:
        input_relation = source_relation
        input_classification_column = source_classification_column
    else:
        input_relation = f"read_parquet({sql_text(str(input_path.resolve()))})"
        input_columns = {
            row[0]
            for row in con.execute(f"DESCRIBE SELECT * FROM {input_relation}").fetchall()
        }
        input_classification_column = (
            classification_code_column
            if classification_code_column in input_columns
            else None
        )
    started = time.perf_counter()
    candidate_started = time.perf_counter()
    create_candidate_table(
        con,
        source_relation=input_relation,
        candidate_table="road_assignment_runtime_candidates",
        temporary=True,
        classification_code_column=input_classification_column,
        excluded_classification_prefixes=excluded_classification_prefixes,
    )
    candidate_seconds = time.perf_counter() - candidate_started
    if assign_canonical:
        catalog_started = time.perf_counter()
        create_phrase_catalog_from_candidates(
            con,
            candidate_relation="road_assignment_runtime_candidates",
            catalog_table="road_assignment_phrase_catalog",
        )
        catalog_seconds = time.perf_counter() - catalog_started
    else:
        catalog_seconds = 0.0
    if folded_ranker_path is None:
        raise ValueError("--folded-ranker-path is required when assigning roads")
    ranker = load_folded_ranker_model(folded_ranker_path)
    score_started = time.perf_counter()
    scored_rows = score_candidate_relation_sql(
        con,
        model=ranker.model,
        candidate_relation="road_assignment_runtime_candidates",
        catalog_only=not assign_canonical,
        temporary=True,
        feature_definitions=ranker.feature_definitions,
    )
    score_seconds = time.perf_counter() - score_started
    score_table = "road_assignment_additive_scores"
    score_column = "ranker_logit"
    winner_table = (
        "road_assignment_canonical_labels"
        if assign_canonical
        else "road_assignment_ranker_winners"
    )
    create_ranker_winners(
        con,
        score_table=score_table,
        winner_table=winner_table,
        score_column=score_column,
    )
    winner_rows = con.execute(f"SELECT count(*) FROM {winner_table}").fetchone()[0]
    if assign_canonical:
        catalog_rows = con.execute(
            "SELECT count(*) FROM road_assignment_phrase_catalog"
        ).fetchone()[0]
        manifest_path = output_database.with_suffix(".road_assignment_manifest.json")
        manifest_path.write_text(
            json.dumps(
                {
                    "version": ARTIFACT_VERSION,
                    "canonical_path": str(canonical_path.resolve()),
                    "catalog_table": "road_assignment_phrase_catalog",
                    "canonical_label_table": winner_table,
                    "catalog_rows": catalog_rows,
                    "candidate_rows": scored_rows,
                    "winner_rows": winner_rows,
                    "ranker_artifact": str(folded_ranker_path.resolve()),
                    "ranker_type": ranker.model_type,
                    "threads": threads,
                    "timings_seconds": {
                        "candidate_generation": candidate_seconds,
                        "catalog_build": catalog_seconds,
                        "scoring": score_seconds,
                        "total": time.perf_counter() - started,
                    },
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    sys.stdout.write(
        f"scored candidates={scored_rows:,} winners={winner_rows:,} "
        f"candidate_seconds={candidate_seconds:.3f} score_seconds={score_seconds:.3f} "
        f"wall_seconds={time.perf_counter() - started:.3f}\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deployable road-phrase artifacts")
    parser.add_argument("--canonical-path", type=Path, required=True)
    parser.add_argument("--output-database", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--input-path", type=Path)
    parser.add_argument("--folded-ranker-path", type=Path)
    parser.add_argument("--assign-canonical", action="store_true")
    parser.add_argument("--classification-code-column", default="classificationcode")
    parser.add_argument(
        "--excluded-classification-prefix",
        action="append",
        default=list(DEFAULT_EXCLUDED_CLASSIFICATION_PREFIXES),
    )
    arguments = parser.parse_args()
    build_catalog(
        canonical_path=arguments.canonical_path,
        output_database=arguments.output_database,
        threads=arguments.threads,
        explain=arguments.explain,
        input_path=arguments.input_path,
        folded_ranker_path=arguments.folded_ranker_path,
        assign_canonical=arguments.assign_canonical,
        classification_code_column=arguments.classification_code_column,
        excluded_classification_prefixes=tuple(arguments.excluded_classification_prefix),
    )


if __name__ == "__main__":
    main()
