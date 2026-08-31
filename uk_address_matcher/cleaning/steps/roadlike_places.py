"""Extract deployable roadlike-place candidates from cleaned canonical addresses."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
import re
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

import duckdb

from uk_address_matcher.cleaning.steps.road_resources import (
    facility_clause_removal_sql,
    sql_text,
    suffix_peel_regex_sql_literal,
    token_pattern,
    token_policy,
)
from uk_address_matcher.sql_pipeline.steps import pipeline_stage

AMBIGUOUS_ADDRESS_PATTERN = (
    "CARAVAN|HOUSE BOAT|HOUSEBOAT|BEACH HUT|TENNIS.*(FROM|UNNAMED ROAD)|"
    "(^|[^A-Z])REAR( OF)?([^A-Z]|$)"
)
BLOCK_CANDIDATE_PATTERN = "(^| )BLOCK( |$)"
FACILITY_CANDIDATE_PATTERN = "SHOPPING CENTRE|INDUSTRIAL ESTATE|INDUSTRIAL PARK"

ROAD_FEATURE_COLUMNS = (
    "road_1_norm",
    "road_1_confidence",
    "road_1_token_count",
    "road_1_margin",
    "road_1_distinctive_tokens",
)
ROAD_TOP_2_FEATURE_COLUMNS = ("road_top_2_norms",)
ROAD_BLOCKING_COLUMNS = ("road_1_norm", "outward_postcode")

_ROAD_SCORECARD_BASE_FEATURES = {
    "start_tail_fraction",
    "end_tail_fraction",
    "width_tail_fraction",
    "candidate_width",
    "tail_length",
    "ends_at_tail",
    "road_syntax_terminal",
    "contains_residence_token",
    "contains_business_token",
    "log_phrase_support",
    "number_diversity_ratio",
    "log_postcode_support",
    "district_count",
    "log_terminal_support",
    "terminal_right_context_diversity",
}
_ROAD_SCORECARD_THRESHOLD_PATTERN = re.compile(r"(.+)_ge_([0-9_]+)$")
_ROAD_SCORECARD_INTERACTIONS = {
    "road_terminal_x_tail_length_ge_4_5": (
        "road_syntax_terminal * CASE WHEN tail_length >= 4.5 THEN 1.0 ELSE 0.0 END"
    ),
    "road_terminal_x_start_tail_fraction_ge_0_5119": (
        "road_syntax_terminal * CASE WHEN start_tail_fraction >= 0.5119 "
        "THEN 1.0 ELSE 0.0 END"
    ),
    "road_terminal_x_end_tail_fraction_ge_0_6754": (
        "road_syntax_terminal * CASE WHEN end_tail_fraction >= 0.6754 "
        "THEN 1.0 ELSE 0.0 END"
    ),
    "tail_length_ge_4_5_x_end_tail_fraction_ge_0_6754": (
        "(tail_length >= 4.5)::DOUBLE * (end_tail_fraction >= 0.6754)::DOUBLE"
    ),
    "tail_length_ge_3_5_x_start_tail_fraction_ge_0_5119": (
        "(tail_length >= 3.5)::DOUBLE * (start_tail_fraction >= 0.5119)::DOUBLE"
    ),
    "residence_x_start_tail_fraction_ge_0_5119": (
        "contains_residence_token * (start_tail_fraction >= 0.5119)::DOUBLE"
    ),
    "road_terminal_x_terminal_right_context_diversity_ge_1707": (
        "road_syntax_terminal * (terminal_right_context_diversity >= 1707.0)::DOUBLE"
    ),
    "tail_length_ge_4_5_x_terminal_right_context_diversity_ge_1707": (
        "(tail_length >= 4.5)::DOUBLE "
        "* (terminal_right_context_diversity >= 1707.0)::DOUBLE"
    ),
    "end_tail_fraction_ge_0_6754_x_terminal_right_context_diversity_ge_1707": (
        "(end_tail_fraction >= 0.6754)::DOUBLE "
        "* (terminal_right_context_diversity >= 1707.0)::DOUBLE"
    ),
    "terminal_right_context_diversity_ge_1707_x_log_terminal_support_ge_13_3338": (
        "(terminal_right_context_diversity >= 1707.0)::DOUBLE "
        "* (log_terminal_support >= 13.3338)::DOUBLE"
    ),
    "road_terminal_x_width_tail_fraction_ge_0_3875": (
        "road_syntax_terminal * (width_tail_fraction >= 0.3875)::DOUBLE"
    ),
}


def _road_candidate_feature_sql(
    candidate_relation: str,
    catalogue_view: str,
    *,
    require_catalogue_support: bool = False,
) -> str:
    policy = token_policy()
    residence_pattern = sql_text(
        token_pattern(tuple(policy["residence_or_non_road_any_token"]))
    )
    road_pattern = sql_text(token_pattern(tuple(policy["road_syntax_terminal_tokens"])))
    business_pattern = sql_text("(^| )(BUSINESS|ESTATE|PARK)( |$)")
    catalogue_join = "INNER JOIN" if require_catalogue_support else "LEFT JOIN"
    return f"""
        WITH candidates AS (SELECT * FROM {candidate_relation})
        SELECT
            candidates.*,
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
            CAST(candidates.candidate_end_position = candidates.numeric_anchor
                + candidates.tail_length AS DOUBLE) AS ends_at_tail,
            ln(1 + coalesce(catalogue.phrase_support, 0)) AS log_phrase_support,
            coalesce(catalogue.distinct_numbers, 0)::DOUBLE
                / greatest(coalesce(catalogue.phrase_addresses, 0), 1)
                AS number_diversity_ratio,
            ln(1 + coalesce(catalogue.distinct_postcodes, 0)) AS log_postcode_support,
            coalesce(catalogue.distinct_districts, 0)::DOUBLE AS district_count,
            ln(1 + coalesce(catalogue.terminal_support, 0)) AS log_terminal_support,
            coalesce(catalogue.terminal_distinct_phrases, 0)::DOUBLE
                AS terminal_right_context_diversity
        FROM candidates
        {catalogue_join} {catalogue_view} AS catalogue
            USING (candidate_phrase, terminal_token)
    """


def _road_scorecard_feature_sql(scorecard: dict[str, object]) -> str:
    coefficients = scorecard["coefficients"]
    rules_by_name = {
        rule["name"]: rule["conditions"]
        for rule in scorecard["rules"]
        if isinstance(rule, dict)
        and isinstance(rule.get("name"), str)
        and isinstance(rule.get("conditions"), list)
    }
    expressions: list[str] = []
    for feature in scorecard["feature_columns"]:
        if float(coefficients[feature]) == 0.0:
            continue
        if feature in _ROAD_SCORECARD_BASE_FEATURES:
            expression = f"candidate_features.{feature}::DOUBLE"
        elif feature == "log_terminal_right_context_diversity":
            expression = (
                "ln(1.0 + candidate_features.terminal_right_context_diversity::DOUBLE)"
            )
        elif feature in _ROAD_SCORECARD_INTERACTIONS:
            expression = _ROAD_SCORECARD_INTERACTIONS[feature]
        elif feature in rules_by_name:
            expression = (
                "CASE WHEN "
                + " AND ".join(
                    "candidate_features.{feature}::DOUBLE {operator} {threshold}".format(
                        feature=condition["feature"],
                        operator=condition["operator"],
                        threshold=float(condition["threshold"]),
                    )
                    for condition in rules_by_name[feature]
                )
                + " THEN 1.0 ELSE 0.0 END"
            )
        else:
            match = _ROAD_SCORECARD_THRESHOLD_PATTERN.fullmatch(feature)
            if match is None or match.group(1) not in _ROAD_SCORECARD_BASE_FEATURES:
                raise ValueError(f"Unsupported road scorecard feature: {feature}")
            base_feature, raw_threshold = match.groups()
            expression = (
                f"(candidate_features.{base_feature}::DOUBLE >= "
                f"{raw_threshold.replace('_', '.')})::DOUBLE"
            )
        expressions.append(f"{expression} AS {feature}")
    return ",\n                    ".join(expressions)


def _score_road_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    candidate_relation: str,
    catalogue_view: str,
    scorecard: dict[str, object],
    output_table: str,
    require_catalogue_support: bool = False,
) -> None:
    coefficients = scorecard.get("coefficients")
    feature_columns = scorecard.get("feature_columns")
    if not isinstance(coefficients, dict) or not isinstance(feature_columns, list):
        raise ValueError("Road scorecard is missing its feature contract")
    score_expression = " + ".join(
        [repr(float(scorecard["intercept"]))]
        + [
            f"({float(coefficients[feature])!r} * {feature})"
            for feature in feature_columns
            if float(coefficients[feature]) != 0.0
        ]
    )
    con.execute(
        f"""
        CREATE TEMPORARY TABLE {output_table} AS
        WITH candidate_features AS (
            {
            _road_candidate_feature_sql(
                candidate_relation,
                catalogue_view,
                require_catalogue_support=require_catalogue_support,
            )
        }
        ), scorecard_features AS (
            SELECT
                address_id,
                candidate_phrase,
                candidate_start_position,
                candidate_end_position,
                {_road_scorecard_feature_sql(scorecard)}
            FROM candidate_features
        )
        SELECT
            address_id,
            candidate_phrase,
            candidate_start_position,
            candidate_end_position,
            {score_expression} AS ranker_logit
        FROM scorecard_features
        """
    )


def derive_rightmost_numeric_position_sql(source_relation: str) -> str:
    """Attach the suffix-peeled rightmost numeric-token position."""
    suffix_pattern = suffix_peel_regex_sql_literal()
    return f"""
        WITH tokenised AS (
            SELECT
                *,
                string_split(
                    trim(regexp_replace(
                        upper(clean_full_address), '{suffix_pattern}', ''
                    )),
                    ' '
                ) AS _roadlike_peeled_tokens
            FROM {source_relation}
        )
        SELECT
            * EXCLUDE (_roadlike_peeled_tokens),
            CAST(list_max(list_transform(
                range(1, array_length(_roadlike_peeled_tokens) + 1),
                position -> CASE
                    WHEN list_contains(
                        numeric_tokens,
                        list_extract(_roadlike_peeled_tokens, position)
                    ) THEN position
                END
            )) AS SMALLINT) AS rightmost_numeric_position
        FROM tokenised
    """


def roadlike_place_prepared_input_sql(
    source_relation: str,
    *,
    use_precomputed_numeric_position: bool = False,
) -> str:
    """Prepare cleaned canonical rows for fast roadlike candidate extraction."""
    suffix_pattern = suffix_peel_regex_sql_literal()
    source_projection = (
        "* EXCLUDE (rightmost_numeric_position)"
        if use_precomputed_numeric_position
        else "*"
    )
    precomputed_projection = (
        ", rightmost_numeric_position" if use_precomputed_numeric_position else ""
    )
    anchor_cte = (
        "SELECT * FROM tokenised"
        if use_precomputed_numeric_position
        else """
            SELECT
                *,
                list_max(list_transform(
                    range(1, array_length(peeled_tokens) + 1),
                    position -> CASE
                        WHEN list_contains(
                            numeric_tokens,
                            list_extract(peeled_tokens, position)
                        ) THEN position
                    END
                )) AS rightmost_numeric_position
            FROM tokenised
        """
    )
    return f"""
        WITH tokenised AS (
            SELECT
                {source_projection},
                regexp_extract(
                    upper(coalesce(postcode, '')),
                    '^\\s*([A-Z]{{1,2}}[0-9]{{1,2}}[A-Z]?)\\s+\\d',
                    1
                ) AS postcode_district,
                string_split(
                    trim(regexp_replace(
                        upper(clean_full_address), '{suffix_pattern}', ''
                    )),
                    ' '
                ) AS peeled_tokens
                {precomputed_projection}
            FROM {source_relation}
            WHERE clean_full_address IS NOT NULL
              AND clean_full_address != ''
              AND array_length(numeric_tokens) > 0
        ), anchors AS (
            {anchor_cte}
        )
        SELECT
            *,
            list_extract(
                peeled_tokens,
                rightmost_numeric_position
            ) AS rightmost_numeric_value
        FROM anchors
    """


def roadlike_place_prepared_candidate_sql(
    source_relation: str,
    *,
    catalogue_width_relation: str | None = None,
) -> str:
    """Build terminal-first candidates from prepared canonical rows."""
    suffix_pattern = suffix_peel_regex_sql_literal()
    raw_text_pattern = sql_text(AMBIGUOUS_ADDRESS_PATTERN)
    block_candidate_pattern = sql_text(BLOCK_CANDIDATE_PATTERN)
    facility_candidate_pattern = sql_text(FACILITY_CANDIDATE_PATTERN)
    road_terminal_pattern = sql_text(
        token_pattern(tuple(token_policy()["road_syntax_terminal_tokens"]))
    )
    width_support_cte = ""
    terminal_width_join = ""
    fallback_width_join = ""
    fallback_filter_ctes = f"""
        ), fallback_candidate_flags AS (
            SELECT
                *,
                NOT regexp_matches(candidate_phrase, {facility_candidate_pattern})
                    AS phrase_allowed,
                max((
                    terminal_token IS NOT NULL
                    AND NOT regexp_matches(
                        candidate_phrase, {facility_candidate_pattern}
                    )
                )::INTEGER) OVER (PARTITION BY address_id) AS has_valid_candidate
            FROM fallback_candidate_windows
        ), fallback_candidates AS (
            SELECT * EXCLUDE (phrase_allowed, has_valid_candidate)
            FROM fallback_candidate_flags
            WHERE has_valid_candidate = 1
              AND phrase_allowed
              AND NOT regexp_matches(candidate_phrase, {block_candidate_pattern})
        )
    """
    if catalogue_width_relation is not None:
        width_support_cte = f""", supported_widths AS (
            SELECT DISTINCT
                terminal_token,
                array_length(string_split(candidate_phrase, ' ')) AS candidate_width
            FROM {catalogue_width_relation}
        ), supported_phrases AS (
            SELECT candidate_phrase, terminal_token
            FROM {catalogue_width_relation}
        )"""
        terminal_width_join = """
            INNER JOIN supported_widths
                ON supported_widths.terminal_token = list_extract(
                    address_tokens, ends.end_position
                )
               AND supported_widths.candidate_width = widths.width
        """
        fallback_width_join = """
            INNER JOIN supported_phrases
                ON supported_phrases.candidate_phrase = array_to_string(
                    list_slice(
                        address_tokens,
                        starts.start_position,
                        starts.start_position + widths.width - 1
                    ),
                    ' '
                )
               AND supported_phrases.terminal_token = list_extract(
                    address_tokens, starts.start_position + widths.width - 1
                )
        """
        fallback_filter_ctes = f"""
        ), fallback_candidates AS (
            SELECT *
            FROM fallback_candidate_windows
            WHERE NOT regexp_matches(candidate_phrase, {block_candidate_pattern})
        )
        """
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
            WHERE NOT regexp_matches(upper(clean_full_address), {raw_text_pattern})
              AND rightmost_numeric_position IS NOT NULL
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
        ), candidate_sources AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                rightmost_numeric_value,
                numeric_anchor,
                address_tokens,
                true AS allow_truncated_windows
            FROM source_rows
            WHERE NOT has_facility_clause
            UNION ALL
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                list_extract(address_tokens, numeric_anchor) AS rightmost_numeric_value,
                numeric_anchor,
                address_tokens,
                false AS allow_truncated_windows
            FROM facility_anchors
            WHERE numeric_anchor IS NOT NULL
        ){width_support_cte}, terminal_candidate_windows AS (
            SELECT
                address_id,
                full_postcode,
                postcode_district,
                rightmost_numeric_value,
                numeric_anchor,
                array_length(address_tokens) - numeric_anchor AS tail_length,
                ends.end_position - widths.width + 1 AS candidate_start_position,
                widths.width AS candidate_width,
                ends.end_position AS candidate_end_position,
                array_to_string(
                    list_slice(
                        address_tokens,
                        ends.end_position - widths.width + 1,
                        ends.end_position
                    ),
                    ' '
                ) AS candidate_phrase,
                list_extract(address_tokens, ends.end_position) AS terminal_token
                        FROM candidate_sources
                        CROSS JOIN range(
                                numeric_anchor + 2, array_length(address_tokens) + 1
                        ) AS ends(end_position)
            CROSS JOIN (VALUES (2), (3)) AS widths(width)
            {terminal_width_join}
            WHERE ends.end_position - widths.width + 1 > numeric_anchor
                            AND regexp_matches(
                                        list_extract(address_tokens, ends.end_position),
                                        {road_terminal_pattern}
                            )
        ), terminal_candidates AS (
            SELECT
                *
            FROM terminal_candidate_windows
            WHERE NOT regexp_matches(candidate_phrase, {facility_candidate_pattern})
        ), terminal_addresses AS (
            SELECT DISTINCT address_id
            FROM terminal_candidates
        ), fallback_candidate_windows AS (
            SELECT
                candidate_sources.address_id,
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
            FROM candidate_sources
            LEFT JOIN terminal_addresses USING (address_id)
            CROSS JOIN range(
                numeric_anchor + 1, array_length(address_tokens) + 1
            ) AS starts(start_position)
            CROSS JOIN (VALUES (2), (3)) AS widths(width)
            {fallback_width_join}
            WHERE terminal_addresses.address_id IS NULL
                            AND (
                                        allow_truncated_windows
                                        OR starts.start_position + widths.width - 1
                                                <= array_length(address_tokens)
                            )
        {fallback_filter_ctes}
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
        FROM terminal_candidates
        UNION ALL
        SELECT *
        FROM fallback_candidates
    """


def _road_tail_signature_sql(source_relation: str) -> str:
    suffix_pattern = suffix_peel_regex_sql_literal()
    raw_text_pattern = sql_text(AMBIGUOUS_ADDRESS_PATTERN)
    facility_candidate_pattern = sql_text(FACILITY_CANDIDATE_PATTERN)
    return f"""
        WITH source_rows AS (
            SELECT
                *,
                regexp_matches(
                    upper(clean_full_address), {facility_candidate_pattern}
                ) AS has_facility_clause
            FROM {source_relation}
            WHERE NOT regexp_matches(upper(clean_full_address), {raw_text_pattern})
              AND rightmost_numeric_position IS NOT NULL
        ), ordinary_tails AS (
            SELECT
                CAST(unique_id AS VARCHAR) AS unique_id,
                list_slice(
                    peeled_tokens,
                    rightmost_numeric_position + 1,
                    array_length(peeled_tokens)
                ) AS road_tail_tokens,
                true AS allow_truncated_windows
            FROM source_rows
            WHERE NOT has_facility_clause
        ), facility_addresses AS (
            SELECT
                CAST(unique_id AS VARCHAR) AS unique_id,
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
                            numeric_tokens,
                            list_extract(address_tokens, position)
                        ) THEN position
                    END
                )) AS numeric_anchor
            FROM facility_addresses
        )
        SELECT * FROM ordinary_tails
        UNION ALL
        SELECT
            unique_id,
            list_slice(
                address_tokens,
                numeric_anchor + 1,
                array_length(address_tokens)
            ) AS road_tail_tokens,
            false AS allow_truncated_windows
        FROM facility_anchors
        WHERE numeric_anchor IS NOT NULL
    """


def roadlike_place_candidate_sql(source_relation: str) -> str:
    """Build terminal-first roadlike candidates from cleaned canonical rows."""
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
                trim(regexp_replace(
                    {facility_clause_removal_sql("upper(clean_full_address)")},
                    '{suffix_pattern}', ''
                )) AS peeled_address,
                regexp_replace(
                    upper(coalesce(postcode, '')), '[^A-Z0-9]', '', 'g'
                ) AS full_postcode,
                regexp_extract(
                    upper(coalesce(postcode, '')),
                    '^\\s*([A-Z]{{1,2}}[0-9]{{1,2}}[A-Z]?)\\s+\\d',
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
                        ) THEN position
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


def roadlike_place_catalog_sql(candidate_relation: str) -> str:
    """Aggregate candidate recurrence evidence required by the road scorer."""
    return f"""
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


@contextmanager
def _materialized_road_scores(
    con: duckdb.DuckDBPyConnection,
    address_table: duckdb.DuckDBPyRelation,
    *,
    uid: str,
    require_catalogue_support: bool = False,
    deduplicate_tails: bool = False,
) -> Iterator[tuple[str, str, str | None, str | None]]:
    from uk_address_matcher.sql_pipeline.helpers import (
        _drop_table_and_registered_aliases,
    )

    input_name = f"__ukam_road_feature_input_{uid}"
    prepared_table = f"__ukam_road_feature_prepared_{uid}"
    catalogue_view = f"__ukam_road_feature_catalogue_{uid}"
    scores_table = f"__ukam_road_feature_scores_{uid}"
    tails_table = f"__ukam_road_feature_tails_{uid}"
    signatures_table = f"__ukam_road_feature_signatures_{uid}"
    con.register(input_name, address_table)
    try:
        con.execute(
            f"CREATE TEMPORARY TABLE {prepared_table} AS "
            f"{
                roadlike_place_prepared_input_sql(
                    input_name,
                    use_precomputed_numeric_position=(
                        'rightmost_numeric_position' in address_table.columns
                    ),
                )
            }"
        )
        with ExitStack() as resources:
            catalogue_path = resources.enter_context(
                pkg_resources.as_file(
                    pkg_resources.files("uk_address_matcher.data").joinpath(
                        "roadlike_places.parquet"
                    )
                )
            )
            model_path = resources.enter_context(
                pkg_resources.as_file(
                    pkg_resources.files("uk_address_matcher.data").joinpath(
                        "road_assignment_scorecard_v1.json"
                    )
                )
            )
            escaped_catalogue_path = str(catalogue_path).replace("'", "''")
            con.execute(
                f"CREATE TEMPORARY VIEW {catalogue_view} AS "
                f"SELECT * FROM read_parquet('{escaped_catalogue_path}')"
            )
            scorecard = json.loads(model_path.read_text(encoding="utf-8"))
            candidate_source = prepared_table
            if deduplicate_tails:
                con.execute(
                    f"CREATE TEMPORARY TABLE {tails_table} AS "
                    f"{_road_tail_signature_sql(prepared_table)}"
                )
                con.execute(f"""
                    CREATE TEMPORARY TABLE {signatures_table} AS
                    SELECT
                        min(unique_id) AS address_id,
                        road_tail_tokens,
                        allow_truncated_windows
                    FROM {tails_table}
                    WHERE array_length(road_tail_tokens) >= 2
                    GROUP BY road_tail_tokens, allow_truncated_windows
                """)
                candidate_source = f"""(
                    SELECT
                        address_id AS unique_id,
                        array_to_string(
                            list_prepend('0', road_tail_tokens), ' '
                        ) AS clean_full_address,
                        '' AS postcode,
                        '' AS postcode_district,
                        '0' AS rightmost_numeric_value,
                        1 AS rightmost_numeric_position,
                        list_prepend('0', road_tail_tokens) AS peeled_tokens,
                        ['0'] AS numeric_tokens
                    FROM {signatures_table}
                    WHERE allow_truncated_windows
                    UNION ALL
                    SELECT
                        prepared.unique_id,
                        prepared.clean_full_address,
                        prepared.postcode,
                        prepared.postcode_district,
                        prepared.rightmost_numeric_value,
                        prepared.rightmost_numeric_position,
                        prepared.peeled_tokens,
                        prepared.numeric_tokens
                    FROM {signatures_table} AS signatures
                    JOIN {prepared_table} AS prepared
                        ON signatures.address_id = CAST(prepared.unique_id AS VARCHAR)
                    WHERE NOT signatures.allow_truncated_windows
                )"""
            _score_road_candidates(
                con,
                candidate_relation=(
                    f"({
                        roadlike_place_prepared_candidate_sql(
                            candidate_source,
                            catalogue_width_relation=(
                                catalogue_view if require_catalogue_support else None
                            ),
                        )
                    })"
                ),
                output_table=scores_table,
                catalogue_view=catalogue_view,
                scorecard=scorecard,
                require_catalogue_support=require_catalogue_support,
            )
        yield (
            input_name,
            scores_table,
            tails_table if deduplicate_tails else None,
            signatures_table if deduplicate_tails else None,
        )
    finally:
        _drop_table_and_registered_aliases(con, input_name)
        _drop_table_and_registered_aliases(con, prepared_table)
        _drop_table_and_registered_aliases(con, catalogue_view)
        _drop_table_and_registered_aliases(con, scores_table)
        _drop_table_and_registered_aliases(con, tails_table)
        _drop_table_and_registered_aliases(con, signatures_table)


def derive_top_1_road_keys(
    con: duckdb.DuckDBPyConnection,
    address_table: duckdb.DuckDBPyRelation,
    *,
    output_table: str | None = None,
    require_catalogue_support: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Derive one compact road key per unique address identifier."""
    required_columns = {"unique_id", "clean_full_address", "postcode", "numeric_tokens"}
    missing_columns = sorted(required_columns.difference(address_table.columns))
    if missing_columns:
        raise ValueError(
            "Road key derivation requires cleaned address columns; "
            f"missing columns: {missing_columns}"
        )
    if "road_1_norm" in address_table.columns:
        return address_table.select(
            "CAST(unique_id AS VARCHAR) AS unique_id, road_1_norm"
        )

    from uk_address_matcher.sql_pipeline.helpers import _uid

    uid = _uid()
    keys_table = output_table or f"__ukam_road_keys_{uid}"
    with _materialized_road_scores(
        con,
        address_table,
        uid=uid,
        require_catalogue_support=require_catalogue_support,
        deduplicate_tails=True,
    ) as (_, scores_table, tails_table, signatures_table):
        assert tails_table is not None
        assert signatures_table is not None
        con.execute(f"""
            CREATE TEMPORARY TABLE {keys_table} AS
            WITH winners AS (
                SELECT
                    address_id,
                    candidate_phrase AS road_1_norm
                FROM {scores_table}
                QUALIFY row_number() OVER (
                    PARTITION BY address_id
                    ORDER BY ranker_logit DESC, candidate_phrase,
                        candidate_start_position
                ) = 1
            )
            SELECT tails.unique_id, winners.road_1_norm
            FROM {tails_table} AS tails
            JOIN {signatures_table} AS signatures
                USING (road_tail_tokens, allow_truncated_windows)
            JOIN winners ON signatures.address_id = winners.address_id
        """)
    return con.table(keys_table)


def add_top_1_road_features(
    con: duckdb.DuckDBPyConnection,
    address_table: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Attach static-scorecard top-1 road features to cleaned address rows.

    This is intentionally an experiment-only adapter until the road comparison
    completes validation. It reuses the packaged catalogue and scorecard without
    changing candidate generation or matcher blocking.
    """
    required_columns = {"unique_id", "clean_full_address", "postcode", "numeric_tokens"}
    missing_columns = sorted(required_columns.difference(address_table.columns))
    if missing_columns:
        raise ValueError(
            "Road feature derivation requires cleaned address columns; "
            f"missing columns: {missing_columns}"
        )
    if set(ROAD_FEATURE_COLUMNS).issubset(address_table.columns):
        return address_table

    from uk_address_matcher.sql_pipeline.helpers import _uid

    uid = _uid()
    features_table = f"__ukam_road_features_{uid}"
    road_type_tokens = ", ".join(
        sql_text(token) for token in token_policy()["road_syntax_terminal_tokens"]
    )
    unusual_tokens = (
        "coalesce(input.unusual_tokens_arr, []::VARCHAR[])"
        if "unusual_tokens_arr" in address_table.columns
        else "[]::VARCHAR[]"
    )
    with _materialized_road_scores(con, address_table, uid=uid) as (
        input_name,
        scores_table,
        _,
        _,
    ):
        con.execute(
            f"""
            CREATE TEMPORARY TABLE {features_table} AS
            WITH ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY address_id
                        ORDER BY ranker_logit DESC, candidate_phrase,
                            candidate_start_position
                    ) AS candidate_rank,
                    lead(ranker_logit) OVER (
                        PARTITION BY address_id
                        ORDER BY ranker_logit DESC, candidate_phrase,
                            candidate_start_position
                    ) AS runner_up_logit
                FROM {scores_table}
            )
            SELECT
                input.*,
                ranked.candidate_phrase AS road_1_norm,
                ranked.ranker_logit AS road_1_confidence,
                array_length(string_split(ranked.candidate_phrase, ' '))
                    AS road_1_token_count,
                coalesce(
                    ranked.ranker_logit - ranked.runner_up_logit,
                    ranked.ranker_logit
                ) AS road_1_margin,
                list_filter(
                    string_split(ranked.candidate_phrase, ' '),
                    token -> list_contains({unusual_tokens}, token)
                        AND NOT list_contains([{road_type_tokens}], token)
                ) AS road_1_distinctive_tokens
            FROM {input_name} AS input
            LEFT JOIN ranked
                ON CAST(input.unique_id AS VARCHAR) = ranked.address_id
                AND ranked.candidate_rank = 1
            """
        )
    return con.table(features_table)


def add_road_blocking_features(
    con: duckdb.DuckDBPyConnection,
    address_table: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Attach scalar road and outward-postcode keys before blocking."""
    if "road_1_norm" in address_table.columns:
        features = address_table
    else:
        road_keys = derive_top_1_road_keys(con, address_table)
        features = con.sql(f"""
            SELECT input.*, road_keys.road_1_norm
            FROM ({address_table.sql_query()}) AS input
            LEFT JOIN ({road_keys.sql_query()}) AS road_keys
                ON CAST(input.unique_id AS VARCHAR) = road_keys.unique_id
        """)
    if "outward_postcode" in features.columns:
        return features
    return features.select("*, split_part(postcode, ' ', 1) AS outward_postcode")


def add_top_2_road_features(
    con: duckdb.DuckDBPyConnection,
    address_table: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Attach the two highest-scoring supported road phrases as an array."""
    required_columns = {"unique_id", "clean_full_address", "postcode", "numeric_tokens"}
    missing_columns = sorted(required_columns.difference(address_table.columns))
    if missing_columns:
        raise ValueError(
            "Road feature derivation requires cleaned address columns; "
            f"missing columns: {missing_columns}"
        )
    if set(ROAD_TOP_2_FEATURE_COLUMNS).issubset(address_table.columns):
        return address_table

    from uk_address_matcher.sql_pipeline.helpers import _uid

    uid = _uid()
    features_table = f"__ukam_road_top_2_features_{uid}"
    with _materialized_road_scores(
        con,
        address_table,
        uid=uid,
    ) as (input_name, scores_table, _, _):
        con.execute(
            f"""
            CREATE TEMPORARY TABLE {features_table} AS
            WITH ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY address_id
                        ORDER BY ranker_logit DESC, candidate_phrase,
                            candidate_start_position
                    ) AS candidate_rank
                FROM {scores_table}
            ), deduplicated_phrases AS (
                SELECT *
                FROM ranked
                QUALIFY row_number() OVER (
                    PARTITION BY address_id, candidate_phrase
                    ORDER BY candidate_rank
                ) = 1
            ), distinct_ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY address_id ORDER BY candidate_rank
                    ) AS distinct_candidate_rank
                FROM deduplicated_phrases
            ), top_two AS (
                SELECT
                    address_id,
                    list(candidate_phrase ORDER BY distinct_candidate_rank)
                        AS road_top_2_norms
                FROM distinct_ranked
                WHERE distinct_candidate_rank <= 2
                GROUP BY address_id
            )
            SELECT input.*, top_two.road_top_2_norms
            FROM {input_name} AS input
            LEFT JOIN top_two
                ON CAST(input.unique_id AS VARCHAR) = top_two.address_id
            """
        )
    return con.table(features_table)


@pipeline_stage(
    name="derive_rightmost_numeric_position",
    description="Store the suffix-peeled rightmost numeric-token position",
    tags=["token_extraction", "roadlike_places"],
)
def _derive_rightmost_numeric_position() -> str:
    return derive_rightmost_numeric_position_sql("{input}")


@pipeline_stage(
    name="prepare_roadlike_place_input",
    description=(
        "Prepare suffix-peeled tokens and rightmost numeric anchors for road phrases"
    ),
    tags=["roadlike_places", "canonical_artifact"],
)
def _prepare_roadlike_place_input() -> str:
    return roadlike_place_prepared_input_sql("{input}")


@pipeline_stage(
    name="derive_prepared_roadlike_place_candidates",
    description=(
        "Extract terminal-first roadlike candidates from prepared canonical rows"
    ),
    tags=["roadlike_places", "canonical_artifact"],
)
def _derive_prepared_roadlike_place_candidates() -> str:
    return roadlike_place_prepared_candidate_sql("{input}")


@pipeline_stage(
    name="derive_roadlike_place_candidates",
    description=(
        "Extract terminal-first roadlike phrase candidates after the rightmost number"
    ),
    tags=["roadlike_places", "canonical_artifact"],
)
def _derive_roadlike_place_candidates() -> str:
    return roadlike_place_candidate_sql("{input}")


__all__ = [
    "ROAD_FEATURE_COLUMNS",
    "ROAD_TOP_2_FEATURE_COLUMNS",
    "add_top_1_road_features",
    "add_top_2_road_features",
    "_derive_rightmost_numeric_position",
    "_derive_roadlike_place_candidates",
    "_derive_prepared_roadlike_place_candidates",
    "_prepare_roadlike_place_input",
    "derive_rightmost_numeric_position_sql",
    "roadlike_place_candidate_sql",
    "roadlike_place_catalog_sql",
    "roadlike_place_prepared_candidate_sql",
    "roadlike_place_prepared_input_sql",
]
