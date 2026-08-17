from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from uk_address_matcher.sql_pipeline.helpers import _uid

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class NumericRangeRerankerConfig:
    maximum_adjustment_bits: float = 20.0
    minimum_non_numeric_bits: float = -100.0
    endpoint_match_bits: float = 6.0
    interior_match_bits: float = 5.5
    lower_endpoint_tf_weight: float = 0.05
    maximum_interior_span: int = 25
    numeric_candidate_slots: int = 1
    numeric_search_depth: int = 6


_RANGE_STRUCT_COLUMNS = {
    "numeric_range_l",
    "numeric_range_r",
    "numeric_tokens_l",
    "numeric_tokens_r",
    "flat_identity_l",
    "flat_identity_r",
    "legacy_numeric_bits",
}
_PAIR_KEY_COLUMNS = {
    "unique_id_l",
    "unique_id_r",
    "ukam_address_id_l",
    "ukam_address_id_r",
    "match_weight",
}

_LEGACY_NUMERIC_FACTOR_COLUMNS = [
    *(f"bf_numeric_token_{position}" for position in range(1, 4)),
    *(f"bf_tf_adj_numeric_token_{position}" for position in range(1, 4)),
]

_NUMERIC_RANGE_STRUCT_TYPE = (
    "STRUCT(raw VARCHAR, lower UINTEGER, upper UINTEGER, width UINTEGER, "
    "lower_suffix VARCHAR, upper_suffix VARCHAR, role UTINYINT, "
    "flags UTINYINT, lower_tf DOUBLE)"
)


def ensure_numeric_range_struct(
    relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Expose the current numeric-range struct, or a typed NULL when absent."""
    if "numeric_range" in relation.columns:
        return relation
    return relation.select(
        f"*, CAST(NULL AS {_NUMERIC_RANGE_STRUCT_TYPE}) AS numeric_range"
    )


def _validate_legacy_numeric_factors(relation: duckdb.DuckDBPyRelation) -> None:
    missing_factors = [
        column
        for column in _LEGACY_NUMERIC_FACTOR_COLUMNS
        if column not in relation.columns
    ]
    if missing_factors:
        raise ValueError(
            "Numeric-range reranking requires Splink numeric factors: "
            f"{', '.join(missing_factors)}"
        )


def _legacy_numeric_bits_sql() -> str:
    return """
        COALESCE(LOG2(NULLIF(bf_numeric_token_1, 0)), 0.0)
        + COALESCE(LOG2(NULLIF(bf_tf_adj_numeric_token_1, 0)), 0.0)
        + COALESCE(LOG2(NULLIF(bf_numeric_token_2, 0)), 0.0)
        + COALESCE(LOG2(NULLIF(bf_tf_adj_numeric_token_2, 0)), 0.0)
        + COALESCE(LOG2(NULLIF(bf_numeric_token_3, 0)), 0.0)
        + COALESCE(LOG2(NULLIF(bf_tf_adj_numeric_token_3, 0)), 0.0)
            AS legacy_numeric_bits
    """


def add_legacy_numeric_bits(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Build a stable relation with bits for already-filtered range rows.

    Callers must first restrict the relation to numeric range/scalar candidates;
    this function deliberately does not calculate a value for the wider
    prediction relation.
    """
    _validate_legacy_numeric_factors(relation)
    prediction_columns = list(_stable_prediction_columns(relation))
    prediction_columns.extend(
        column
        for column in sorted(_RANGE_STRUCT_COLUMNS - {"legacy_numeric_bits"})
        if column in relation.columns and column not in prediction_columns
    )

    return con.sql(f"""
        SELECT
            {", ".join(prediction_columns)},
            {_legacy_numeric_bits_sql()}
        FROM ({relation.sql_query()}) AS predictions
    """)


def _stable_prediction_columns(relation: duckdb.DuckDBPyRelation) -> list[str]:
    range_blocking_columns = {
        f"{column}_{side}"
        for column in (
            "numeric_range_count",
            "numeric_range_start",
            "numeric_range_end",
            "numeric_range_start_suffix",
            "numeric_range_end_suffix",
            "numeric_range_role",
            "numeric_range_flags",
            "numeric_scalar_tokens",
            "numeric_scalar_suffixes",
            "numeric_scalar_roles",
        )
        for side in ("l", "r")
    }
    excluded_columns = {
        column
        for column in relation.columns
        if column.startswith(("gamma_", "bf_", "tf_"))
        or column.startswith("numeric_token_")
        or column == "legacy_numeric_bits"
        or column in range_blocking_columns
    }
    return [column for column in relation.columns if column not in excluded_columns]


def project_splink_predictions(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    *,
    retain_intermediate_calculation_columns: bool,
) -> duckdb.DuckDBPyRelation:
    """Project prediction output without calculating range bits for all rows."""
    if retain_intermediate_calculation_columns:
        return relation
    return con.sql(f"""
        SELECT {", ".join(_stable_prediction_columns(relation))}
        FROM ({relation.sql_query()}) AS predictions
    """)


def rerank_shortlisted_predictions(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    config: NumericRangeRerankerConfig,
) -> duckdb.DuckDBPyRelation:
    """Apply numeric range adjustments to an already-shortlisted relation.

    The function owns candidate filtering, adjustment construction, and the
    four-key left join back to the shortlist. Non-range pairs are retained with
    neutral diagnostics and a zero adjustment, so callers do not need a second
    numeric-specific branch in their scoring SQL.
    """
    adjustment_name = _materialise_numeric_range_adjustments(con, relation, config)
    stable_columns = _stable_prediction_columns(relation)
    return con.sql(f"""
        SELECT
            {", ".join(f"top_matches.{column}" for column in stable_columns)},
            adjustments.legacy_numeric_bits,
            COALESCE(adjustments.numeric_range_relationship, 'neutral')
                AS numeric_range_relationship,
            COALESCE(adjustments.numeric_range_guard_passed, FALSE)
                AS numeric_range_guard_passed,
            COALESCE(adjustments.numeric_range_guard_reason, 'neutral_or_ineligible')
                AS numeric_range_guard_reason,
            COALESCE(adjustments.numeric_range_base_bits, 0.0)
                AS numeric_range_base_bits,
            COALESCE(adjustments.numeric_range_tf_bits, 0.0)
                AS numeric_range_tf_bits,
            COALESCE(adjustments.numeric_range_adjustment, 0.0)
                AS numeric_range_adjustment
        FROM ({relation.sql_query()}) AS top_matches
        LEFT JOIN {adjustment_name} AS adjustments
          ON adjustments.unique_id_l = top_matches.unique_id_l
         AND adjustments.unique_id_r = top_matches.unique_id_r
         AND adjustments.ukam_address_id_l = top_matches.ukam_address_id_l
         AND adjustments.ukam_address_id_r = top_matches.ukam_address_id_r
    """)


def build_numeric_range_candidate_pool(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    config: NumericRangeRerankerConfig,
    *,
    top_n_matches: int,
    numeric_candidate_slots: int,
    numeric_search_depth: int | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return a bounded shortlist using one unified numeric-adjusted score."""
    if top_n_matches < 1:
        raise ValueError("top_n_matches must be at least 1")
    if numeric_candidate_slots < 0:
        raise ValueError("numeric_candidate_slots cannot be negative")

    stable_columns = _stable_prediction_columns(relation)
    pair_keys = sorted(_PAIR_KEY_COLUMNS - {"match_weight"})
    pair_key_projection = ", ".join(pair_keys)
    pair_join = " AND ".join(
        f"pool.{column} = candidate.{column}" for column in pair_keys
    )
    adjustment_join = " AND ".join(
        f"adjustments.{column} = candidate.{column}" for column in pair_keys
    )
    search_depth = max(top_n_matches, numeric_search_depth or top_n_matches)
    search_top_name = f"__ukam_numeric_search_top_{_uid()}"
    con.sql(f"""
        SELECT {pair_key_projection}, match_weight
        FROM ({relation.sql_query()}) AS candidate
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC, unique_id_l DESC
        ) <= {search_depth}
    """).create(search_top_name)
    raw_top_name = f"__ukam_numeric_raw_top_{_uid()}"
    con.sql(f"""
        SELECT {pair_key_projection}
        FROM {search_top_name}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC, unique_id_l DESC
        ) <= {top_n_matches}
    """).create(raw_top_name)

    adjustment_input_columns = sorted(
        (_RANGE_STRUCT_COLUMNS - {"legacy_numeric_bits"})
        | _PAIR_KEY_COLUMNS
        | set(_LEGACY_NUMERIC_FACTOR_COLUMNS)
    )
    search_join = " AND ".join(
        f"search.{column} = candidate.{column}" for column in pair_keys
    )
    adjustment_input = con.sql(f"""
        SELECT {", ".join(f"candidate.{column}" for column in adjustment_input_columns)}
        FROM ({relation.sql_query()}) AS candidate
        INNER JOIN {search_top_name} AS search ON {search_join}
    """)
    adjustment_name = _materialise_numeric_range_adjustments(
        con,
        adjustment_input,
        config,
    )

    pool_name = raw_top_name
    if numeric_candidate_slots:
        numeric_top_name = f"__ukam_numeric_top_{_uid()}"
        con.sql(f"""
                        SELECT {pair_key_projection}
                        FROM {adjustment_name}
                        WHERE numeric_range_guard_passed
                            AND numeric_range_adjustment > 0.0
            QUALIFY ROW_NUMBER() OVER (
                                PARTITION BY unique_id_r
                ORDER BY
                                        match_weight + numeric_range_adjustment DESC,
                                        unique_id_l DESC
            ) <= {numeric_candidate_slots}
        """).create(numeric_top_name)
        pool_name = f"__ukam_numeric_candidate_pool_{_uid()}"
        con.sql(f"""
            SELECT * FROM {raw_top_name}
            UNION
            SELECT * FROM {numeric_top_name}
        """).create(pool_name)

    return con.sql(f"""
        WITH candidate_pool AS (
            SELECT
                {", ".join(f"candidate.{column}" for column in stable_columns)},
                adjustments.legacy_numeric_bits,
                COALESCE(adjustments.numeric_range_relationship, 'neutral')
                    AS numeric_range_relationship,
                COALESCE(adjustments.numeric_range_guard_passed, FALSE)
                    AS numeric_range_guard_passed,
                COALESCE(
                    adjustments.numeric_range_guard_reason,
                    'neutral_or_ineligible'
                ) AS numeric_range_guard_reason,
                COALESCE(adjustments.numeric_range_base_bits, 0.0)
                    AS numeric_range_base_bits,
                COALESCE(adjustments.numeric_range_tf_bits, 0.0)
                    AS numeric_range_tf_bits,
                COALESCE(adjustments.numeric_range_adjustment, 0.0)
                    AS numeric_range_adjustment
                    FROM ({relation.sql_query()}) AS candidate
                    INNER JOIN {pool_name} AS pool ON {pair_join}
            LEFT JOIN {adjustment_name} AS adjustments
              ON {adjustment_join}
        )
        SELECT *
        FROM candidate_pool
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY
                match_weight + numeric_range_adjustment DESC,
                unique_id_l DESC
        ) <= {top_n_matches}
    """)


def _materialise_numeric_range_adjustments(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    config: NumericRangeRerankerConfig,
) -> str:
    range_candidate_columns = sorted(
        (_RANGE_STRUCT_COLUMNS - {"legacy_numeric_bits"})
        | _PAIR_KEY_COLUMNS
        | set(_LEGACY_NUMERIC_FACTOR_COLUMNS)
    )
    range_candidates = con.sql(f"""
        SELECT {", ".join(range_candidate_columns)}
        FROM ({relation.sql_query()}) AS candidates
        WHERE (
            numeric_range_l IS NOT NULL
            AND numeric_range_r IS NULL
        )
        OR (
            numeric_range_l IS NULL
            AND numeric_range_r IS NOT NULL
        )
    """)
    range_candidates_with_bits = add_legacy_numeric_bits(con, range_candidates)
    adjustments = build_numeric_range_adjustments(
        con,
        range_candidates_with_bits,
        config,
    )
    adjustment_name = f"__ukam_numeric_range_adjustments_{_uid()}"
    adjustments.create(adjustment_name)
    return adjustment_name


def build_numeric_range_adjustments(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    config: NumericRangeRerankerConfig,
) -> duckdb.DuckDBPyRelation:
    """Build one guarded adjustment row per shortlisted range/scalar pair.

    A pair is eligible only when one side has one range and the other side has
    no range. Endpoint matches are accepted at either endpoint when suffix and
    role evidence agree. Interior matches require a plain, increasing range no
    wider than the configured span and a scalar strictly between its endpoints.
    The guard then rejects weak non-numeric evidence and conflicting flat
    identities before the capped adjustment is calculated.
    """
    required_columns = _RANGE_STRUCT_COLUMNS | _PAIR_KEY_COLUMNS
    missing_columns = sorted(required_columns.difference(relation.columns))
    if missing_columns:
        raise ValueError(
            "Numeric-range reranking requires retained columns: "
            f"{', '.join(missing_columns)}"
        )

    source_name = f"__ukam_numeric_range_source_{_uid()}"
    relation.create(source_name)
    range_candidate_name = f"__ukam_numeric_range_candidates_{_uid()}"
    con.sql(f"""
        SELECT *
        FROM {source_name} AS candidate
        WHERE (
            numeric_range_l IS NOT NULL
            AND numeric_range_r IS NULL
        )
        OR (
            numeric_range_l IS NULL
            AND numeric_range_r IS NOT NULL
        )
    """).create(range_candidate_name)

    return con.sql(
        f"""
        WITH attributes AS (
            SELECT
                candidate.*,
                numeric_range_l AS range_l,
                numeric_range_r AS range_r,
                match_weight - legacy_numeric_bits AS non_numeric_bits
            FROM {range_candidate_name} AS candidate
        ),
        scalar_values AS (
            SELECT
                attributes.*,
                list_transform(
                    list_filter(
                        numeric_tokens_l,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> TRY_CAST(
                        regexp_extract(token, '^\\d{{1,5}}', 0) AS UINTEGER
                    )
                ) AS numeric_scalar_tokens_l,
                list_transform(
                    list_filter(
                        numeric_tokens_l,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> NULLIF(regexp_extract(token, '[A-Z]$', 0), '')
                ) AS numeric_scalar_suffixes_l,
                list_transform(
                    list_filter(
                        numeric_tokens_l,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> 0::UTINYINT
                ) AS numeric_scalar_roles_l,
                list_transform(
                    list_filter(
                        numeric_tokens_r,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> TRY_CAST(
                        regexp_extract(token, '^\\d{{1,5}}', 0) AS UINTEGER
                    )
                ) AS numeric_scalar_tokens_r,
                list_transform(
                    list_filter(
                        numeric_tokens_r,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> NULLIF(regexp_extract(token, '[A-Z]$', 0), '')
                ) AS numeric_scalar_suffixes_r,
                list_transform(
                    list_filter(
                        numeric_tokens_r,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    ),
                    token -> 0::UTINYINT
                ) AS numeric_scalar_roles_r
            FROM attributes
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN range_l IS NOT NULL
                        AND len(numeric_scalar_tokens_r) > 0
                        AND (range_l.flags & 27) = 0
                        AND range_l.role <> 3
                        AND len(list_filter(
                            list_zip(
                                numeric_scalar_tokens_r,
                                numeric_scalar_suffixes_r,
                                numeric_scalar_roles_r
                            ),
                            value -> (
                                value[1] IN (range_l.lower, range_l.upper)
                                AND value[2] IS NOT DISTINCT FROM CASE
                                    WHEN value[1] = range_l.lower
                                        THEN range_l.lower_suffix
                                    ELSE range_l.upper_suffix
                                END
                                AND (
                                    value[3] = 0
                                    OR range_l.role = 0
                                    OR value[3] = range_l.role
                                )
                            )
                        )) > 0 THEN 'scalar_range_endpoint'
                    WHEN range_r IS NOT NULL
                        AND len(numeric_scalar_tokens_l) > 0
                        AND (range_r.flags & 27) = 0
                        AND range_r.role <> 3
                        AND len(list_filter(
                            list_zip(
                                numeric_scalar_tokens_l,
                                numeric_scalar_suffixes_l,
                                numeric_scalar_roles_l
                            ),
                            value -> (
                                value[1] IN (range_r.lower, range_r.upper)
                                AND value[2] IS NOT DISTINCT FROM CASE
                                    WHEN value[1] = range_r.lower
                                        THEN range_r.lower_suffix
                                    ELSE range_r.upper_suffix
                                END
                                AND (
                                    value[3] = 0
                                    OR range_r.role = 0
                                    OR value[3] = range_r.role
                                )
                            )
                        )) > 0 THEN 'scalar_range_endpoint'
                    WHEN range_l IS NOT NULL
                        AND range_l.lower < range_l.upper
                        AND range_l.width <= {config.maximum_interior_span}
                        AND (range_l.flags & 31) = 0
                        AND range_l.role <> 3
                        AND range_l.lower_suffix IS NULL
                        AND range_l.upper_suffix IS NULL
                        AND len(list_filter(
                            list_zip(
                                numeric_scalar_tokens_r,
                                numeric_scalar_suffixes_r,
                                numeric_scalar_roles_r
                            ),
                            value -> (
                                value[1] > range_l.lower
                                AND value[1] < range_l.upper
                                AND value[2] IS NULL
                                AND (
                                    value[3] = 0
                                    OR range_l.role = 0
                                    OR value[3] = range_l.role
                                )
                            )
                        )) > 0 THEN 'scalar_range_interior'
                    WHEN range_r IS NOT NULL
                        AND range_r.lower < range_r.upper
                        AND range_r.width <= {config.maximum_interior_span}
                        AND (range_r.flags & 31) = 0
                        AND range_r.role <> 3
                        AND range_r.lower_suffix IS NULL
                        AND range_r.upper_suffix IS NULL
                        AND len(list_filter(
                            list_zip(
                                numeric_scalar_tokens_l,
                                numeric_scalar_suffixes_l,
                                numeric_scalar_roles_l
                            ),
                            value -> (
                                value[1] > range_r.lower
                                AND value[1] < range_r.upper
                                AND value[2] IS NULL
                                AND (
                                    value[3] = 0
                                    OR range_r.role = 0
                                    OR value[3] = range_r.role
                                )
                            )
                        )) > 0 THEN 'scalar_range_interior'
                    ELSE 'neutral'
                END AS numeric_range_relationship
            FROM scalar_values
        ),
        scored AS (
            SELECT
                *,
                CASE
                    WHEN numeric_range_relationship IN (
                        'scalar_range_endpoint', 'scalar_range_interior'
                    )
                    AND non_numeric_bits > {config.minimum_non_numeric_bits}
                    AND (
                        flat_identity_l IS NULL
                        OR flat_identity_r IS NULL
                        OR flat_identity_l = flat_identity_r
                    ) THEN TRUE
                    ELSE FALSE
                END AS numeric_range_guard_passed,
                CASE
                    WHEN numeric_range_relationship NOT IN (
                        'scalar_range_endpoint', 'scalar_range_interior'
                    ) THEN 'neutral_or_ineligible'
                    WHEN non_numeric_bits <= {config.minimum_non_numeric_bits}
                        THEN 'insufficient_non_numeric_evidence'
                    WHEN flat_identity_l IS NOT NULL
                        AND flat_identity_r IS NOT NULL
                        AND flat_identity_l <> flat_identity_r
                        THEN 'flat_conflict'
                    ELSE 'eligible'
                END AS numeric_range_guard_reason,
                CASE
                    WHEN range_l IS NOT NULL
                        THEN range_l.lower_tf
                        ELSE range_r.lower_tf
                END AS lower_endpoint_tf,
                CASE numeric_range_relationship
                    WHEN 'scalar_range_endpoint' THEN {config.endpoint_match_bits}
                    WHEN 'scalar_range_interior' THEN {config.interior_match_bits}
                    ELSE 0.0
                END AS numeric_range_base_bits
            FROM classified
        )
        SELECT
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            match_weight,
            numeric_range_relationship,
            numeric_range_guard_passed,
            numeric_range_guard_reason,
            legacy_numeric_bits,
            numeric_range_base_bits,
            CASE
                WHEN lower_endpoint_tf IS NULL OR lower_endpoint_tf <= 0
                    THEN 0.0
                ELSE {config.lower_endpoint_tf_weight}
                    * log2(1.0 / lower_endpoint_tf)
            END AS numeric_range_tf_bits,
            CASE
                WHEN numeric_range_guard_passed
                    THEN LEAST(
                        {config.maximum_adjustment_bits},
                        GREATEST(
                            0.0,
                            numeric_range_base_bits
                            + CASE
                                WHEN lower_endpoint_tf IS NULL
                                    OR lower_endpoint_tf <= 0 THEN 0.0
                                ELSE {config.lower_endpoint_tf_weight}
                                    * log2(1.0 / lower_endpoint_tf)
                              END
                            - legacy_numeric_bits
                        )
                    )
                ELSE 0.0
            END AS numeric_range_adjustment
        FROM scored
        """
    )
