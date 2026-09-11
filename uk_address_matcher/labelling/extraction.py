from __future__ import annotations

from typing import TYPE_CHECKING

from uk_address_matcher.labelling.schema import quote_identifier

if TYPE_CHECKING:
    from duckdb import DuckDBPyRelation

    from uk_address_matcher.post_linkage.match_result.result import MatchResult


def build_final_review_relation(
    *,
    match_result: MatchResult,
    bundle_id: str,
    uk_address_matcher_version: str,
    created_at_utc: str,
    top_n_candidates: int,
    canonical_label_column: str,
    messy_columns: tuple[str, ...],
    canonical_columns: tuple[str, ...],
    canonical_id_type: str,
    canonical_label_type: str,
) -> DuckDBPyRelation:
    """Build the complete bundle relation using the active matching connection."""
    con = match_result.con
    messy_relation = match_result._messy_relation
    canonical_relation = match_result._canonical_relation
    if messy_relation is None or canonical_relation is None:
        raise ValueError("The retained matching relations are unavailable for export.")

    base_sql = _build_base_rows_sql(
        match_result=match_result,
        canonical_label_column=canonical_label_column,
        messy_columns=messy_columns,
        canonical_label_type=canonical_label_type,
    )
    candidate_sql = _build_candidate_rows_sql(
        match_result=match_result,
        base_sql=base_sql,
        canonical_label_column=canonical_label_column,
        canonical_label_type=canonical_label_type,
        canonical_columns=canonical_columns,
    )
    candidate_extra_fields = _candidate_extra_fields_sql(canonical_columns, "candidate")
    messy_projection = _messy_projection_sql(messy_columns)
    empty_candidates = "[]"

    return con.sql(f"""
        WITH
        base_rows AS (
            {base_sql}
        ),
        all_candidates AS (
            {candidate_sql}
        ),
        candidates_to_export AS (
            SELECT *
            FROM all_candidates
            WHERE reranked_rank <= {top_n_candidates}
                OR source <> 'splink'
        ),
        referenced_labels AS (
            SELECT DISTINCT label_id
            FROM candidates_to_export
        ),
        canonical_variants AS (
            SELECT
                canonical.{quote_identifier(canonical_label_column)} AS label_id,
                list(
                    struct_pack(
                        canonical_id := canonical.unique_id,
                        label_id := canonical.{quote_identifier(canonical_label_column)},
                        canonical_address := canonical.clean_full_address::VARCHAR,
                        canonical_postcode := canonical.postcode::VARCHAR
                        {_variant_extra_fields_sql(canonical_columns, "canonical")}
                    )
                    ORDER BY
                        canonical.original_address_concat,
                        canonical.postcode,
                        canonical.unique_id,
                        canonical.ukam_address_id
                ) AS variants
            FROM ({canonical_relation.sql_query()}) AS canonical
            INNER JOIN referenced_labels AS labels
                ON canonical.{quote_identifier(canonical_label_column)}
                    IS NOT DISTINCT FROM labels.label_id
            GROUP BY canonical.{quote_identifier(canonical_label_column)}
        ),
        candidates_with_variants AS (
            SELECT
                candidate.*,
                variants.variants
            FROM candidates_to_export AS candidate
            INNER JOIN canonical_variants AS variants
                ON candidate.label_id IS NOT DISTINCT FROM variants.label_id
        ),
        candidate_summaries AS (
            SELECT
                messy_ukam_address_id,
                MAX(candidate_count) AS candidate_count,
                list(
                    struct_pack(
                        rank := reranked_rank,
                        canonical_id := canonical_id,
                        label_id := label_id,
                        canonical_address := canonical_address,
                        canonical_postcode := canonical_postcode,
                        source := source,
                        splink_rank := splink_rank,
                        splink_match_weight := splink_match_weight,
                        splink_match_probability := splink_match_probability,
                        rerank_adjustment := rerank_adjustment,
                        match_weight := candidate_match_weight,
                        distinguishability := candidate_distinguishability,
                        is_model_selection := is_model_selection,
                        variants := variants
                        {candidate_extra_fields}
                    )
                    ORDER BY reranked_rank
                ) AS top_candidates,
            FROM candidates_with_variants AS candidate
            GROUP BY messy_ukam_address_id
        )
        SELECT
            '{_sql_literal(bundle_id)}' AS bundle_id,
            '{_sql_literal(uk_address_matcher_version)}' AS uk_address_matcher_version,
            CAST('{_sql_literal(created_at_utc)}' AS TIMESTAMPTZ) AS created_at_utc,
            base.unique_id,
            base.messy_address,
            base.messy_cleaned_address,
            base.messy_postcode,
            base.ukam_label,
            base.has_existing_label
            {messy_projection},
            base.resolved_canonical_id,
            base.resolved_label_id,
            base.resolved_canonical_address,
            base.resolved_canonical_postcode,
            base.match_reason,
            base.match_stage,
            base.is_matched,
            base.match_weight,
            base.distinguishability,
            COALESCE(summary.candidate_count, 0)::BIGINT AS candidate_count,
            COALESCE(summary.top_candidates, {empty_candidates}) AS top_candidates
        FROM base_rows AS base
        LEFT JOIN candidate_summaries AS summary
            USING (messy_ukam_address_id)
        ORDER BY base.unique_id
    """)


def _build_base_rows_sql(
    *,
    match_result: MatchResult,
    canonical_label_column: str,
    messy_columns: tuple[str, ...],
    canonical_label_type: str,
) -> str:
    messy_relation = match_result._messy_relation
    canonical_relation = match_result._canonical_relation
    if messy_relation is None or canonical_relation is None:
        raise ValueError("The retained matching relations are unavailable for export.")

    final_relation = match_result._relation
    match_weight = (
        "result.match_weight::DOUBLE"
        if "match_weight" in final_relation.columns
        else "NULL::DOUBLE"
    )
    distinguishability = (
        "result.distinguishability::DOUBLE"
        if "distinguishability" in final_relation.columns
        else "NULL::DOUBLE"
    )
    label = quote_identifier(canonical_label_column)
    messy_extra = "".join(
        f",\n                messy.{quote_identifier(column)}" for column in messy_columns
    )
    ukam_label = (
        f"CAST(messy.ukam_label AS {canonical_label_type})"
        if "ukam_label" in messy_relation.columns
        else f"NULL::{canonical_label_type}"
    )
    has_existing_label = (
        "messy.ukam_label IS NOT NULL"
        if "ukam_label" in messy_relation.columns
        else "FALSE"
    )

    return f"""
        SELECT
            messy.ukam_address_id AS messy_ukam_address_id,
            messy.unique_id,
            messy.original_address_concat::VARCHAR AS messy_address,
            messy.clean_full_address::VARCHAR AS messy_cleaned_address,
            messy.postcode::VARCHAR AS messy_postcode,
            {ukam_label} AS ukam_label,
            {has_existing_label} AS has_existing_label
            {messy_extra},
            CASE WHEN result.resolved_canonical_id IS NOT NULL
                THEN result.resolved_canonical_id END AS resolved_canonical_id,
            result.canonical_ukam_address_id AS resolved_canonical_ukam_address_id,
            CASE WHEN result.resolved_canonical_id IS NOT NULL
                THEN canonical.{label} END AS resolved_label_id,
            CASE WHEN result.resolved_canonical_id IS NOT NULL
                THEN canonical.clean_full_address::VARCHAR END
                AS resolved_canonical_address,
            CASE WHEN result.resolved_canonical_id IS NOT NULL
                THEN canonical.postcode::VARCHAR END AS resolved_canonical_postcode,
            CASE WHEN result.resolved_canonical_id IS NOT NULL
                THEN CAST(result.match_reason AS VARCHAR) END AS match_reason,
            CASE
                WHEN result.resolved_canonical_id IS NULL THEN 'unmatched'
                WHEN CAST(result.match_reason AS VARCHAR) LIKE 'exact%' THEN 'exact'
                WHEN CAST(result.match_reason AS VARCHAR) LIKE 'peeled%' THEN 'peeled'
                WHEN CAST(result.match_reason AS VARCHAR) =
                    'unique_trigram: unique trigram match'
                    THEN 'unique_trigram'
                WHEN CAST(result.match_reason AS VARCHAR) = 'splink: probabilistic match'
                    THEN 'splink'
                ELSE 'unmatched'
            END AS match_stage,
            result.resolved_canonical_id IS NOT NULL AS is_matched,
            CASE WHEN CAST(result.match_reason AS VARCHAR) = 'splink: probabilistic match'
                THEN {match_weight} END AS match_weight,
            CASE WHEN CAST(result.match_reason AS VARCHAR) = 'splink: probabilistic match'
                THEN {distinguishability} END AS distinguishability
        FROM ({messy_relation.sql_query()}) AS messy
        INNER JOIN ({final_relation.sql_query()}) AS result
            ON result.ukam_address_id = messy.ukam_address_id
        LEFT JOIN ({canonical_relation.sql_query()}) AS canonical
            ON canonical.ukam_address_id = result.canonical_ukam_address_id
    """


def _build_candidate_rows_sql(
    *,
    match_result: MatchResult,
    base_sql: str,
    canonical_label_column: str,
    canonical_label_type: str,
    canonical_columns: tuple[str, ...],
) -> str:
    splink_stage = match_result._splink_stage
    has_splink_candidates = (
        splink_stage is not None
        and splink_stage.predictions_table is not None
        and splink_stage.improved_predictions_table is not None
    )
    canonical_relation = match_result._canonical_relation
    if canonical_relation is None:
        raise ValueError("The retained canonical relation is unavailable for export.")
    deterministic_sql = _deterministic_candidates_sql(
        base_sql=base_sql,
        canonical_relation=canonical_relation,
        canonical_columns=canonical_columns,
    )
    if not has_splink_candidates:
        return deterministic_sql

    raw_table = quote_identifier(splink_stage.predictions_table)
    improved_table = quote_identifier(splink_stage.improved_predictions_table)
    label = quote_identifier(canonical_label_column)
    canonical_extra = _candidate_source_fields_sql(
        canonical_columns,
        canonical_label_column,
    )

    return f"""
        WITH
        raw_pairs AS (
            SELECT
                ukam_address_id_r AS messy_ukam_address_id,
                ukam_address_id_l AS canonical_ukam_address_id,
                match_weight::DOUBLE AS splink_match_weight,
                match_probability::DOUBLE AS splink_match_probability
            FROM {raw_table}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ukam_address_id_r, ukam_address_id_l
                ORDER BY match_weight DESC
            ) = 1
        ),
        final_pairs AS (
            SELECT
                ukam_address_id_r AS messy_ukam_address_id,
                ukam_address_id_l AS canonical_ukam_address_id,
                match_weight::DOUBLE AS candidate_match_weight
            FROM {improved_table}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ukam_address_id_r, ukam_address_id_l
                ORDER BY match_weight DESC
            ) = 1
        ),
        pair_candidates AS (
            SELECT
                final_pairs.messy_ukam_address_id,
                canonical.unique_id AS canonical_id,
                canonical.{label} AS label_id,
                canonical.clean_full_address::VARCHAR AS canonical_address,
                canonical.postcode::VARCHAR AS canonical_postcode,
                {canonical_extra}
                raw_pairs.splink_match_weight,
                raw_pairs.splink_match_probability,
                final_pairs.candidate_match_weight,
                final_pairs.candidate_match_weight - raw_pairs.splink_match_weight
                    AS rerank_adjustment,
                final_pairs.canonical_ukam_address_id
            FROM final_pairs
            INNER JOIN raw_pairs
                USING (messy_ukam_address_id, canonical_ukam_address_id)
            INNER JOIN ({canonical_relation.sql_query()}) AS canonical
                ON canonical.ukam_address_id = final_pairs.canonical_ukam_address_id
            WHERE canonical.{label} IS NOT NULL
        ),
        identity_candidates AS (
            SELECT *
            FROM pair_candidates
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY messy_ukam_address_id, label_id
                ORDER BY
                    candidate_match_weight DESC,
                    label_id,
                    canonical_id,
                    canonical_ukam_address_id
            ) = 1
        ),
        ranked_splink_candidates AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY messy_ukam_address_id
                    ORDER BY
                        splink_match_weight DESC,
                        label_id,
                        canonical_id,
                        canonical_ukam_address_id
                ) AS splink_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY messy_ukam_address_id
                    ORDER BY
                        candidate_match_weight DESC,
                        label_id,
                        canonical_id,
                        canonical_ukam_address_id
                ) AS reranked_rank,
                candidate_match_weight - LEAD(candidate_match_weight) OVER (
                    PARTITION BY messy_ukam_address_id
                    ORDER BY
                        candidate_match_weight DESC,
                        label_id,
                        canonical_id,
                        canonical_ukam_address_id
                ) AS candidate_distinguishability,
                COUNT(*) OVER (PARTITION BY messy_ukam_address_id)::BIGINT
                    AS candidate_count
            FROM identity_candidates
        ),
        splink_candidates AS (
            SELECT
                candidate.*,
                'splink' AS source,
                base.is_matched
                    AND base.match_stage = 'splink'
                    AND base.resolved_label_id IS NOT DISTINCT FROM candidate.label_id
                    AS is_model_selection
            FROM ranked_splink_candidates AS candidate
            LEFT JOIN ({base_sql}) AS base
                USING (messy_ukam_address_id)
        )
        SELECT * FROM splink_candidates
        UNION ALL BY NAME
        {deterministic_sql}
    """


def _deterministic_candidates_sql(
    *,
    base_sql: str,
    canonical_relation: DuckDBPyRelation,
    canonical_columns: tuple[str, ...],
) -> str:
    canonical_extra = _deterministic_candidate_fields_sql(canonical_columns)
    return f"""
        SELECT
            base.messy_ukam_address_id,
            base.resolved_canonical_id AS canonical_id,
            base.resolved_label_id AS label_id,
            base.resolved_canonical_address AS canonical_address,
            base.resolved_canonical_postcode AS canonical_postcode,
            base.match_reason AS source,
            {canonical_extra}
            NULL::BIGINT AS splink_rank,
            NULL::DOUBLE AS splink_match_weight,
            NULL::DOUBLE AS splink_match_probability,
            NULL::DOUBLE AS rerank_adjustment,
            NULL::DOUBLE AS candidate_match_weight,
            NULL::DOUBLE AS candidate_distinguishability,
            1::BIGINT AS reranked_rank,
            1::BIGINT AS candidate_count,
            TRUE AS is_model_selection
        FROM ({base_sql}) AS base
        INNER JOIN ({canonical_relation.sql_query()}) AS canonical
            ON canonical.ukam_address_id = base.resolved_canonical_ukam_address_id
        WHERE base.is_matched AND base.match_stage <> 'splink'
    """


def _candidate_source_fields_sql(
    canonical_columns: tuple[str, ...],
    canonical_label_column: str,
) -> str:
    return "".join(
        f"canonical.{quote_identifier(column)} AS {quote_identifier(column)},\n"
        "                "
        for column in canonical_columns
        if column
        not in {
            "unique_id",
            canonical_label_column,
            "original_address_concat",
            "postcode",
        }
    )


def _deterministic_candidate_fields_sql(canonical_columns: tuple[str, ...]) -> str:
    return "".join(
        f"canonical.{quote_identifier(column)} AS {quote_identifier(column)},\n"
        "            "
        for column in canonical_columns
    )


def _variant_extra_fields_sql(columns: tuple[str, ...], alias: str) -> str:
    return "".join(
        f", {quote_identifier(column)} := {alias}.{quote_identifier(column)}"
        for column in columns
    )


def _candidate_extra_fields_sql(columns: tuple[str, ...], alias: str) -> str:
    return "".join(
        f", {quote_identifier(column)} := {alias}.{quote_identifier(column)}"
        for column in columns
    )


def _messy_projection_sql(columns: tuple[str, ...]) -> str:
    return "".join(
        f",\n            base.{quote_identifier(column)}" for column in columns
    )


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")
