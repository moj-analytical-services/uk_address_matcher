from __future__ import annotations

from splink.internals.pipeline import CTEPipeline

from .base import PreSplinkIntegration

UPSTREAM_NGRAM_COMPARISON_OUTPUT_COLUMN = "ngram_final_score"

NGRAM_REQUIRED_MATCH_COLUMNS = (
    "clean_full_address",
    "unusual_tokens_arr",
    "very_unusual_tokens_arr",
)

NGRAM_CORE_FEATURE_COLUMNS = ("ngram_final_score",)


def build_blocked_pair_ngram_feature_sql(
    *,
    blocked_with_cols_table: str,
    nodes_table: str,
) -> str:
    trigram_intersection_expr = """
        list_count(
            list_intersect(
                COALESCE(canon_ngrams.canonical_trigrams, []::VARCHAR[]),
                COALESCE(messy_ngrams.messy_trigrams, []::VARCHAR[])
            )
        )
    """

    shared_unusual_token_count_expr = """
        list_count(
            list_distinct(
                list_intersect(
                    COALESCE(unusual_tokens_arr_l, []::VARCHAR[]),
                    COALESCE(unusual_tokens_arr_r, []::VARCHAR[])
                )
            )
        )
    """

    shared_very_unusual_token_count_expr = """
        list_count(
            list_distinct(
                list_intersect(
                    COALESCE(very_unusual_tokens_arr_l, []::VARCHAR[]),
                    COALESCE(very_unusual_tokens_arr_r, []::VARCHAR[])
                )
            )
        )
    """

    unusual_token_union_count_expr = """
        list_count(
            list_distinct(
                list_concat(
                    COALESCE(unusual_tokens_arr_l, []::VARCHAR[]),
                    COALESCE(unusual_tokens_arr_r, []::VARCHAR[])
                )
            )
        )
    """

    very_unusual_token_union_count_expr = """
        list_count(
            list_distinct(
                list_concat(
                    COALESCE(very_unusual_tokens_arr_l, []::VARCHAR[]),
                    COALESCE(very_unusual_tokens_arr_r, []::VARCHAR[])
                )
            )
        )
    """

    trigram_union_expr = f"""
        (
            list_count(
                COALESCE(messy_ngrams.messy_trigrams, []::VARCHAR[])
            )
            + list_count(
                COALESCE(canon_ngrams.canonical_trigrams, []::VARCHAR[])
            )
            - {trigram_intersection_expr}
        )
    """

    final_score_expr = """
        ngram_trigram_jaccard * 0.88
        + ngram_unusual_token_overlap_ratio * 0.08
        + ngram_very_unusual_token_overlap_ratio * 0.04
    """

    return f"""
        WITH canonical_ngram_arrays AS (
            SELECT
                nodes.ukam_address_id,
                CASE
                    WHEN length(nodes.clean_full_address) >= 3 THEN list_distinct(
                        list_transform(
                            range(1, length(nodes.clean_full_address) - 1),
                            i -> '3:' || substring(nodes.clean_full_address, i, 3)
                        )
                    )
                    ELSE []::VARCHAR[]
                END AS canonical_trigrams
            FROM {nodes_table} AS nodes
            WHERE nodes.source_dataset = 'c_'
                AND nodes.clean_full_address IS NOT NULL
                AND nodes.ukam_address_id IN (
                    SELECT ukam_address_id_l
                    FROM {blocked_with_cols_table}
                )
        ),
        messy_ngram_arrays AS (
            SELECT
                nodes.ukam_address_id,
                CASE
                    WHEN length(nodes.clean_full_address) >= 3 THEN list_distinct(
                        list_transform(
                            range(1, length(nodes.clean_full_address) - 1),
                            i -> '3:' || substring(nodes.clean_full_address, i, 3)
                        )
                    )
                    ELSE []::VARCHAR[]
                END AS messy_trigrams
            FROM {nodes_table} AS nodes
            WHERE nodes.source_dataset = 'm_'
                AND nodes.clean_full_address IS NOT NULL
                AND nodes.ukam_address_id IN (
                    SELECT ukam_address_id_r
                    FROM {blocked_with_cols_table}
                )
        ),
        pairwise_component_counts AS (
            SELECT
                blocked.*,
                {shared_unusual_token_count_expr}
                    AS ngram_shared_unusual_token_count,
                {shared_very_unusual_token_count_expr}
                    AS ngram_shared_very_unusual_token_count,
                {unusual_token_union_count_expr}
                    AS ngram_unusual_token_union_count,
                {very_unusual_token_union_count_expr}
                    AS ngram_very_unusual_token_union_count,
                {trigram_intersection_expr} AS ngram_intersection_count,
                {trigram_union_expr} AS ngram_union_count
            FROM {blocked_with_cols_table} AS blocked
            LEFT JOIN canonical_ngram_arrays AS canon_ngrams
                ON blocked.ukam_address_id_l = canon_ngrams.ukam_address_id
            LEFT JOIN messy_ngram_arrays AS messy_ngrams
                ON blocked.ukam_address_id_r = messy_ngrams.ukam_address_id
        ),
        pairwise_metrics AS (
            SELECT
                component_counts.* EXCLUDE (
                    ngram_shared_unusual_token_count,
                    ngram_shared_very_unusual_token_count,
                    ngram_unusual_token_union_count,
                    ngram_very_unusual_token_union_count,
                    ngram_intersection_count,
                    ngram_union_count
                ),
                CASE
                    WHEN component_counts.ngram_unusual_token_union_count = 0 THEN 0.0
                    ELSE component_counts.ngram_shared_unusual_token_count::DOUBLE
                        / component_counts.ngram_unusual_token_union_count::DOUBLE
                END AS ngram_unusual_token_overlap_ratio,
                CASE
                    WHEN (
                        component_counts.ngram_very_unusual_token_union_count = 0
                    ) THEN 0.0
                    ELSE component_counts.ngram_shared_very_unusual_token_count::DOUBLE
                        / component_counts.ngram_very_unusual_token_union_count::DOUBLE
                END AS ngram_very_unusual_token_overlap_ratio,
                CASE
                    WHEN component_counts.ngram_union_count = 0 THEN 0.0
                    ELSE component_counts.ngram_intersection_count::DOUBLE
                        / component_counts.ngram_union_count::DOUBLE
                END AS ngram_trigram_jaccard,
                ({final_score_expr}) AS ngram_final_score
            FROM pairwise_component_counts AS component_counts
        )
        SELECT
            pairwise_metrics.*
        FROM pairwise_metrics
    """


class UpstreamNgramIntegration(PreSplinkIntegration):
    def __init__(self) -> None:
        super().__init__(
            name="ngram_jaccard",
            comparison_output_column=UPSTREAM_NGRAM_COMPARISON_OUTPUT_COLUMN,
            required_match_columns=NGRAM_REQUIRED_MATCH_COLUMNS,
            core_feature_columns=NGRAM_CORE_FEATURE_COLUMNS,
        )

    def enqueue_blocked_pair_feature_sql(
        self,
        pipeline: CTEPipeline,
        *,
        input_table: str,
        nodes_table: str,
    ) -> str:
        table_name = "__ukam__blocked_with_ngram_jaccard_features"
        pipeline.enqueue_sql(
            build_blocked_pair_ngram_feature_sql(
                blocked_with_cols_table=input_table,
                nodes_table=nodes_table,
            ),
            table_name,
        )
        return table_name
