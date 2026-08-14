from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.post_linkage.token_classification import (
    substantive_identity_token_sql,
)

NO_POSTCODE_OR_IDENTITY_PENALTY = 8.0
FLAT_LETTER_CONFLICT_PENALTY = 3.0
WEAK_ADDRESS_AND_POSTCODE_PENALTY = 3.0


def improve_predictions_using_structural_evidence(
    *,
    df_predict: DuckDBPyRelation,
    df_canonical: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Apply additive structural-evidence adjustments before candidate ranking."""
    source_identity_sql = substantive_identity_token_sql("token")
    candidate_identity_sql = substantive_identity_token_sql("token")

    return con.sql(f"""
        WITH candidate_uprns AS (
            SELECT DISTINCT
                unique_id_r,
                CAST(unique_id_l AS VARCHAR) AS unique_id_l
            FROM df_predict
        ),
        sources AS (
            SELECT DISTINCT
                unique_id_r,
                coalesce(
                    regexp_replace(upper(postcode_r), '\\s+', '', 'g'),
                    ''
                ) AS source_postcode,
                list_distinct(list_filter(
                    regexp_split_to_array(upper(clean_full_address_r), '\\s+'),
                    token -> {source_identity_sql}
                )) AS source_identity_tokens,
                upper(coalesce(flat_letter_r, '')) AS source_flat_letter,
                flat_number_r AS source_flat_number,
                coalesce(has_flat_indicator_r, FALSE) AS source_has_flat_indicator
            FROM df_predict
        ),
        canonical_variants AS (
            SELECT
                CAST(unique_id AS VARCHAR) AS unique_id_l,
                coalesce(
                    regexp_replace(upper(postcode), '\\s+', '', 'g'),
                    ''
                ) AS candidate_postcode,
                list_distinct(list_filter(
                    regexp_split_to_array(upper(clean_full_address), '\\s+'),
                    token -> {candidate_identity_sql}
                )) AS candidate_identity_tokens,
                upper(coalesce(flat_letter, '')) AS candidate_flat_letter
            FROM df_canonical
        ),
        variant_evidence AS (
            SELECT
                candidates.unique_id_r,
                candidates.unique_id_l,
                bool_or(
                    source.source_postcode != ''
                    AND source.source_postcode = canonical.candidate_postcode
                ) AS candidate_uprn_has_exact_full_postcode,
                bool_or(list_has_any(
                    source.source_identity_tokens,
                    canonical.candidate_identity_tokens
                )) AS candidate_uprn_has_substantive_identity_overlap,
                bool_or(
                    source.source_flat_letter != ''
                    AND source.source_flat_letter = canonical.candidate_flat_letter
                ) AS candidate_uprn_has_exact_flat_letter,
                bool_or(
                    canonical.candidate_flat_letter != ''
                    AND source.source_flat_letter != canonical.candidate_flat_letter
                ) AS candidate_uprn_has_conflicting_flat_letter,
                any_value(source.source_flat_letter) AS source_flat_letter,
                any_value(source.source_flat_number) AS source_flat_number,
                any_value(source.source_has_flat_indicator)
                    AS source_has_flat_indicator
            FROM candidate_uprns AS candidates
            JOIN sources AS source USING (unique_id_r)
            JOIN canonical_variants AS canonical USING (unique_id_l)
            GROUP BY candidates.unique_id_r, candidates.unique_id_l
        ),
        candidate_evidence AS (
            SELECT
                evidence.*,
                bool_or(candidate_uprn_has_exact_flat_letter) OVER (
                    PARTITION BY unique_id_r
                ) AS competing_uprn_has_exact_flat_letter
            FROM variant_evidence AS evidence
        ),
        adjustments AS (
            SELECT
                predictions.*,
                evidence.candidate_uprn_has_exact_full_postcode,
                evidence.candidate_uprn_has_substantive_identity_overlap,
                evidence.candidate_uprn_has_exact_flat_letter,
                evidence.candidate_uprn_has_conflicting_flat_letter,
                evidence.competing_uprn_has_exact_flat_letter,
                CASE
                    WHEN NOT evidence.candidate_uprn_has_exact_full_postcode
                    THEN -predictions.structural_bigram_reward
                    ELSE 0.0
                END AS structural_bigram_adjustment,
                CASE
                    WHEN NOT evidence.candidate_uprn_has_exact_full_postcode
                        AND NOT evidence.candidate_uprn_has_substantive_identity_overlap
                    THEN -{NO_POSTCODE_OR_IDENTITY_PENALTY}
                    ELSE 0.0
                END AS no_postcode_or_identity_adjustment,
                CASE
                    WHEN evidence.source_has_flat_indicator
                        AND evidence.source_flat_letter != ''
                        AND evidence.source_flat_number IS NULL
                        AND NOT evidence.candidate_uprn_has_exact_flat_letter
                        AND evidence.candidate_uprn_has_conflicting_flat_letter
                        AND evidence.competing_uprn_has_exact_flat_letter
                    THEN -{FLAT_LETTER_CONFLICT_PENALTY}
                    ELSE 0.0
                END AS flat_letter_conflict_adjustment,
                CASE
                    WHEN NOT evidence.candidate_uprn_has_exact_full_postcode
                        AND predictions.gamma_postcode <= 2
                        AND predictions.gamma_address_without_numbers = 0
                    THEN -{WEAK_ADDRESS_AND_POSTCODE_PENALTY}
                    ELSE 0.0
                END AS weak_address_and_postcode_adjustment
            FROM df_predict AS predictions
            JOIN candidate_evidence AS evidence
                ON evidence.unique_id_r = predictions.unique_id_r
                AND evidence.unique_id_l = CAST(predictions.unique_id_l AS VARCHAR)
        )
        SELECT
            adjustments.* EXCLUDE (match_weight),
            match_weight
                + structural_bigram_adjustment
                + no_postcode_or_identity_adjustment
                + flat_letter_conflict_adjustment
                + weak_address_and_postcode_adjustment AS match_weight
        FROM adjustments
    """)
