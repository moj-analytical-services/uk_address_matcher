from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True, repr=False)
class NgramJaccardStage(MatchingStage):
    """Match residual address records with bounded fuzzy candidate ranking.

    This stage is intended for records that were not solved by earlier, more
    deterministic stages (for example exact and peeled matching). It runs in two
    retrieval/scoring rounds per postcode block:

    1. Generate candidate pairs from rare shared word tokens.
    2. Score the shortlist with character ngram overlap and consistency features,
       then select one winner per messy record.

    The design keeps precision high by combining hard gates (blocking, minimum
    shared rare tokens, minimum score) with ranking controls (candidate caps and
    optional score-gap requirement).

    Metrics in the signature:

        - `min_jaccard`:
            Lower bound for the final similarity score used to accept a candidate.
            Higher values are stricter and usually increase precision while reducing
            recall. With a value of 0.8, we expect 5/6 tokens to overlap.
        - `use_postcode_fallback`:
            Whether to run a second pass that relaxes postcode blocking from full
            postcode equality to postcode-with-last-character-dropped for unresolved
            rows.
        - `num_of_chunks`:
            Optional number of deterministic hash chunks for unmatched input rows.
            Useful for controlling memory/latency on large batches by running several
            smaller passes and unioning their results.
        - `max_token_frequency`:
            Upper frequency threshold for tokens used in round-1 candidate retrieval.
            Very common tokens (for example generic street words) are excluded so they
            do not create large, low-value candidate fan-out.
        - `candidate_cap_per_messy`:
            Maximum number of round-1 candidates retained per messy record before
            detailed scoring. This bounds worst-case work in round 2.
        - `min_shared_rare_tokens`:
            Minimum count of shared retained tokens required for a pair to remain a
            candidate after round 1.
        - `min_score_gap`:
            Optional minimum margin between the top-ranked and second-ranked candidate
            scores per messy record. When set, ambiguous near-ties are rejected.
    """

    min_jaccard: float = 0.82
    use_postcode_fallback: bool = True
    num_of_chunks: int | None = None
    max_token_frequency: int = 800
    candidate_cap_per_messy: int = 60
    min_shared_rare_tokens: int = 1
    min_score_gap: float | None = None

    def __post_init__(self) -> None:
        if self.min_jaccard < 0.0 or self.min_jaccard > 1.0:
            raise ValueError("min_jaccard must be between 0.0 and 1.0 inclusive.")
        if self.num_of_chunks is not None and self.num_of_chunks < 1:
            raise ValueError("num_of_chunks must be at least 1 when provided.")
        if self.max_token_frequency < 1:
            raise ValueError("max_token_frequency must be at least 1.")
        if self.candidate_cap_per_messy < 1:
            raise ValueError("candidate_cap_per_messy must be at least 1.")
        if self.min_shared_rare_tokens < 1:
            raise ValueError("min_shared_rare_tokens must be at least 1.")
        if self.min_score_gap is not None and self.min_score_gap < 0.0:
            raise ValueError("min_score_gap must be >= 0.0 when provided.")

    def _run_single_pass(
        self,
        *,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        postcode_strategy: str,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_messy_postcodes(postcode_strategy),
                _ngram_jaccard_matches(
                    postcode_strategy=postcode_strategy,
                    min_jaccard=self.min_jaccard,
                    max_token_frequency=self.max_token_frequency,
                    candidate_cap_per_messy=self.candidate_cap_per_messy,
                    min_shared_rare_tokens=self.min_shared_rare_tokens,
                    min_score_gap=self.min_score_gap,
                ),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )

    def _run_pass(
        self,
        *,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        postcode_strategy: str,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        if explain or self.num_of_chunks is None:
            return self._run_single_pass(
                con=con,
                stage_name=stage_name,
                df_unmatched=df_unmatched,
                df_canonical=df_canonical,
                postcode_strategy=postcode_strategy,
                debug_options=debug_options,
                explain=explain,
            )

        total_rows = df_unmatched.count("*").fetchone()[0]
        if total_rows == 0:
            return self._run_single_pass(
                con=con,
                stage_name=stage_name,
                df_unmatched=df_unmatched,
                df_canonical=df_canonical,
                postcode_strategy=postcode_strategy,
                debug_options=debug_options,
                explain=explain,
            )

        chunk_count = min(self.num_of_chunks, total_rows)
        uid = _uid()
        result_table = f"__ukam_ngram_jaccard_{postcode_strategy}_{uid}"
        first_chunk = True

        if postcode_strategy == "exact":
            chunk_key_expr = "COALESCE(postcode, '__null__')"
        else:
            chunk_key_expr = """
                COALESCE(
                    CASE
                        WHEN postcode IS NULL OR LENGTH(postcode) <= 1 THEN NULL
                        ELSE LEFT(postcode, LENGTH(postcode) - 1)
                    END,
                    '__null__'
                )
            """

        for chunk_index in range(chunk_count):
            chunk_unmatched = con.sql(
                f"""
                SELECT *
                FROM ({df_unmatched.sql_query()}) AS unmatched
                WHERE (abs(hash({chunk_key_expr})) % {chunk_count}) = {chunk_index}
                """
            )

            if chunk_unmatched.count("*").fetchone()[0] == 0:
                continue

            chunk_matches = self._run_single_pass(
                con=con,
                stage_name=f"{stage_name}_chunk_{chunk_index + 1}_of_{chunk_count}",
                df_unmatched=chunk_unmatched,
                df_canonical=df_canonical,
                postcode_strategy=postcode_strategy,
                debug_options=debug_options if first_chunk else None,
                explain=False,
            )

            if chunk_matches is None:
                continue

            if first_chunk:
                con.execute(f"DROP TABLE IF EXISTS {result_table}")
                chunk_matches.create(result_table)
                first_chunk = False
            else:
                chunk_matches.insert_into(result_table)

        if first_chunk:
            return self._run_single_pass(
                con=con,
                stage_name=stage_name,
                df_unmatched=df_unmatched.limit(0),
                df_canonical=df_canonical,
                postcode_strategy=postcode_strategy,
                debug_options=debug_options,
                explain=False,
            )

        return con.table(result_table)

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        def _materialise_relation(
            relation: duckdb.DuckDBPyRelation,
            table_prefix: str,
        ) -> duckdb.DuckDBPyRelation:
            table_name = f"__ukam_{table_prefix}_{_uid()}"
            con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            relation.create(f'"{table_name}"')
            return con.table(f'"{table_name}"')

        strict_matches = self._run_pass(
            con=con,
            stage_name=f"{stage_name}_exact_postcode",
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            postcode_strategy="exact",
            debug_options=debug_options,
            explain=explain,
        )

        if strict_matches is not None and not explain:
            strict_matches = _materialise_relation(
                strict_matches,
                f"{stage_name}_strict_matches",
            )

        if explain or not self.use_postcode_fallback or strict_matches is None:
            return strict_matches

        unresolved_after_strict = con.sql(
            f"""
            SELECT unmatched.*
            FROM ({df_unmatched.sql_query()}) AS unmatched
            LEFT JOIN ({strict_matches.sql_query()}) AS strict
                ON unmatched.ukam_address_id = strict.ukam_address_id
            WHERE strict.ukam_address_id IS NULL
            """
        )

        if unresolved_after_strict.count("*").fetchone()[0] == 0:
            return strict_matches

        fallback_matches = self._run_pass(
            con=con,
            stage_name=f"{stage_name}_postcode_fallback",
            df_unmatched=unresolved_after_strict,
            df_canonical=df_canonical,
            postcode_strategy="drop_last_char",
            debug_options=debug_options,
            explain=explain,
        )

        if fallback_matches is not None and not explain:
            fallback_matches = _materialise_relation(
                fallback_matches,
                f"{stage_name}_fallback_matches",
            )

        if fallback_matches is None:
            return strict_matches

        return con.sql(
            f"""
            SELECT *
            FROM ({strict_matches.sql_query()})
            UNION ALL
            SELECT fallback.*
            FROM ({fallback_matches.sql_query()}) AS fallback
            LEFT JOIN ({strict_matches.sql_query()}) AS strict
                ON fallback.ukam_address_id = strict.ukam_address_id
            WHERE strict.ukam_address_id IS NULL
            """
        )


@pipeline_stage(
    name="ngram_jaccard_matches",
    description=(
        "Score postcode-blocked candidates with character unigram+bigram "
        "Jaccard similarity and keep top-1 deterministic match"
    ),
    tags=["phase_1", "matching", "ngram", "jaccard"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _ngram_jaccard_matches(
    *,
    postcode_strategy: str,
    min_jaccard: float,
    max_token_frequency: int,
    candidate_cap_per_messy: int,
    min_shared_rare_tokens: int,
    min_score_gap: float | None,
) -> list[CTEStep]:
    if postcode_strategy not in {"exact", "drop_last_char"}:
        raise ValueError(
            "postcode_strategy must be 'exact' or 'drop_last_char'. "
            f"Got '{postcode_strategy}'."
        )

    if postcode_strategy == "exact":
        join_key_expr = "messy.postcode = canon.postcode"
        block_label = "full_postcode"
    else:
        join_key_expr = """
            CASE
                WHEN messy.postcode IS NULL OR LENGTH(messy.postcode) <= 1 THEN NULL
                ELSE LEFT(messy.postcode, LENGTH(messy.postcode) - 1)
            END
            =
            CASE
                WHEN canon.postcode IS NULL OR LENGTH(canon.postcode) <= 1 THEN NULL
                ELSE LEFT(canon.postcode, LENGTH(canon.postcode) - 1)
            END
        """
        block_label = "postcode_drop_last_char"

    match_reason_value = MatchReason.NGRAM_JACCARD.value
    enum_values = str(MatchReason.enum_values())

    messy_phrase_tokens_sql = """
        WITH normalised AS (
            SELECT
                ukam_address_id,
                regexp_replace(
                    upper(trim(clean_full_address)),
                    '\\s+',
                    ' ',
                    'g'
                ) AS clean_address
            FROM {__ukam__tmp_messy_addresses}
            WHERE clean_full_address IS NOT NULL
        ),
        tokenised AS (
            SELECT
                ukam_address_id,
                string_split(clean_address, ' ') AS tokens
            FROM normalised
        )
        SELECT DISTINCT
            t.ukam_address_id,
            tok.token AS token
        FROM tokenised AS t,
        UNNEST(t.tokens) AS tok(token)
        WHERE length(tok.token) >= 2
    """

    canonical_phrase_tokens_sql = """
        WITH normalised AS (
            SELECT
                ukam_address_id,
                canonical_unique_id,
                regexp_replace(
                    upper(trim(clean_full_address)),
                    '\\s+',
                    ' ',
                    'g'
                ) AS clean_address
            FROM {canonical_addresses_restricted}
            WHERE clean_full_address IS NOT NULL
        ),
        tokenised AS (
            SELECT
                ukam_address_id,
                canonical_unique_id,
                string_split(clean_address, ' ') AS tokens
            FROM normalised
        )
        SELECT DISTINCT
            t.ukam_address_id,
            t.canonical_unique_id,
            tok.token AS token
        FROM tokenised AS t,
        UNNEST(t.tokens) AS tok(token)
        WHERE length(tok.token) >= 2
    """

    messy_token_frequency_sql = """
        SELECT token, COUNT(*) AS messy_freq
        FROM {round1_messy_phrase_tokens}
        GROUP BY token
    """

    canonical_token_frequency_sql = """
        SELECT token, COUNT(*) AS canonical_freq
        FROM {round1_canonical_phrase_tokens}
        GROUP BY token
    """

    rare_tokens_sql = f"""
        SELECT
            m.token
        FROM {{round1_messy_token_frequency}} AS m
        INNER JOIN {{round1_canonical_token_frequency}} AS c
            ON m.token = c.token
        WHERE GREATEST(m.messy_freq, c.canonical_freq) <= {max_token_frequency}
    """

    joined_postings_sql = """
        SELECT
            m.ukam_address_id AS messy_ukam_address_id,
            c.ukam_address_id AS canonical_ukam_address_id,
            c.canonical_unique_id,
            m.token
        FROM {round1_messy_phrase_tokens} AS m
        INNER JOIN {round1_canonical_phrase_tokens} AS c
            ON m.token = c.token
        INNER JOIN {round1_rare_tokens} AS rare
            ON m.token = rare.token
    """

    round1_candidate_pairs_sql = """
        SELECT
            messy_ukam_address_id,
            canonical_ukam_address_id,
            MIN(canonical_unique_id) AS canonical_unique_id,
            COUNT(*) AS shared_rare_token_count
        FROM {round1_joined_postings}
        GROUP BY
            messy_ukam_address_id,
            canonical_ukam_address_id
    """

    candidates_sql = f"""
        SELECT
            messy_ukam_address_id,
            canonical_ukam_address_id,
            canonical_unique_id,
            shared_rare_token_count
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY messy_ukam_address_id
                    ORDER BY
                        shared_rare_token_count DESC,
                        canonical_ukam_address_id ASC
                ) AS rn
            FROM {{round1_candidate_pairs}}
            WHERE shared_rare_token_count >= {min_shared_rare_tokens}
        ) AS ranked
        WHERE rn <= {candidate_cap_per_messy}
    """

    candidate_messy_ids_sql = """
        SELECT DISTINCT messy_ukam_address_id AS ukam_address_id
        FROM {candidate_pairs}
    """

    candidate_canonical_ids_sql = """
        SELECT DISTINCT canonical_ukam_address_id AS ukam_address_id
        FROM {candidate_pairs}
    """

    messy_char_ngrams_sql = """
        WITH normalised AS (
            SELECT
                ukam_address_id,
                regexp_replace(
                    upper(trim(clean_full_address)),
                    '\\s+',
                    ' ',
                    'g'
                ) AS clean_address
            FROM {__ukam__tmp_messy_addresses}
            INNER JOIN {candidate_messy_ids}
                USING (ukam_address_id)
            WHERE clean_full_address IS NOT NULL
        ),
        trigrams AS (
            SELECT
                n.ukam_address_id,
                '3:' || substring(n.clean_address, i, 3) AS gram
            FROM normalised AS n,
            UNNEST(range(1, length(n.clean_address) - 1)) AS u(i)
        )
        SELECT DISTINCT ukam_address_id, gram
        FROM trigrams
    """

    canonical_char_ngrams_sql = """
        WITH normalised AS (
            SELECT
                ukam_address_id,
                regexp_replace(
                    upper(trim(clean_full_address)),
                    '\\s+',
                    ' ',
                    'g'
                ) AS clean_address
            FROM {canonical_addresses_restricted}
            INNER JOIN {candidate_canonical_ids}
                USING (ukam_address_id)
            WHERE clean_full_address IS NOT NULL
        ),
        trigrams AS (
            SELECT
                n.ukam_address_id,
                '3:' || substring(n.clean_address, i, 3) AS gram
            FROM normalised AS n,
            UNNEST(range(1, length(n.clean_address) - 1)) AS u(i)
        )
        SELECT DISTINCT ukam_address_id, gram
        FROM trigrams
    """

    candidate_features_sql = """
        SELECT
            pair.messy_ukam_address_id,
            pair.canonical_ukam_address_id,
            pair.canonical_unique_id,
            pair.shared_rare_token_count,
            messy.postcode AS messy_postcode,
            canon.postcode AS canonical_postcode,
            list_extract(messy.numeric_tokens, 1) AS messy_primary_number,
            list_extract(canon.numeric_tokens, 1) AS canonical_primary_number,
            CASE
                WHEN messy.postcode IS NOT NULL
                    AND canon.postcode IS NOT NULL
                    AND messy.postcode = canon.postcode
                THEN 1
                ELSE 0
            END AS postcode_exact,
            CASE
                WHEN messy.postcode IS NOT NULL
                    AND canon.postcode IS NOT NULL
                    AND LENGTH(messy.postcode) > 1
                    AND LENGTH(canon.postcode) > 1
                    AND LEFT(messy.postcode, LENGTH(messy.postcode) - 1)
                        = LEFT(canon.postcode, LENGTH(canon.postcode) - 1)
                THEN 1
                ELSE 0
            END AS postcode_prefix,
            CASE
                WHEN messy.numeric_tokens IS NOT NULL
                    AND canon.numeric_tokens IS NOT NULL
                    AND array_length(messy.numeric_tokens) > 0
                    AND array_length(canon.numeric_tokens) > 0
                    AND list_extract(messy.numeric_tokens, 1) IS NOT NULL
                    AND list_extract(canon.numeric_tokens, 1) IS NOT NULL
                    AND list_extract(messy.numeric_tokens, 1)
                        != list_extract(canon.numeric_tokens, 1)
                    AND list_sort(list_distinct(messy.numeric_tokens))
                        != list_sort(list_distinct(canon.numeric_tokens))
                THEN 1
                ELSE 0
            END AS numeric_set_mismatch
        FROM {candidate_pairs} AS pair
        INNER JOIN {__ukam__tmp_messy_addresses} AS messy
            ON pair.messy_ukam_address_id = messy.ukam_address_id
        INNER JOIN {canonical_addresses_restricted} AS canon
            ON pair.canonical_ukam_address_id = canon.ukam_address_id
    """

    messy_ngram_counts_sql = """
        SELECT
            ukam_address_id AS messy_ukam_address_id,
            COUNT(*) AS messy_ngram_count
        FROM {messy_char_ngrams}
        GROUP BY ukam_address_id
    """

    canonical_ngram_counts_sql = """
        SELECT
            ukam_address_id AS canonical_ukam_address_id,
            COUNT(*) AS canonical_ngram_count
        FROM {canonical_char_ngrams}
        GROUP BY ukam_address_id
    """

    pair_intersections_sql = """
        SELECT
            pair.messy_ukam_address_id,
            pair.canonical_ukam_address_id,
            pair.canonical_unique_id,
            COUNT(*) AS intersection_count
        FROM {candidate_pairs} AS pair
        INNER JOIN {messy_char_ngrams} AS messy_gram
            ON pair.messy_ukam_address_id = messy_gram.ukam_address_id
        INNER JOIN {canonical_char_ngrams} AS canon_gram
            ON pair.canonical_ukam_address_id = canon_gram.ukam_address_id
            AND messy_gram.gram = canon_gram.gram
        GROUP BY
            pair.messy_ukam_address_id,
            pair.canonical_ukam_address_id,
            pair.canonical_unique_id
    """

    scored_pairs_sql = """
        SELECT
            features.messy_ukam_address_id,
            features.canonical_ukam_address_id,
            features.canonical_unique_id,
            features.shared_rare_token_count,
            inter.intersection_count,
            (messy_counts.messy_ngram_count + canon_counts.canonical_ngram_count
                - inter.intersection_count
            ) AS union_count,
            CASE
                WHEN (
                    messy_counts.messy_ngram_count
                    + canon_counts.canonical_ngram_count
                    - inter.intersection_count
                ) = 0 THEN 0.0
                ELSE inter.intersection_count::DOUBLE
                    / (
                        messy_counts.messy_ngram_count
                        + canon_counts.canonical_ngram_count
                        - inter.intersection_count
                    )::DOUBLE
            END AS jaccard_similarity,
            features.postcode_exact,
            features.postcode_prefix,
            features.messy_primary_number,
            features.canonical_primary_number,
            features.numeric_set_mismatch,
            CASE
                WHEN features.messy_primary_number IS NOT NULL
                    AND features.canonical_primary_number IS NOT NULL
                    AND features.messy_primary_number = features.canonical_primary_number
                THEN 1
                ELSE 0
            END AS primary_number_match,
            CASE
                WHEN features.messy_primary_number IS NOT NULL
                    AND features.canonical_primary_number IS NOT NULL
                    AND features.messy_primary_number != features.canonical_primary_number
                    AND features.numeric_set_mismatch = 1
                THEN 1
                ELSE 0
            END AS primary_number_disagree
        FROM {pair_intersections} AS inter
        INNER JOIN {candidate_features} AS features
            ON inter.messy_ukam_address_id = features.messy_ukam_address_id
            AND inter.canonical_ukam_address_id = features.canonical_ukam_address_id
        INNER JOIN {messy_ngram_counts} AS messy_counts
            ON inter.messy_ukam_address_id = messy_counts.messy_ukam_address_id
        INNER JOIN {canonical_ngram_counts} AS canon_counts
            ON inter.canonical_ukam_address_id = canon_counts.canonical_ukam_address_id
    """

    scored_pairs_with_final_score_sql = """
        SELECT
            *,
            (
                jaccard_similarity * 0.80
                + (LEAST(shared_rare_token_count, 8)::DOUBLE / 8.0) * 0.10
                + postcode_exact::DOUBLE * 0.05
                + primary_number_match::DOUBLE * 0.05
                - CASE
                    WHEN messy_primary_number IS NOT NULL
                        AND canonical_primary_number IS NULL
                    THEN 0.08
                    ELSE 0.0
                END
            ) AS final_score
        FROM {scored_pairs}
        WHERE NOT (
            numeric_set_mismatch = 1
            OR
            primary_number_disagree = 1
            AND jaccard_similarity < 0.99
        )
    """

    score_gap_predicate_sql = ""
    if min_score_gap is not None:
        score_gap_predicate_sql = (
            f"AND (second_score IS NULL OR final_score - second_score >= {min_score_gap})"
        )

    ranked_pairs_sql = f"""
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY messy_ukam_address_id
                ORDER BY final_score DESC, jaccard_similarity DESC, canonical_ukam_address_id ASC
            ) AS rn,
            LEAD(final_score) OVER (
                PARTITION BY messy_ukam_address_id
                ORDER BY final_score DESC, jaccard_similarity DESC, canonical_ukam_address_id ASC
            ) AS second_score
        FROM {{scored_pairs_with_final_score}}
        WHERE final_score >= {min_jaccard}
    """

    final_matches_sql = f"""
        SELECT
            messy_ukam_address_id AS ukam_address_id,
            canonical_ukam_address_id,
            canonical_unique_id AS resolved_canonical_id,
            shared_rare_token_count,
            intersection_count,
            union_count,
            jaccard_similarity,
            final_score,
            '{block_label}'::VARCHAR AS blocking_strategy,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason
        FROM {{ranked_pairs}}
        WHERE rn = 1
        {score_gap_predicate_sql}
    """

    return [
        CTEStep("round1_messy_phrase_tokens", messy_phrase_tokens_sql),
        CTEStep("round1_canonical_phrase_tokens", canonical_phrase_tokens_sql),
        CTEStep("round1_messy_token_frequency", messy_token_frequency_sql),
        CTEStep("round1_canonical_token_frequency", canonical_token_frequency_sql),
        CTEStep("round1_rare_tokens", rare_tokens_sql),
        CTEStep("round1_joined_postings", joined_postings_sql),
        CTEStep("round1_candidate_pairs", round1_candidate_pairs_sql),
        CTEStep("candidate_pairs", candidates_sql),
        CTEStep("candidate_messy_ids", candidate_messy_ids_sql),
        CTEStep("candidate_canonical_ids", candidate_canonical_ids_sql),
        CTEStep("messy_char_ngrams", messy_char_ngrams_sql),
        CTEStep("canonical_char_ngrams", canonical_char_ngrams_sql),
        CTEStep("messy_ngram_counts", messy_ngram_counts_sql),
        CTEStep("canonical_ngram_counts", canonical_ngram_counts_sql),
        CTEStep("candidate_features", candidate_features_sql),
        CTEStep("pair_intersections", pair_intersections_sql),
        CTEStep("scored_pairs", scored_pairs_sql),
        CTEStep("scored_pairs_with_final_score", scored_pairs_with_final_score_sql),
        CTEStep("ranked_pairs", ranked_pairs_sql),
        CTEStep("ngram_jaccard_matches", final_matches_sql),
    ]
