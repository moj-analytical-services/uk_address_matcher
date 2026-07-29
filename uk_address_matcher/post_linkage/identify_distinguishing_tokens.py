from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation

_POSITIONAL_TOKENS_SQL = "('LEFT', 'RIGHT', 'CENTRE', 'FRONT')"


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
    top_n_matches: int = 5,
    use_bigrams: bool = True,
    additional_columns_to_retain: list[str] | None = None,
    histogram_eligibility_column: str | None = None,
    REWARD_MULTIPLIER: float = 3.0,
    PUNISHMENT_MULTIPLIER: float = 1.65,
    BIGRAM_REWARD_MULTIPLIER: float = 2.2,
    BIGRAM_PUNISHMENT_MULTIPLIER: float = 1.15,
    MISSING_TOKEN_PENALTY: float = 0.0,
    POSITIONAL_CONFLICT_PENALTY: float = 6.0,
) -> DuckDBPyRelation:
    matches_table = "__ukam__distinguishability_matches"

    retained_columns = ""
    if additional_columns_to_retain:
        retained_columns = "".join(
            f"{column}_l, {column}_r, " for column in additional_columns_to_retain
        )
    if "ukam_label_r" in df_predict.columns:
        retained_columns += "ukam_label_r, "

    eligibility_filter = ""
    if histogram_eligibility_column is not None:
        if histogram_eligibility_column not in df_predict.columns:
            raise ValueError(
                "histogram_eligibility_column must exist in df_predict: "
                f"{histogram_eligibility_column}"
            )
        eligibility_filter = f"WHERE COALESCE({histogram_eligibility_column}, FALSE)"

    candidate_token_lists_sql = """
        array_agg(candidate_tokens).
        list_transform(candidate_tokens -> list_distinct(candidate_tokens))
    """
    canonical_bigrams_sql = """
        list_transform(
            array_agg(candidate_tokens),
            candidate_tokens -> list_distinct(
                list_transform(
                    list_zip(
                        list_slice(candidate_tokens, 1, length(candidate_tokens) - 1),
                        list_slice(candidate_tokens, 2, length(candidate_tokens))
                    ),
                    pair -> ARRAY[pair[1], pair[2]]
                )
            )
        ).flatten() AS bigrams_in_block_l,
        """

    con.sql(f"""
        SELECT *
        FROM df_predict
        WHERE match_weight > {match_weight_threshold}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r, unique_id_l
            ORDER BY match_weight DESC, ukam_address_id_r DESC, ukam_address_id_l DESC
        ) = 1
    """).create("good_matches")

    con.sql(f"""
        SELECT *
        FROM good_matches
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY unique_id_r
            ORDER BY match_weight DESC, unique_id_l DESC
        ) <= {top_n_matches}
    """).create("top_n_matches")

    con.sql("""
        WITH intermediate AS (
            SELECT *, map_keys(common_end_tokens_hist_r) AS common_end_tokens_r
            FROM top_n_matches
        ),
        enriched AS (
            SELECT
                *,
                COALESCE(
                    common_end_tokens_r.list_transform(
                        value -> COALESCE(
                            struct_extract(
                                TRY_CAST(value AS STRUCT(tok VARCHAR, rel_freq DOUBLE)),
                                'tok'
                            ),
                            TRY_CAST(value AS VARCHAR)
                        )
                    ),
                    CAST([] AS VARCHAR[])
                ) AS common_end_tokens_tok
            FROM intermediate
        )
        SELECT
            *,
            clean_full_address_l
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .list_reverse()
                .list_filter((token, position) -> NOT (
                    position = 1 AND common_end_tokens_tok.list_contains(token)
                ))
                .list_filter((token, position) -> NOT (
                    position = 1 AND common_end_tokens_tok.list_contains(token)
                ))
                .list_reverse()
                .array_to_string(' ') AS __token_address_l,
            clean_full_address_r
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .list_reverse()
                .list_filter((token, position) -> NOT (
                    position = 1 AND common_end_tokens_tok.list_contains(token)
                ))
                .list_filter((token, position) -> NOT (
                    position = 1 AND common_end_tokens_tok.list_contains(token)
                ))
                .list_reverse()
                .array_to_string(' ') AS __token_address_r
        FROM enriched
    """).create("token_addresses")

    con.sql(f"""
        WITH source_tokens AS (
            SELECT DISTINCT
                ukam_address_id_r,
                concat_ws(' ', __token_address_r, postcode_r)
                    .trim()
                    .upper()
                    .regexp_split_to_array('\\s+') AS tokens_r
            FROM token_addresses
        ),
        block_tokens AS (
            SELECT
                source.ukam_address_id_r,
                source.tokens_r,
                concat_ws(' ', candidate.__token_address_l, candidate.postcode_l)
                    .trim()
                    .upper()
                    .regexp_split_to_array('\\s+') AS candidate_tokens
            FROM token_addresses AS candidate
            JOIN source_tokens AS source USING (ukam_address_id_r)
            {eligibility_filter}
        ),
        block_histograms AS (
            SELECT
                ukam_address_id_r,
                ANY_VALUE(tokens_r) AS tokens_r,
                list_aggregate(
                    {candidate_token_lists_sql}.flatten(),
                    'histogram'
                ) AS hist_all_tokens_in_block_l,
                {canonical_bigrams_sql}
            FROM block_tokens
            GROUP BY ukam_address_id_r
        )
        SELECT
            *,
            map_from_entries(
                list_filter(
                    map_entries(hist_all_tokens_in_block_l),
                    entry -> list_contains(tokens_r, entry.key)
                )
            ) AS hist_overlapping_tokens_r_block_l,
            list_aggregate(bigrams_in_block_l, 'histogram')
                AS hist_all_bigrams_in_block_l,
            list_transform(
                list_zip(
                    list_slice(tokens_r, 1, length(tokens_r) - 1),
                    list_slice(tokens_r, 2, length(tokens_r))
                ),
                pair -> ARRAY[pair[1], pair[2]]
            ) AS bigrams_r
        FROM block_histograms
    """).create("block_statistics")

    con.sql(f"""
        WITH intermediate AS (
            SELECT
                candidate.match_weight,
                candidate.match_probability,
                candidate.unique_id_l,
                candidate.unique_id_r,
                candidate.original_address_concat_l,
                candidate.original_address_concat_r,
                candidate.ukam_address_id_l,
                candidate.ukam_address_id_r,
                candidate.postcode_l,
                candidate.postcode_r,
                concat_ws(' ', candidate.__token_address_l, candidate.postcode_l)
                    .trim()
                    .upper()
                    .regexp_split_to_array('\\s+') AS tokens_l,
                statistics.tokens_r,
                statistics.hist_all_tokens_in_block_l,
                statistics.hist_overlapping_tokens_r_block_l,
                statistics.hist_all_bigrams_in_block_l,
                statistics.bigrams_r,
                {retained_columns}
                list_distinct(
                    list_filter(
                        concat_ws(' ', candidate.__token_address_l, candidate.postcode_l)
                            .trim()
                            .upper()
                            .regexp_split_to_array('\\s+'),
                            token -> token IN {_POSITIONAL_TOKENS_SQL}
                    )
                ) AS positional_tokens_l,
                list_distinct(
                    list_filter(
                        statistics.tokens_r,
                            token -> token IN {_POSITIONAL_TOKENS_SQL}
                    )
                ) AS positional_tokens_r
            FROM token_addresses AS candidate
            LEFT JOIN block_statistics AS statistics USING (ukam_address_id_r)
        ),
        candidate_features AS (
            SELECT
                *,
                map_from_entries(
                    list_filter(
                        map_entries(hist_overlapping_tokens_r_block_l),
                        entry -> list_contains(tokens_l, entry.key)
                    )
                ) AS overlapping_tokens_this_l_and_r,
                map_from_entries(list_distinct(list_transform(
                    list_filter(tokens_r, token -> token NOT IN tokens_l),
                    token -> {{'key': token, 'value': true}}
                ))) AS tokens_r_not_in_l_map,
                list_filter(tokens_l, token -> token NOT IN tokens_r) AS missing_tokens,
                list_transform(
                    list_zip(
                        list_slice(tokens_l, 1, length(tokens_l) - 1),
                        list_slice(tokens_l, 2, length(tokens_l))
                    ),
                    pair -> ARRAY[pair[1], pair[2]]
                ) AS bigrams_l
            FROM intermediate
        ),
        evidence AS (
            SELECT
                *,
                map_from_entries(
                    list_filter(
                        map_entries(hist_all_tokens_in_block_l),
                        entry -> map_contains(tokens_r_not_in_l_map, entry.key)
                    )
                ) AS tokens_elsewhere_in_block_but_not_this,
                map_from_entries(list_distinct(list_transform(
                    list_filter(bigrams_r, bigram -> bigram NOT IN bigrams_l),
                    bigram -> {{'key': bigram, 'value': true}}
                ))) AS bigrams_r_not_in_l_map,
                map_from_entries(
                    list_filter(
                        map_entries(hist_overlapping_tokens_r_block_l),
                        entry -> list_contains(tokens_l, entry.key)
                    )
                ) AS overlapping_tokens_this_l_and_r_again
            FROM candidate_features
        ),
        adjusted_evidence AS (
            SELECT
                *,
                overlapping_tokens_this_l_and_r_again
                    AS overlapping_tokens_this_l_and_r,
                map_from_entries(
                    list_filter(
                        map_entries(hist_all_bigrams_in_block_l),
                        entry -> list_contains(bigrams_r, entry.key)
                    )
                ) AS hist_overlapping_bigrams_r_block_l,
                map_from_entries(
                    list_filter(
                        map_entries(hist_all_bigrams_in_block_l),
                        entry -> map_contains(bigrams_r_not_in_l_map, entry.key)
                    )
                ) AS bigrams_elsewhere_in_block_but_not_this
            FROM evidence
        ),
        components AS (
            SELECT
                *,
                map_from_entries(
                    list_filter(
                        map_entries(hist_overlapping_bigrams_r_block_l),
                        entry -> list_contains(bigrams_l, entry.key)
                    )
                ) AS overlapping_bigrams_this_l_and_r,
                COALESCE(list_sum(list_transform(
                    map_values(overlapping_tokens_this_l_and_r),
                    value -> 1.0 / (value * value)
                )), 0.0) * {REWARD_MULTIPLIER} AS token_reward,
                COALESCE(len(map_entries(tokens_elsewhere_in_block_but_not_this)), 0)::DOUBLE
                    * {PUNISHMENT_MULTIPLIER} AS token_absence_penalty,
                COALESCE(len(missing_tokens), 0)::DOUBLE * {MISSING_TOKEN_PENALTY}
                    AS missing_token_penalty,
                CASE
                    WHEN COALESCE(len(positional_tokens_l), 0) > 0
                        AND COALESCE(len(positional_tokens_r), 0) > 0
                        AND COALESCE(
                            len(list_intersect(positional_tokens_l, positional_tokens_r)),
                            0
                        ) = 0
                    THEN {POSITIONAL_CONFLICT_PENALTY}
                    ELSE 0.0
                END AS positional_conflict_penalty
            FROM adjusted_evidence
        ),
        scored_components AS (
            SELECT
                *,
                map_from_entries(
                    list_filter(
                        map_entries(overlapping_bigrams_this_l_and_r),
                        entry -> NOT (
                            map_contains(overlapping_tokens_this_l_and_r, entry.key[1])
                            AND overlapping_tokens_this_l_and_r[entry.key[1]] <= entry.value
                            AND map_contains(overlapping_tokens_this_l_and_r, entry.key[2])
                            AND overlapping_tokens_this_l_and_r[entry.key[2]] <= entry.value
                        )
                    )
                ) AS overlapping_bigrams_this_l_and_r_filtered,
                map_from_entries(
                    list_filter(
                        map_entries(bigrams_elsewhere_in_block_but_not_this),
                        entry -> NOT (
                            map_contains(tokens_elsewhere_in_block_but_not_this, entry.key[1])
                            AND tokens_elsewhere_in_block_but_not_this[entry.key[1]] <= entry.value
                            AND map_contains(tokens_elsewhere_in_block_but_not_this, entry.key[2])
                            AND tokens_elsewhere_in_block_but_not_this[entry.key[2]] <= entry.value
                        )
                    )
                ) AS bigrams_elsewhere_in_block_but_not_this_filtered
            FROM components
        ),
        scored_candidates AS (
            SELECT
                *,
                COALESCE(list_sum(list_transform(
                    map_values(overlapping_bigrams_this_l_and_r_filtered),
                    value -> 1.0 / (value * value)
                )), 0.0) * {BIGRAM_REWARD_MULTIPLIER} AS bigram_reward,
                COALESCE(len(map_entries(bigrams_elsewhere_in_block_but_not_this_filtered)), 0)::DOUBLE
                    * {BIGRAM_PUNISHMENT_MULTIPLIER} AS bigram_absence_penalty
            FROM scored_components
        )
        SELECT
            unique_id_l,
            unique_id_r,
            ukam_address_id_r,
            ukam_address_id_l,
            match_weight AS match_weight_original,
            token_reward,
            token_absence_penalty,
            bigram_reward,
            bigram_absence_penalty,
            missing_token_penalty,
            positional_conflict_penalty,
            token_reward
                - token_absence_penalty
                + bigram_reward
                - bigram_absence_penalty
                - missing_token_penalty
                - positional_conflict_penalty AS mw_adjustment,
            match_weight + mw_adjustment AS match_weight,
            overlapping_tokens_this_l_and_r,
            tokens_elsewhere_in_block_but_not_this,
            hist_all_tokens_in_block_l,
            hist_overlapping_tokens_r_block_l,
            missing_tokens,
            positional_tokens_l,
            positional_tokens_r,
            {"bigrams_l, bigrams_r, overlapping_bigrams_this_l_and_r, " if use_bigrams else ""}
            {"bigrams_elsewhere_in_block_but_not_this, hist_all_bigrams_in_block_l, " if use_bigrams else ""}
            {"hist_overlapping_bigrams_r_block_l, overlapping_bigrams_this_l_and_r_filtered, " if use_bigrams else ""}
            {"bigrams_elsewhere_in_block_but_not_this_filtered, " if use_bigrams else ""}
            original_address_concat_l,
            postcode_l,
            original_address_concat_r,
            postcode_r,
            {retained_columns}
        FROM scored_candidates
    """).create(matches_table)

    return con.table(matches_table)
