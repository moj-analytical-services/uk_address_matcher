from duckdb import DuckDBPyConnection, DuckDBPyRelation

# Sub-premise positional descriptors (e.g. "FLAT LEFT" vs "FLAT RIGHT"). These survive
# cleaning into clean_full_address, so they appear as tokens here. They are decisive for
# discriminating otherwise-identical sibling candidates, so the reranker treats a
# conflict on them as a structured penalty rather than a generic distinguishing token.
_POSITIONAL_TOKENS_SQL = "('LEFT', 'RIGHT', 'CENTRE', 'FRONT', 'REAR')"


def improve_predictions_using_distinguishing_tokens(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    match_weight_threshold: float = -20,
    top_n_matches: int = 5,
    use_bigrams: bool = True,
    additional_columns_to_retain: list[str] | None = None,
    REWARD_MULTIPLIER=3,
    PUNISHMENT_MULTIPLIER=1.5,
    BIGRAM_REWARD_MULTIPLIER=3,
    BIGRAM_PUNISHMENT_MULTIPLIER=1.5,
    MISSING_TOKEN_PENALTY=0.1,
    POSITIONAL_CONFLICT_PENALTY=6.0,
    IDENTITY_RESIDUE_BONUS_BITS=0.0,
    RAW_NO_DIGIT_BIGRAM_REWARD_CAP: float | None = None,
    RAW_NO_DIGIT_RARE_MISSING_POSITIVE_CAP: float | None = None,
    RAW_NO_DIGIT_ROLE_CONFLICT_POSITIVE_CAP: float | None = None,
    SUB_PREMISE_CONFLICT_POSITIVE_CAP: float | None = None,
    SUB_PREMISE_PARTIAL_POSITIVE_CAP: float | None = None,
    GUARD_RAW_NO_DIGIT_RARE_IDENTITY=False,
    GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_MATCH_WEIGHT=8.0,
    GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_UPLIFT=6.0,
):
    """
    Improve match predictions by identifying distinguishing tokens between addresses.

    Args:
        df_predict: DuckDB relation containing the prediction data
        con: DuckDB connection
        match_weight_threshold: Minimum match weight to consider
        top_n_matches: Number of top matches to consider for each unique_id_r
        use_bigrams: Whether to use bigram-based matching (default: True)

    Returns:
        DuckDBPyRelation: Table with improved match predictions
    """
    _distinguishing_token_matches_table = "__ukam__distinguishability_matches"

    add_cols_select = ""
    if additional_columns_to_retain:
        for col in additional_columns_to_retain:
            add_cols_select += f"{col}_l, {col}_r, "

    if "ukam_label_r" in df_predict.columns:
        add_cols_select += "ukam_label_r, "

    flat_identity_l_expr = (
        "flat_identity_l" if "flat_identity_l" in df_predict.columns else "NULL::VARCHAR"
    )
    flat_identity_r_expr = (
        "flat_identity_r" if "flat_identity_r" in df_predict.columns else "NULL::VARCHAR"
    )
    sub_premise_location_l_expr = (
        "sub_premise_location_l"
        if "sub_premise_location_l" in df_predict.columns
        else "NULL::VARCHAR"
    )
    sub_premise_location_r_expr = (
        "sub_premise_location_r"
        if "sub_premise_location_r" in df_predict.columns
        else "NULL::VARCHAR"
    )
    has_flat_indicator_l_expr = (
        "has_flat_indicator_l"
        if "has_flat_indicator_l" in df_predict.columns
        else "NULL::BOOLEAN"
    )
    has_flat_indicator_r_expr = (
        "has_flat_indicator_r"
        if "has_flat_indicator_r" in df_predict.columns
        else "NULL::BOOLEAN"
    )

    # Split the large SQL query into separate CTE steps

    # Step 1: Create good_matches CTE
    sql_good_matches = f"""
    SELECT *
    FROM df_predict
    WHERE match_weight > {match_weight_threshold}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_id_r, unique_id_l
        ORDER BY match_weight DESC, ukam_address_id_r DESC, ukam_address_id_l DESC
    ) = 1
    """
    good_matches = con.sql(sql_good_matches)  # noqa: F841

    # Step 2: Create top_n_matches CTE
    sql_top_n_matches = f"""
    SELECT *
    FROM good_matches
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_id_r
        ORDER BY match_weight DESC, unique_id_l DESC
    ) <= {top_n_matches}  -- e.g., 5 for top 5 matches
    """
    top_n_matches = con.sql(sql_top_n_matches)

    # Step 3: Create remove_common_end_tokens CTE
    # TODO(ThomasHepworth): Refine this code when we have time.
    sql_remove_common_end_tokens = """
    WITH intermediate AS (
        SELECT
            *,
            map_keys(common_end_tokens_hist_r) AS common_end_tokens_r
        FROM top_n_matches
    ),
    -- If we have a column of NULL values, common_end_tokens_hist can often be of the
    -- wrong type and will cause errors.
    enriched AS (
        SELECT
            *,
            COALESCE(
                common_end_tokens_r.list_transform(
                    x -> COALESCE(
                        struct_extract(
                            TRY_CAST(x AS STRUCT(tok VARCHAR, rel_freq DOUBLE)),
                            'tok'
                        ),
                        TRY_CAST(x AS VARCHAR)
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
            .regexp_split_to_array('\\s+') AS clean_full_address_l_tokens,
        clean_full_address_r
            .trim()
            .upper()
            .regexp_split_to_array('\\s+') AS clean_full_address_r_tokens,
        clean_full_address_l
            .trim()
            .upper()
            .regexp_split_to_array('\\s+')
            .list_reverse()
            .list_filter((tok, i) -> not (
                i = 1 and common_end_tokens_tok.list_contains(tok)
            ))
            .list_filter((tok, i) -> not (
                i = 1 and common_end_tokens_tok.list_contains(tok)
            ))
            .list_reverse()
            .array_to_string(' ')
            AS __token_address_l,

        clean_full_address_r
            .trim()
            .upper()
            .regexp_split_to_array('\\s+')
            .list_reverse()
            .list_filter((tok, i) -> not (
                i = 1 and common_end_tokens_tok.list_contains(tok)
            ))
            .list_filter((tok, i) -> not (
                i = 1 and common_end_tokens_tok.list_contains(tok)
            ))
            .list_reverse()
            .array_to_string(' ')
                AS __token_address_r

    FROM enriched

            """
    remove_common_end_tokens = con.sql(sql_remove_common_end_tokens)  # noqa: F841

    # Step 4: Create tokenise_r CTE
    sql_tokenise_r = """
    SELECT DISTINCT
        ukam_address_id_r,

        __token_address_r
            .trim()
            .upper()
            .regexp_split_to_array('\\s+')
            as identity_tokens_r,

        concat_ws(' ', __token_address_r, postcode_r)
            .trim()
            .upper()
            .regexp_split_to_array('\\s+')
            as tokens_r
    FROM remove_common_end_tokens
    """
    tokenise_r = con.sql(sql_tokenise_r)  # noqa: F841

    # Step 5: Create tokens CTE
    sql_tokens = f"""
    SELECT
        t.ukam_address_id_r,
        t.tokens_r,
        t.identity_tokens_r,

        -----------------
        -- TOKENS SECTION
        -----------------

        __token_address_l
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .array_agg()
                .flatten()
                as identity_tokens_in_block_l,

        list_aggregate(identity_tokens_in_block_l, 'histogram')
            AS hist_all_identity_tokens_in_block_l,

        map_from_entries(
                list_filter(
                    map_entries(hist_all_identity_tokens_in_block_l),
                    x -> list_contains(identity_tokens_r, x.key)
                )
            ) AS hist_overlapping_identity_tokens_r_block_l,

        concat_ws(' ', __token_address_l, postcode_l)
                .trim()
                .upper()
                .regexp_split_to_array('\\s+')
                .array_agg()
                .flatten()
                as tokens_in_block_l,

        -- Counts of tokens in canonical addresses within block
        list_aggregate(tokens_in_block_l, 'histogram') AS hist_all_tokens_in_block_l,

        -- Filter to only include tokens that appear in both r and the block
        map_from_entries(
                list_filter(
                    map_entries(hist_all_tokens_in_block_l),
                    x -> list_contains(tokens_r, x.key)
                )
            ) AS hist_overlapping_tokens_r_block_l,

        {
        '''
        -----------------
        -- BIGRAMS SECTION
        -----------------

        -- Create bigrams from all tokens in block
        list_transform(
            list_zip(
                list_slice(tokens_in_block_l, 1, length(tokens_in_block_l) - 1),
                list_slice(tokens_in_block_l, 2, length(tokens_in_block_l))
            ),
            tup -> ARRAY[tup[1], tup[2]]
        ) AS bigrams_in_block_l,

        -- Counts of bigrams in canonical addresses within block
        list_aggregate(bigrams_in_block_l, 'histogram') AS hist_all_bigrams_in_block_l,

        -- Create bigrams from tokens_r
        list_transform(
            list_zip(
                list_slice(tokens_r, 1, length(tokens_r) - 1),
                list_slice(tokens_r, 2, length(tokens_r))
            ),
            tup -> ARRAY[tup[1], tup[2]]
        ) AS bigrams_r,

        -- Filter to only include bigrams that appear in both r and the block
        map_from_entries(
            list_filter(
                map_entries(hist_all_bigrams_in_block_l),
                x -> list_contains(bigrams_r, x.key)
            )
        ) AS hist_overlapping_bigrams_r_block_l
        '''
        if use_bigrams
        else ""
    }

    FROM remove_common_end_tokens m
    JOIN tokenise_r t USING (ukam_address_id_r)
    GROUP BY t.ukam_address_id_r, t.tokens_r, t.identity_tokens_r
    """
    tokens = con.sql(sql_tokens)  # noqa: F841

    # Step 6: Create intermediate CTE
    sql_intermediate = f"""
    SELECT
        match_weight,
        match_probability,
        -- Raw unique IDs
        unique_id_l,
        m.unique_id_r,
        original_address_concat_l,
        original_address_concat_r,
        -- unique IDs generated by ukam cleaning process
        ukam_address_id_l,
        ukam_address_id_r,

        -----------------
        -- TOKENS SECTION
        -----------------

        __token_address_l
            .trim()
            .upper()
            .regexp_split_to_array('\\s+') AS identity_tokens_l,
        t.identity_tokens_r,

        concat_ws(' ', __token_address_l, postcode_l)
            .trim()
            .upper()
            .regexp_split_to_array('\\s+')
            -- .list_filter(tok -> tok NOT IN ('FLAT'))
            AS tokens_l,
        t.tokens_r,

        -- Filter out any tokens not in l block!
        map_from_entries(
            list_filter(
                map_entries(hist_overlapping_tokens_r_block_l),
                x -> list_contains(tokens_l, x.key)
            )
        ) AS overlapping_tokens_this_l_and_r,

        map_from_entries(
            list_filter(
                map_entries(hist_overlapping_identity_tokens_r_block_l),
                x -> list_contains(identity_tokens_l, x.key)
            )
        ) AS overlapping_identity_tokens_this_l_and_r,

        t.hist_all_tokens_in_block_l,
        t.hist_overlapping_tokens_r_block_l,

        --list_filter(t.tokens_r, tok -> tok NOT IN tokens_l) as tokens_r_not_in_l,

        map_from_entries(list_distinct(list_transform(
            list_filter(t.tokens_r, tok -> tok NOT IN tokens_l),
            tok -> {{"key": tok, 'value': true}}
        ))) as tokens_r_not_in_l_map,



        -- missing tokens are tokens in the canonical address but not in the messy address
        -- e.g. 'annex at'
        list_filter(tokens_l, t -> t NOT IN tokens_r) AS missing_tokens,

        -- Sub-premise positional descriptors present on each side (e.g. LEFT / REAR).
        -- Carried forward so the final scoring step can penalise a positional conflict.
        list_distinct(
            list_filter(tokens_l, tok -> tok IN {_POSITIONAL_TOKENS_SQL})
        ) AS positional_tokens_l,
        list_distinct(
            list_filter(t.tokens_r, tok -> tok IN {_POSITIONAL_TOKENS_SQL})
        ) AS positional_tokens_r,

        {
        '''
        -----------------
        -- BIGRAMS SECTION
        -----------------

        -- Create bigrams from tokens_l
        list_transform(
            list_zip(
                list_slice((tokens_l), 1,
                          length((tokens_l)) - 1),
                list_slice((tokens_l), 2,
                          length((tokens_l)))
            ),
            tup -> ARRAY[tup[1], tup[2]]
        ) AS bigrams_l,

        t.bigrams_r,

        -- Filter to only include bigrams that appear in both this l and r
        map_from_entries(
            list_filter(
                map_entries(hist_overlapping_bigrams_r_block_l),
                x -> list_contains(bigrams_l, x.key)
            )
        ) AS overlapping_bigrams_this_l_and_r,

        t.hist_all_bigrams_in_block_l,
        t.hist_overlapping_bigrams_r_block_l,

        -- Bigrams in r but not in this l
        list_filter(t.bigrams_r, bg -> bg NOT IN bigrams_l) as bigrams_r_not_in_l,

        map_from_entries(list_distinct(list_transform(
            list_filter(t.bigrams_r, bg -> bg NOT IN bigrams_l),
            bg -> {'key': bg, 'value': true}
        ))) as bigrams_r_not_in_l_map,
        '''
        if use_bigrams
        else ""
    }

        postcode_l,
        postcode_r,
        __token_address_l AS actual_clean_full_address_l_used_by_reranker,
        __token_address_r AS actual_clean_full_address_r_used_by_reranker,
        list_filter(clean_full_address_l_tokens, tok -> tok NOT IN identity_tokens_l)
            AS common_end_tokens_removed_l,
        list_filter(clean_full_address_r_tokens, tok -> tok NOT IN t.identity_tokens_r)
            AS common_end_tokens_removed_r,
            {flat_identity_l_expr} AS flat_identity_l,
            {flat_identity_r_expr} AS flat_identity_r,
            {sub_premise_location_l_expr} AS sub_premise_location_l,
            {sub_premise_location_r_expr} AS sub_premise_location_r,
            {has_flat_indicator_l_expr} AS has_flat_indicator_l,
            {has_flat_indicator_r_expr} AS has_flat_indicator_r,
        {add_cols_select}
    FROM remove_common_end_tokens m
    LEFT JOIN tokens t USING (ukam_address_id_r)
    """
    intermediate = con.sql(sql_intermediate)  # noqa: F841

    # Step 7: Final query
    sql_final = f"""
    SELECT
        map_from_entries(
            list_filter(
                map_entries(hist_all_tokens_in_block_l),
                x -> map_contains(tokens_r_not_in_l_map, x.key)
            )
        ) AS tokens_elsewhere_in_block_but_not_this,

     {
        '''
      -- Bigrams that appear elsewhere in the block but not in this l
        map_from_entries(
            list_filter(
                map_entries(hist_all_bigrams_in_block_l),
                x -> map_contains(bigrams_r_not_in_l_map, x.key)
            )
        ) AS bigrams_elsewhere_in_block_but_not_this,
        '''
        if use_bigrams
        else ""
    }
        unique_id_l,
        unique_id_r,
        match_weight,
        match_probability,
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,
        ukam_address_id_l,
        ukam_address_id_r,

        -----------------
        -- TOKENS SECTION
        -----------------

        overlapping_tokens_this_l_and_r,
        overlapping_identity_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        hist_overlapping_tokens_r_block_l,
        hist_all_tokens_in_block_l,
        missing_tokens,
        identity_tokens_l,
        identity_tokens_r,
        tokens_l,
        tokens_r,
        NOT regexp_matches(lower(coalesce(original_address_concat_r, '')), '[0-9]')
            AS source_no_digit_excluding_postcode,
        list_filter(
            list_filter(identity_tokens_r, tok -> tok NOT IN identity_tokens_l),
            tok -> coalesce(tokens_elsewhere_in_block_but_not_this[tok], 0) = 1
        ) AS rare_missing_identity_tokens,
        CASE
            WHEN len(identity_tokens_r) = 0 THEN 0.0
            ELSE len(map_keys(overlapping_identity_tokens_this_l_and_r))::DOUBLE
                / len(identity_tokens_r)
        END AS source_identity_recall,
        CASE
            WHEN len(identity_tokens_l) = 0 OR len(identity_tokens_r) = 0 THEN false
            WHEN len(list_intersect(identity_tokens_l, identity_tokens_r)) > 0 THEN false
            ELSE (
                len(
                    list_intersect(
                        identity_tokens_l,
                        ['FARM', 'FARMHOUSE', 'BARN', 'COTTAGE', 'BUNGALOW', 'CARAVAN', 'FLAT', 'ANNEXE', 'ANNEX', 'LODGE', 'HOUSE', 'HOTEL', 'SCHOOL', 'CHURCH', 'PRESBYTERY']
                    )
                ) > 0
                AND len(
                    list_intersect(
                        identity_tokens_r,
                        ['FARM', 'FARMHOUSE', 'BARN', 'COTTAGE', 'BUNGALOW', 'CARAVAN', 'FLAT', 'ANNEXE', 'ANNEX', 'LODGE', 'HOUSE', 'HOTEL', 'SCHOOL', 'CHURCH', 'PRESBYTERY']
                    )
                ) > 0
            )
        END AS role_conflict,
        actual_clean_full_address_l_used_by_reranker,
        actual_clean_full_address_r_used_by_reranker,
        common_end_tokens_removed_l,
        common_end_tokens_removed_r,
        positional_tokens_l,
        positional_tokens_r,
            flat_identity_l,
            flat_identity_r,
            sub_premise_location_l,
            sub_premise_location_r,
            has_flat_indicator_l,
            has_flat_indicator_r,

        {
        '''
        -----------------
        -- BIGRAMS SECTION
        -----------------
        -- Filter out from bigrams tokens already covered in tokens (unigrams) part

        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this,
        hist_overlapping_bigrams_r_block_l,
        hist_all_bigrams_in_block_l,

        overlapping_bigrams_this_l_and_r
        .map_entries()
        .list_filter(x ->
            NOT (
                (
                    map_contains(overlapping_tokens_this_l_and_r, x.key[1])
                    AND overlapping_tokens_this_l_and_r[x.key[1]] <= x.value
                )
                AND
                (
                    map_contains(overlapping_tokens_this_l_and_r, x.key[2])
                    AND overlapping_tokens_this_l_and_r[x.key[2]] <= x.value
                )
            )
        )
        .map_from_entries() AS overlapping_bigrams_this_l_and_r_filtered,


        bigrams_elsewhere_in_block_but_not_this
        .map_entries()
         .list_filter(x ->
            NOT (
                (
                    map_contains(tokens_elsewhere_in_block_but_not_this, x.key[1])
                    AND tokens_elsewhere_in_block_but_not_this[x.key[1]] <= x.value
                )
                AND
                (
                    map_contains(tokens_elsewhere_in_block_but_not_this, x.key[2])
                    AND tokens_elsewhere_in_block_but_not_this[x.key[2]] <= x.value
                )
            )
        )
        .map_from_entries() AS bigrams_elsewhere_in_block_but_not_this_filtered,
        '''
        if use_bigrams
        else ""
    }
    {add_cols_select}

    FROM intermediate
    ORDER BY ukam_address_id_r
    """

    windowed_tokens = con.sql(sql_final)  # noqa: F841

    # Calculate new match weights based on distinguishing tokens and bigrams

    sql = f"""
    CREATE OR REPLACE TABLE {_distinguishing_token_matches_table} AS

    WITH scored AS (
        SELECT
            *,
            CASE
                WHEN len(map_keys(overlapping_identity_tokens_this_l_and_r)) = 0 THEN NULL
                ELSE list_sort(map_keys(overlapping_identity_tokens_this_l_and_r))
                    .array_to_string('|')
            END AS identity_residue_key,
            len(map_keys(overlapping_identity_tokens_this_l_and_r))
                AS shared_identity_token_count,
            len(identity_tokens_r) AS source_identity_token_count
        FROM windowed_tokens
    ),
    scored_with_bonus AS (
        SELECT
            *,
            COUNT(*) OVER (
                PARTITION BY ukam_address_id_r, identity_residue_key
            ) AS candidate_surface_residue_count,
            CASE
                WHEN {IDENTITY_RESIDUE_BONUS_BITS} <= 0 THEN 0.0
                WHEN postcode_l IS DISTINCT FROM postcode_r THEN 0.0
                WHEN source_identity_token_count < 2 THEN 0.0
                WHEN shared_identity_token_count < 2 THEN 0.0
                WHEN identity_residue_key IS NULL THEN 0.0
                WHEN candidate_surface_residue_count != 1 THEN 0.0
                WHEN shared_identity_token_count::DOUBLE / source_identity_token_count < 0.95 THEN 0.0
                ELSE {IDENTITY_RESIDUE_BONUS_BITS}
            END AS identity_residue_bonus_bits
        FROM scored
    ),
    raw_adjustments AS (
        SELECT
            unique_id_l,
            unique_id_r,
            ukam_address_id_r,
            ukam_address_id_l,
            match_weight AS match_weight_original,
            ifnull(map_values(overlapping_tokens_this_l_and_r)
                .list_transform(x -> 1/(x^2))
                .list_sum() * {REWARD_MULTIPLIER}, 0) AS token_reward,
            ifnull(map_values(tokens_elsewhere_in_block_but_not_this)
                .list_transform(x -> 1)
                .list_sum() * {PUNISHMENT_MULTIPLIER}, 0) AS token_punishment,
            (len(missing_tokens) * {MISSING_TOKEN_PENALTY}) AS missing_token_penalty,
            (CASE
                WHEN len(positional_tokens_l) > 0
                    AND len(positional_tokens_r) > 0
                    AND len(list_intersect(positional_tokens_l, positional_tokens_r)) = 0
                THEN {POSITIONAL_CONFLICT_PENALTY}
                ELSE 0
              END) AS positional_conflict_penalty,
            {
        f'''
            ifnull(map_values(overlapping_bigrams_this_l_and_r_filtered)
                .list_transform(x -> 1/(x^2))
                .list_sum() * {BIGRAM_REWARD_MULTIPLIER}, 0) AS bigram_reward_raw,
            ifnull(map_values(bigrams_elsewhere_in_block_but_not_this_filtered)
                .list_transform(x -> 1)
                .list_sum() * {BIGRAM_PUNISHMENT_MULTIPLIER}, 0) AS bigram_punishment,
            '''
        if use_bigrams
        else '''
            0.0 AS bigram_reward_raw,
            0.0 AS bigram_punishment,
            '''
    }
            source_no_digit_excluding_postcode,
            rare_missing_identity_tokens,
            source_identity_recall,
            role_conflict,
            identity_residue_bonus_bits,
            identity_residue_key,
            candidate_surface_residue_count,
            shared_identity_token_count,
            source_identity_token_count,
            overlapping_tokens_this_l_and_r,
            overlapping_identity_tokens_this_l_and_r,
            tokens_elsewhere_in_block_but_not_this,
            missing_tokens,
            identity_tokens_l,
            identity_tokens_r,
            tokens_l,
            tokens_r,
            actual_clean_full_address_l_used_by_reranker,
            actual_clean_full_address_r_used_by_reranker,
            common_end_tokens_removed_l,
            common_end_tokens_removed_r,
            positional_tokens_l,
            positional_tokens_r,
            flat_identity_l,
            flat_identity_r,
            sub_premise_location_l,
            sub_premise_location_r,
            has_flat_indicator_l,
            has_flat_indicator_r,
            original_address_concat_l,
            postcode_l,
            original_address_concat_r,
            postcode_r,
            {
        '''
            overlapping_bigrams_this_l_and_r,
            bigrams_elsewhere_in_block_but_not_this,
            overlapping_bigrams_this_l_and_r_filtered,
            bigrams_elsewhere_in_block_but_not_this_filtered,
            '''
        if use_bigrams
        else ""
    }
            {add_cols_select}
        FROM scored_with_bonus
    ),
    promoted_adjustments AS (
        SELECT
            *,
            CASE
                WHEN source_no_digit_excluding_postcode
                    AND {
        "NULL"
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is None
        else str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
    } IS NOT NULL
                THEN least(
                    bigram_reward_raw,
                    {
        str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is not None
        else "0.0"
    }
                )
                ELSE bigram_reward_raw
            END AS bigram_reward,
            CASE
                WHEN source_no_digit_excluding_postcode
                    AND {
        "NULL"
        if RAW_NO_DIGIT_RARE_MISSING_POSITIVE_CAP is None
        else str(RAW_NO_DIGIT_RARE_MISSING_POSITIVE_CAP)
    } IS NOT NULL
                    AND len(rare_missing_identity_tokens) > 0
                THEN least(
                    (
                        token_reward - token_punishment - missing_token_penalty - positional_conflict_penalty
                        + CASE
                            WHEN source_no_digit_excluding_postcode
                                AND {
        "NULL"
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is None
        else str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
    } IS NOT NULL
                            THEN least(
                                bigram_reward_raw,
                                {
        str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is not None
        else "0.0"
    }
                            )
                            ELSE bigram_reward_raw
                          END
                        - bigram_punishment
                        + identity_residue_bonus_bits
                    ),
                    {
        str(RAW_NO_DIGIT_RARE_MISSING_POSITIVE_CAP)
        if RAW_NO_DIGIT_RARE_MISSING_POSITIVE_CAP is not None
        else "0.0"
    }
                )
                ELSE (
                    token_reward - token_punishment - missing_token_penalty - positional_conflict_penalty
                    + CASE
                        WHEN source_no_digit_excluding_postcode
                            AND {
        "NULL"
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is None
        else str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
    } IS NOT NULL
                        THEN least(
                            bigram_reward_raw,
                            {
        str(RAW_NO_DIGIT_BIGRAM_REWARD_CAP)
        if RAW_NO_DIGIT_BIGRAM_REWARD_CAP is not None
        else "0.0"
    }
                        )
                        ELSE bigram_reward_raw
                      END
                    - bigram_punishment
                    + identity_residue_bonus_bits
                )
            END AS mw_adjustment_after_rare_missing_cap
        FROM raw_adjustments
    ),
    final_scored AS (
        SELECT
            *,
            CASE
                WHEN source_no_digit_excluding_postcode
                    AND {
        "NULL"
        if RAW_NO_DIGIT_ROLE_CONFLICT_POSITIVE_CAP is None
        else str(RAW_NO_DIGIT_ROLE_CONFLICT_POSITIVE_CAP)
    } IS NOT NULL
                    AND role_conflict
                THEN least(
                    mw_adjustment_after_rare_missing_cap,
                    {
        str(RAW_NO_DIGIT_ROLE_CONFLICT_POSITIVE_CAP)
        if RAW_NO_DIGIT_ROLE_CONFLICT_POSITIVE_CAP is not None
        else "0.0"
    }
                )
                ELSE mw_adjustment_after_rare_missing_cap
            END AS mw_adjustment_after_role_conflict_cap,
            CASE
                WHEN flat_identity_l IS NOT NULL
                    AND flat_identity_r IS NOT NULL
                    AND flat_identity_l != flat_identity_r
                THEN true
                WHEN sub_premise_location_l IS NOT NULL
                    AND sub_premise_location_r IS NOT NULL
                    AND sub_premise_location_l != sub_premise_location_r
                THEN true
                ELSE false
            END AS structured_sub_premise_conflict,
            CASE
                WHEN flat_identity_l IS NULL
                    AND flat_identity_r IS NULL
                    AND sub_premise_location_l IS NULL
                    AND sub_premise_location_r IS NULL
                    AND coalesce(has_flat_indicator_l, false) = false
                    AND coalesce(has_flat_indicator_r, false) = false
                THEN false
                WHEN flat_identity_l IS NULL
                    OR flat_identity_r IS NULL
                    OR sub_premise_location_l IS NULL
                    OR sub_premise_location_r IS NULL
                    OR coalesce(has_flat_indicator_l, false)
                        != coalesce(has_flat_indicator_r, false)
                THEN true
                ELSE false
            END AS structured_sub_premise_partial
        FROM promoted_adjustments
    ),
    capped_structured_sub_premise AS (
        SELECT
            *,
            CASE
                WHEN structured_sub_premise_conflict
                    AND {
        "NULL"
        if SUB_PREMISE_CONFLICT_POSITIVE_CAP is None
        else str(SUB_PREMISE_CONFLICT_POSITIVE_CAP)
    } IS NOT NULL
                THEN least(
                    mw_adjustment_after_role_conflict_cap,
                    {
        str(SUB_PREMISE_CONFLICT_POSITIVE_CAP)
        if SUB_PREMISE_CONFLICT_POSITIVE_CAP is not None
        else "0.0"
    }
                )
                WHEN structured_sub_premise_partial
                    AND {
        "NULL"
        if SUB_PREMISE_PARTIAL_POSITIVE_CAP is None
        else str(SUB_PREMISE_PARTIAL_POSITIVE_CAP)
    } IS NOT NULL
                THEN least(
                    mw_adjustment_after_role_conflict_cap,
                    {
        str(SUB_PREMISE_PARTIAL_POSITIVE_CAP)
        if SUB_PREMISE_PARTIAL_POSITIVE_CAP is not None
        else "0.0"
    }
                )
                ELSE mw_adjustment_after_role_conflict_cap
            END AS mw_adjustment
        FROM final_scored
    )
    SELECT
        unique_id_l,
        unique_id_r,
        ukam_address_id_r,
        ukam_address_id_l,
        mw_adjustment,
        match_weight_original,
        token_reward,
        token_punishment,
        missing_token_penalty,
        positional_conflict_penalty,
        bigram_reward,
        bigram_punishment,
        identity_residue_bonus_bits,
        identity_residue_key,
        candidate_surface_residue_count,
        shared_identity_token_count,
        source_identity_token_count,
        CASE
            WHEN {str(bool(GUARD_RAW_NO_DIGIT_RARE_IDENTITY)).upper()}
                AND source_no_digit_excluding_postcode
                AND len(rare_missing_identity_tokens) > 0
                AND match_weight_original >= {
        GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_MATCH_WEIGHT
    }
                AND mw_adjustment >= {GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_UPLIFT}
            THEN 0.0
            ELSE mw_adjustment
        END AS mw_adjustment_effective,
        CASE
            WHEN {str(bool(GUARD_RAW_NO_DIGIT_RARE_IDENTITY)).upper()}
                AND source_no_digit_excluding_postcode
                AND len(rare_missing_identity_tokens) > 0
                AND match_weight_original >= {
        GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_MATCH_WEIGHT
    }
                AND mw_adjustment >= {GUARD_RAW_NO_DIGIT_RARE_IDENTITY_MIN_UPLIFT}
            THEN true
            ELSE false
        END AS raw_no_digit_rare_identity_guard_fired,
        (match_weight_original + mw_adjustment_effective) AS match_weight,
        overlapping_tokens_this_l_and_r,
        overlapping_identity_tokens_this_l_and_r,
        tokens_elsewhere_in_block_but_not_this,
        missing_tokens,
        identity_tokens_l,
        identity_tokens_r,
        tokens_l,
        tokens_r,
        source_no_digit_excluding_postcode,
        rare_missing_identity_tokens,
        actual_clean_full_address_l_used_by_reranker,
        actual_clean_full_address_r_used_by_reranker,
        common_end_tokens_removed_l,
        common_end_tokens_removed_r,
        positional_tokens_l,
        positional_tokens_r,
        source_identity_recall,
        role_conflict,
        flat_identity_l,
        flat_identity_r,
        sub_premise_location_l,
        sub_premise_location_r,
        has_flat_indicator_l,
        has_flat_indicator_r,
        structured_sub_premise_conflict,
        structured_sub_premise_partial,
        CASE
            WHEN structured_sub_premise_conflict
                AND mw_adjustment_after_role_conflict_cap > mw_adjustment
            THEN true
            ELSE false
        END AS structured_sub_premise_conflict_guard_fired,
        CASE
            WHEN structured_sub_premise_partial
                AND mw_adjustment_after_role_conflict_cap > mw_adjustment
            THEN true
            ELSE false
        END AS structured_sub_premise_partial_guard_fired,
        {
        '''
        overlapping_bigrams_this_l_and_r,
        bigrams_elsewhere_in_block_but_not_this,
        overlapping_bigrams_this_l_and_r_filtered,
        bigrams_elsewhere_in_block_but_not_this_filtered,
        '''
        if use_bigrams
        else ""
    }
        original_address_concat_l,
        postcode_l,
        original_address_concat_r,
        postcode_r,
        {add_cols_select}
    FROM capped_structured_sub_premise
    """

    con.execute(sql)
    matches = con.table(_distinguishing_token_matches_table)
    return matches
