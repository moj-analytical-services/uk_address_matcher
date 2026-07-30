from __future__ import annotations

from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="add_term_frequencies_to_address_tokens",
    description="Compute token-level relative frequencies and attach them to each record",
    tags="term_frequency_analysis",
)
def _add_term_frequencies_to_address_tokens():
    """Compute token-level relative frequencies and attach them to each record."""

    base_sql = """
    SELECT * FROM {input}
    """

    rel_tok_freq_cte_sql = """
    SELECT
        token,
        count(*)::DOUBLE / sum(count(*)) OVER () AS rel_freq
    FROM (
        SELECT
            unnest(address_without_numbers_tokenised) AS token
        FROM {base}
    )
    GROUP BY token
    """

    # 1. Explode to rows - we only need ID, Token, and Index
    exploded_tokens_sql = """
    SELECT
        ukam_address_id,
        UNNEST(address_without_numbers_tokenised) AS token,
        GENERATE_SUBSCRIPTS(address_without_numbers_tokenised, 1) AS token_idx
    FROM {base}
    """

    # 2. Join to frequencies - we ONLY keep the Frequency (Float) and the Index (Int)
    # We drop the Token string here. It is dead weight for the sort.
    joined_scalars_sql = """
    SELECT
        e.ukam_address_id,
        e.token_idx,
        COALESCE(f.rel_freq, 5e-5) AS rel_freq
    FROM {exploded_tokens} e
    LEFT JOIN {rel_tok_freq_cte} f
        ON e.token = f.token
    """

    # 3. Aggregate the frequencies back into a per-address array.
    # A plain `list(rel_freq ORDER BY token_idx)` forces DuckDB to run an
    # ordered list aggregate, which sorts within every group during the
    # aggregation and dominates the cleaning plan. Instead we collect a single
    # `list(struct(idx, freq))` (one aggregate, so idx/freq stay aligned) and
    # sort it once with `list_sort` (token_idx is the leading struct field and
    # is unique within a group). This is ~3x faster with identical output.
    reaggregated_freqs_sql = """
    SELECT
        ukam_address_id,
        list_transform(
            list_sort(list(struct_pack(idx := token_idx, freq := rel_freq))),
            s -> s.freq
        ) AS freq_arr
    FROM {joined_scalars}
    GROUP BY ukam_address_id
    """

    # 4. Zip the sorted frequencies back to the ORIGINAL token list
    # This guarantees order (because the original list is the source of truth)
    # and constructs the Structs at the very last moment
    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        list_transform(
            list_zip(base.address_without_numbers_tokenised, agg.freq_arr),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {reaggregated_freqs} AS agg
        ON base.ukam_address_id = agg.ukam_address_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("rel_tok_freq_cte", rel_tok_freq_cte_sql),
        CTEStep("exploded_tokens", exploded_tokens_sql),
        CTEStep("joined_scalars", joined_scalars_sql),
        CTEStep("reaggregated_freqs", reaggregated_freqs_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="add_term_frequencies_to_address_tokens_using_registered_df",
    description=("Attach precomputed token frequencies"),
    tags="term_frequency_analysis",
)
def _add_term_frequencies_to_address_tokens_using_registered_df():
    """Attach precomputed token frequencies from ``__ukam__tmp_rel_tok_freq``."""

    base_sql = """
    SELECT * FROM {input}
    """

    # 1. Explode to rows - we only need ID, Token, and Index
    exploded_tokens_sql = """
    SELECT
        ukam_address_id,
        UNNEST(address_without_numbers_tokenised) AS token,
        GENERATE_SUBSCRIPTS(address_without_numbers_tokenised, 1) AS token_idx
    FROM {base}
    """

    # 2. Join to frequencies - we ONLY keep the Frequency (Float) and the Index (Int)
    # We drop the Token string here. It is dead weight for the sort.
    joined_scalars_sql = """
    SELECT
        e.ukam_address_id,
        e.token_idx,
        COALESCE(__ukam__tmp_rel_tok_freq.rel_freq, 5e-5) AS rel_freq
    FROM {exploded_tokens} e
    LEFT JOIN __ukam__tmp_rel_tok_freq
        ON e.token = __ukam__tmp_rel_tok_freq.token
    """

    # 3. Aggregate the frequencies back into a per-address array.
    # A plain `list(rel_freq ORDER BY token_idx)` forces DuckDB to run an
    # ordered list aggregate, which sorts within every group during the
    # aggregation and dominates the cleaning plan. Instead we collect a single
    # `list(struct(idx, freq))` (one aggregate, so idx/freq stay aligned) and
    # sort it once with `list_sort` (token_idx is the leading struct field and
    # is unique within a group). This is ~3x faster with identical output.
    reaggregated_freqs_sql = """
    SELECT
        ukam_address_id,
        list_transform(
            list_sort(list(struct_pack(idx := token_idx, freq := rel_freq))),
            s -> s.freq
        ) AS freq_arr
    FROM {joined_scalars}
    GROUP BY ukam_address_id
    """

    # 4. Zip the sorted frequencies back to the ORIGINAL token list
    # This guarantees order (because the original list is the source of truth)
    # and constructs the Structs at the very last moment
    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        list_transform(
            list_zip(base.address_without_numbers_tokenised, agg.freq_arr),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {reaggregated_freqs} AS agg
        ON base.ukam_address_id = agg.ukam_address_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("exploded_tokens", exploded_tokens_sql),
        CTEStep("joined_scalars", joined_scalars_sql),
        CTEStep("reaggregated_freqs", reaggregated_freqs_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="add_numeric_term_frequencies_using_registered_df",
    description="Attach numeric token term frequencies from registered lookup",
    tags="term_frequency_analysis",
)
def _add_numeric_term_frequencies_using_registered_df():
    """Attach numeric term frequency columns for numeric_token_1..3."""

    base_sql = """
    SELECT * FROM {input}
    """

    final_sql = """
    SELECT
        base.*,
        tf1.tf_numeric_token AS tf_numeric_token_1,
        tf2.tf_numeric_token AS tf_numeric_token_2,
        tf3.tf_numeric_token AS tf_numeric_token_3
    FROM {base} AS base
    LEFT JOIN __ukam__tmp_numeric_term_frequencies AS tf1
        ON base.numeric_token_1 = tf1.numeric_token
    LEFT JOIN __ukam__tmp_numeric_term_frequencies AS tf2
        ON base.numeric_token_2 = tf2.numeric_token
    LEFT JOIN __ukam__tmp_numeric_term_frequencies AS tf3
        ON base.numeric_token_3 = tf3.numeric_token
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="move_common_end_tokens_to_field",
    description=(
        "Move frequently occurring trailing tokens "
        "(e.g. counties) into a dedicated field and remove "
        "from token frequency array"
    ),
    tags="term_frequency_analysis",
)
def _move_common_end_tokens_to_field():
    """
    Move frequently occurring trailing tokens (e.g. counties) into a dedicated field and
    remove them from the token frequency array so missing endings aren't over-penalised.
    """

    base_sql = """
    SELECT * FROM {input}
    """

    read_common_tokens_sql = package_resource_read_sql(
        "uk_address_matcher.data", "common_end_tokens.csv"
    )
    common_end_tokens_sql = f"""
        select array_agg(token) as end_tokens_to_remove
        from ({read_common_tokens_sql})
        where token_count > 3000
        """

    joined_sql = """
    SELECT *
    FROM {base}
    CROSS JOIN {common_end_tokens}
    """

    end_tokens_included_sql = """
    SELECT
        * EXCLUDE (end_tokens_to_remove),
        list_filter(
            token_rel_freq_arr[-3:],
            x -> list_contains(end_tokens_to_remove, x.tok)
        ) AS common_end_tokens
    FROM {joined}
    """

    remove_end_tokens_expr = """
    list_filter(
        token_rel_freq_arr,
        (x, i) -> NOT (
            i > len(token_rel_freq_arr) - 2
            AND list_contains(list_transform(common_end_tokens, x -> x.tok), x.tok)
        )
    )
    """

    final_sql = f"""
    SELECT
        * EXCLUDE (token_rel_freq_arr),
        {remove_end_tokens_expr} AS token_rel_freq_arr
    FROM {{end_tokens_included}}
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("common_end_tokens", common_end_tokens_sql),
        CTEStep("joined", joined_sql),
        CTEStep("end_tokens_included", end_tokens_included_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="first_unusual_token",
    description="Attach the first token below the frequency threshold (0.001) if present",
    tags="term_frequency_analysis",
)
def _first_unusual_token():
    """Attach the first token below the frequency threshold (0.001) if present."""

    first_token_expr = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )

    sql = f"""
    SELECT
        {first_token_expr} AS first_unusual_token,
        *
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="use_first_unusual_token_if_no_numeric_token",
    description="Fallback to the unusual token when numeric_token_1 is missing",
    tags="term_frequency_analysis",
)
def _use_first_unusual_token_if_no_numeric_token():
    """Fallback to the unusual token when numeric_token_1 is missing."""

    sql = """
    SELECT
        * EXCLUDE (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        CASE
            WHEN {input}.numeric_token_1 IS NULL THEN first_unusual_token.tok
            ELSE {input}.numeric_token_1
        END AS numeric_token_1,
        CASE
            WHEN {input}.numeric_token_1 IS NULL THEN
                list_filter(
                    token_rel_freq_arr,
                    x -> coalesce(x.tok != first_unusual_token.tok, TRUE)
                )
            ELSE token_rel_freq_arr
        END AS token_rel_freq_arr
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="separate_unusual_tokens",
    description="Split token list into frequency bands for matching heuristics",
    tags="term_frequency_analysis",
)
def _separate_unusual_tokens():
    """Split token list into frequency bands for matching heuristics."""

    sql = """
    SELECT
        *,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-4 AND x.rel_freq >= 5e-5
            ),
            x -> x.tok
        ) AS unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 5e-5 AND x.rel_freq >= 1e-7
            ),
            x -> x.tok
        ) AS very_unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-7
            ),
            x -> x.tok
        ) AS extremely_unusual_tokens_arr
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="generalised_token_aliases",
    description="Apply token aliasing/normalisation over distinguishing tokens",
    tags="token_transformation",
)
def _generalised_token_aliases():
    """Apply token aliasing/normalisation over distinguishing tokens."""

    GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
        CASE
            WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
            WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
            WHEN token in ('BASEMENT') THEN ['LEVEL']
            ELSE [TOKEN]
        END
    """

    sql = f"""
    SELECT
        *,
        flatten(
            list_transform(distinguishing_adj_start_tokens, token ->
               {GENERALISED_TOKEN_ALIASES_CASE_STATEMENT}
            )
        ) AS distinguishing_adj_token_aliases
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="get_token_frequeny_table",
    description="Build a token frequency table from numeric and non-numeric tokens",
    tags="term_frequency_analysis",
)
def _get_token_frequeny_table():
    """Build a token frequency table from numeric and non-numeric tokens."""

    sql = """
    SELECT
        token_counts.token,
        token_counts.rel_freq
    FROM (
        SELECT
            token,
            COUNT(*) AS count,
            COUNT(*)::DOUBLE / (SELECT COUNT(*) FROM {unnested}) AS rel_freq
        FROM {unnested}
        GROUP BY token
    ) AS token_counts
    ORDER BY count DESC
    """

    concatenated_sql = """
    SELECT
        list_concat(
            array_filter(
                [numeric_token_1, numeric_token_2, numeric_token_3],
                x -> x IS NOT NULL
            ),
            address_without_numbers_tokenised
        ) AS all_tokens
    FROM {input}
    """

    unnested_sql = """
    SELECT unnest(all_tokens) AS token
    FROM {concatenated_tokens}
    """

    steps = [
        CTEStep("concatenated_tokens", concatenated_sql),
        CTEStep("unnested", unnested_sql),
        CTEStep("final", sql),
    ]

    return steps


@pipeline_stage(
    name="create_histograms_from_token_frequencies",
    description="Create histogram aggregates from token frequency arrays",
    tags="term_frequency_analysis",
)
def _create_histograms_from_token_frequencies():
    """Create histogram aggregates from token frequency arrays."""

    sql = """
    WITH histograms AS (
        SELECT
            *,
            list_aggregate(token_rel_freq_arr, 'histogram') AS token_rel_freq_arr_hist,
            list_aggregate(common_end_tokens, 'histogram') AS common_end_tokens_hist
        FROM {input}
    )
    SELECT
        * EXCLUDE (
            token_rel_freq_arr,
            common_end_tokens
        ),
        map_from_entries(
            list_transform(
                map_entries(token_rel_freq_arr_hist),
                entry -> struct_pack(
                    key := entry.key.tok,
                    value := struct_pack(
                        idf_q := CAST(
                            ROUND(-LOG10(entry.key.rel_freq) * 256)
                            AS USMALLINT
                        ),
                        token_count := CAST(entry.value AS UTINYINT)
                    )
                )
            )
        ) AS token_idf_q_hist
    FROM histograms
    """
    return sql
