from __future__ import annotations

import importlib.resources as pkg_resources

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
        count(*) / sum(count(*)) OVER () AS rel_freq
    FROM (
        SELECT
            unnest(address_without_numbers_tokenised) AS token
        FROM {base}
    )
    GROUP BY token
    """

    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """

    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE({rel_tok_freq_cte}.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN {rel_tok_freq_cte}
        ON {addresses_exploded}.token = {rel_tok_freq_cte}.token
    """

    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """

    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("rel_tok_freq_cte", rel_tok_freq_cte_sql),
        CTEStep("addresses_exploded", addresses_exploded_sql),
        CTEStep("address_groups", address_groups_sql),
        CTEStep("token_freq_lookup", token_freq_lookup_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="add_term_frequencies_to_address_tokens_using_registered_df",
    description="Attach precomputed token frequencies from registered DataFrame rel_tok_freq",
    tags="term_frequency_analysis",
)
def _add_term_frequencies_to_address_tokens_using_registered_df():
    """Attach precomputed token frequencies registered as rel_tok_freq."""

    base_sql = """
    SELECT * FROM {input}
    """

    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """

    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE(rel_tok_freq.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN rel_tok_freq
        ON {addresses_exploded}.token = rel_tok_freq.token
    """

    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """

    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("addresses_exploded", addresses_exploded_sql),
        CTEStep("address_groups", address_groups_sql),
        CTEStep("token_freq_lookup", token_freq_lookup_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="move_common_end_tokens_to_field",
    description="Move frequently occurring trailing tokens (e.g. counties) into a dedicated field and remove from token frequency array",
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

    with pkg_resources.path(
        "uk_address_matcher.data", "common_end_tokens.csv"
    ) as csv_path:
        common_end_tokens_sql = f"""
            select array_agg(token) as end_tokens_to_remove
            from read_csv_auto('{csv_path}')
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
            COUNT(*) / (SELECT COUNT(*) FROM {unnested}) AS rel_freq
        FROM {unnested}
        GROUP BY token
    ) AS token_counts
    ORDER BY count DESC
    """

    concatenated_sql = """
    SELECT
        unique_id,
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
    name="attach_numeric_term_frequencies",
    description="Attach numeric term frequencies from __ukam_numeric_term_frequencies table",
    tags="term_frequency_analysis",
)
def _attach_numeric_term_frequencies(
    numeric_tf_table: str = "__ukam_numeric_term_frequencies",
):
    """Attach numeric TF columns from precomputed __ukam_numeric_term_frequencies table."""

    final_sql = f"""
    SELECT
        {{input}}.*,
        tf1.tf_numeric_token AS tf_numeric_token_1,
        tf2.tf_numeric_token AS tf_numeric_token_2,
        tf3.tf_numeric_token AS tf_numeric_token_3
    FROM {{input}}
    LEFT JOIN {numeric_tf_table} AS tf1
        ON {{input}}.numeric_token_1 = tf1.numeric_token
    LEFT JOIN {numeric_tf_table} AS tf2
        ON {{input}}.numeric_token_2 = tf2.numeric_token
    LEFT JOIN {numeric_tf_table} AS tf3
        ON {{input}}.numeric_token_3 = tf3.numeric_token
    """

    return final_sql


@pipeline_stage(
    name="create_histograms_from_token_frequencies",
    description="Create histogram aggregates from token frequency arrays",
    tags="term_frequency_analysis",
)
def _create_histograms_from_token_frequencies():
    """Create histogram aggregates from token frequency arrays."""

    sql = """
    SELECT
        * EXCLUDE (
            token_rel_freq_arr,
            common_end_tokens
        ),
        list_aggregate(token_rel_freq_arr, 'histogram') AS token_rel_freq_arr_hist,
        list_aggregate(common_end_tokens, 'histogram') AS common_end_tokens_hist
    FROM {input}
    """
    return sql
