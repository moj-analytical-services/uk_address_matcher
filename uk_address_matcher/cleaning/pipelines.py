from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.steps import (
    _add_numeric_range_lower_endpoint_tf,
    _add_numeric_term_frequencies_using_registered_df,
    _add_term_frequencies_to_address_tokens_using_registered_df,
    _canonicalise_postcode,
    _clean_address_string_first_pass,
    _clean_address_string_second_pass,
    _derive_numeric_range,
    _extract_postcode_from_address,
    _first_unusual_token,
    _generalised_token_aliases,
    _get_token_frequeny_table,
    _join_excluding_with_next_token,
    _lookup_keys_in_inverted_index,
    _move_common_end_tokens_to_field,
    _normalise_abbreviations_and_units,
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _parse_out_numbers,
    _parse_out_sub_premise_location,
    _preserve_original_address_concat,
    _remove_duplicate_end_tokens,
    _rename_and_select_columns,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
    _separate_unusual_tokens,
    _set_exploding_unique_ids_to_self,
    _split_numeric_tokens_to_cols,
    _strip_country_suffix,
    _tokenise_address_without_numbers,
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
    _use_first_unusual_token_if_no_numeric_token,
)
from uk_address_matcher.cleaning.steps.term_frequencies import (
    _create_histograms_from_token_frequencies,
)
from uk_address_matcher.sql_pipeline.helpers import (
    _duckdb_table_exists,
    package_resource_read_sql,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, create_sql_pipeline

_NUMERIC_TOKENS_WORK_NAME = "__ukam__numeric_tokens_work"


def _ensure_postcode_column(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """Ensure the relation has a postcode column, adding NULL if missing.

    Handles case-insensitive matching (e.g., 'PostCode', 'POSTCODE', 'postcode').
    """
    # Check for postcode column case-insensitively
    columns_lower = [col.lower() for col in rel.columns]

    if "postcode" in columns_lower:
        # Find the actual column name (might be PostCode, POSTCODE, etc.)
        postcode_col = [col for col in rel.columns if col.lower() == "postcode"][0]

        if postcode_col != "postcode":
            # Rename to lowercase and ensure VARCHAR type
            return rel.select(
                f"* EXCLUDE ({postcode_col}), CAST({postcode_col} AS VARCHAR) AS postcode"
            )
        else:
            # Already lowercase, just ensure VARCHAR type
            return rel.select(
                "* EXCLUDE (postcode), CAST(postcode AS VARCHAR) AS postcode"
            )
    else:
        # No postcode column exists, add NULL
        return rel.select("*, CAST(NULL AS VARCHAR) AS postcode")


QUEUE_CLEAN_FULL_ADDRESS = [
    _preserve_original_address_concat,
    _extract_postcode_from_address,
    _rename_and_select_columns,
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
    _canonicalise_postcode,
    _clean_address_string_first_pass,
    _normalise_abbreviations_and_units,
    _join_excluding_with_next_token,
    _strip_country_suffix,
    _remove_duplicate_end_tokens,  # clean_full_address now completed
]


QUEUE_DERIVE_NON_TF_FEATURES = [
    _parse_out_flat_position_and_letter,
    _parse_out_sub_premise_location,
    _parse_out_business_unit,
    _parse_out_numbers,
    _derive_numeric_range,
    _clean_address_string_second_pass,
    _split_numeric_tokens_to_cols,
    _tokenise_address_without_numbers,
]


QUEUE_PRE_TF = [
    *QUEUE_CLEAN_FULL_ADDRESS,
    *QUEUE_DERIVE_NON_TF_FEATURES,
]


QUEUE_PRE_TF_WITH_DISTINGUISHING_WRT_ADJACENT = [
    *QUEUE_CLEAN_FULL_ADDRESS,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
    _generalised_token_aliases,
    *QUEUE_DERIVE_NON_TF_FEATURES,
]

QUEUE_POST_TF = [
    _move_common_end_tokens_to_field,
    _first_unusual_token,
    _use_first_unusual_token_if_no_numeric_token,
    _separate_unusual_tokens,
    _create_histograms_from_token_frequencies,
]

# Minimal pipeline for TF derivation: just clean address
QUEUE_FOR_TF_DERIVATION = QUEUE_CLEAN_FULL_ADDRESS

# Inverted index blocking pipelines
QUEUE_INVERTED_INDEX_LOOKUP = [
    _lookup_keys_in_inverted_index(),
]

QUEUE_INVERTED_INDEX_SELF = [
    _set_exploding_unique_ids_to_self,
]


def _clean_data_pre_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    # Ensure postcode column exists before pipeline entry
    address_table_with_postcode = _ensure_postcode_column(address_table)
    pipeline = create_sql_pipeline(
        con,
        input_rel=address_table_with_postcode,
        stage_specs=QUEUE_PRE_TF,
        pipeline_name="Clean data pre term frequencies",
        pipeline_description="The cleaning pipeline pre the term frequency steps",
    )
    result = pipeline.run(debug_options)

    exclude_columns = []
    if "source_dataset" in result.columns:
        exclude_columns.append("source_dataset")
    if "address_without_numbers" in result.columns:
        exclude_columns.append("address_without_numbers")

    if exclude_columns:
        exclude_sql = ", ".join(exclude_columns)
        result = con.sql(
            f"SELECT * EXCLUDE ({exclude_sql}) FROM ({result.sql_query()}) AS cleaned"
        )

    return result


def _clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    pre_cleaned_addresses: bool = False,
    additional_stages: list = [],
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    # Ensure postcode column exists before pipeline entry
    if not pre_cleaned_addresses:
        address_table = _ensure_postcode_column(address_table)
    pre_queue = (
        QUEUE_PRE_TF_WITH_DISTINGUISHING_WRT_ADJACENT
        if derive_distinguishing_wrt_adjacent_records
        else QUEUE_PRE_TF
    )

    tf_and_post = [
        _add_term_frequencies_to_address_tokens_using_registered_df,
        _add_numeric_term_frequencies_using_registered_df,
    ] + QUEUE_POST_TF
    if not pre_cleaned_addresses or "numeric_range" in address_table.columns:
        tf_and_post.insert(2, _add_numeric_range_lower_endpoint_tf)
    stage_queue = (
        pre_queue + tf_and_post + additional_stages
        if not pre_cleaned_addresses
        else tf_and_post + additional_stages
    )

    pipeline = create_sql_pipeline(
        con,
        address_table,
        stage_queue,
        pipeline_name="Clean data using precomputed term frequencies",
        pipeline_description=(
            "Clean address data using a supplied table of relative token frequencies"
        ),
    )
    result = pipeline.run(debug_options)

    exclude_columns = []
    if "source_dataset" in result.columns:
        exclude_columns.append("source_dataset")
    if "address_without_numbers" in result.columns:
        exclude_columns.append("address_without_numbers")

    if exclude_columns:
        exclude_sql = ", ".join(exclude_columns)
        result = con.sql(
            f"SELECT * EXCLUDE ({exclude_sql}) FROM ({result.sql_query()}) AS cleaned"
        )

    return result


def get_numeric_term_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    pre_cleaned_addresses: bool = False,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    stage_queue = [
        _rename_and_select_columns,
        _trim_whitespace_address_and_postcode,
        _upper_case_address_and_postcode,
        _clean_address_string_first_pass,
        _parse_out_flat_position_and_letter,
        _parse_out_numbers,
    ]

    if not pre_cleaned_addresses:
        pipeline = create_sql_pipeline(
            con,
            df_address_table,
            stage_queue,
            pipeline_name="Get numeric term frequencies",
            pipeline_description=(
                "Derive numeric tokens and compute frequency distribution"
            ),
        )
        numeric_tokens_rel = pipeline.run(debug_options)
        con.register(_NUMERIC_TOKENS_WORK_NAME, numeric_tokens_rel)
    else:
        con.register(_NUMERIC_TOKENS_WORK_NAME, df_address_table)

    sql = """
    with unnested as (
        select unnest(numeric_tokens) as numeric_token
        from ukam__numeric_tokens_work
    )
    select
        numeric_token,
        count(*)/(select count(*) from unnested) as tf_numeric_token
    from unnested
    group by numeric_token
    order by 2 desc
    """
    return con.sql(sql)


def get_address_token_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    pre_cleaned_addresses: bool = False,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    stage_queue = [
        _rename_and_select_columns,
        _trim_whitespace_address_and_postcode,
        _upper_case_address_and_postcode,
        _clean_address_string_first_pass,
        _parse_out_flat_position_and_letter,
        _parse_out_numbers,
        _clean_address_string_second_pass,
        _split_numeric_tokens_to_cols,
        _tokenise_address_without_numbers,
        _get_token_frequeny_table,
    ]
    if pre_cleaned_addresses:
        stage_queue = stage_queue[-1:]  # only need the last step if pre-cleaned

    pipeline = create_sql_pipeline(
        con,
        df_address_table,
        stage_queue,
        pipeline_name="Get address token frequencies",
        pipeline_description=("Tokenise addresses and compute frequency distribution"),
    )
    return pipeline.run(debug_options)


def _create_term_frequency_tables(
    con: DuckDBPyConnection,
    term_frequency_lookup: Optional[DuckDBPyRelation] = None,
) -> DuckDBPyRelation:
    """Register address token  and numeric term frequency tables.

        - Numeric term frequencies are always loaded from pre-baked data.
            'Numeric' means e.g. how often token '1' appears
            relative to token '7' in UK addresses.
        They are used implicitly in the linking model by loading them into
        linker.table_management.register_term_frequency_lookup
        - Token term frequencies are usually derived from the data itself,
            but if not provided by the user a pre-baked table is used.

    Args:
        con: DuckDB connection.
        term_frequency_lookup: Optional pre-computed term frequency table with
            'token' and 'rel_freq' columns. If not provided, uses the package's
            pre-baked term frequencies.

    Returns:
        The registered term frequency table.
    """
    # Use provided TOKEN term frequencies or load pre-baked
    if term_frequency_lookup is not None:
        address_token_frequencies_rel = term_frequency_lookup
    else:
        read_tf_sql = package_resource_read_sql(
            "uk_address_matcher.data", "address_token_frequencies.parquet"
        )
        address_token_frequencies_rel = con.sql(read_tf_sql)

    # Materialise the term frequency table to avoid lazy evaluation issues
    # when the underlying data is modified or dropped
    con.sql("DROP TABLE IF EXISTS __ukam__tmp_rel_tok_freq")
    address_token_frequencies_rel.create("__ukam__tmp_rel_tok_freq")

    # Always load pre-baked NUMERIC term frequencies (see docstring)
    read_numeric_tf_sql = package_resource_read_sql(
        "uk_address_matcher.data", "numeric_token_frequencies.parquet"
    )
    numeric_term_frequencies_rel = con.sql(read_numeric_tf_sql)
    con.sql("DROP TABLE IF EXISTS __ukam__tmp_numeric_term_frequencies")
    numeric_term_frequencies_rel.create("__ukam__tmp_numeric_term_frequencies")

    return con.table("__ukam__tmp_rel_tok_freq")


def _register_inverted_index_table(
    con: DuckDBPyConnection,
    inverted_index: Optional[DuckDBPyRelation] = None,
    inverted_index_n: Optional[int] = None,
) -> Optional[str]:
    """Register inverted index table for key lookups.

    Args:
        con: DuckDB connection.
        inverted_index: Pre-computed inverted index table with 'key',
            'unique_ids', and 'index_strategy' columns. If None, no
            table is registered.
        inverted_index_n: Number of canonical addresses the index was built
            over (``N`` for IDF weighting of signature evidence). When ``None``
            a fallback is derived from the distinct ids present in the index.

    Returns:
        The registered table name, or None if no inverted_index provided.
    """
    if inverted_index is None:
        return None

    existing_alias = getattr(inverted_index, "alias", None)
    if isinstance(existing_alias, str) and _duckdb_table_exists(con, existing_alias):
        con.sql("DROP VIEW IF EXISTS __ukam_inverted_index")
        con.execute(
            f"CREATE TEMP VIEW __ukam_inverted_index AS SELECT * FROM {existing_alias}"
        )
    else:
        # Materialise to avoid lazy evaluation issues
        con.sql("DROP TABLE IF EXISTS __ukam_inverted_index")
        con.sql(f"SELECT * FROM ({inverted_index.sql_query()})").create(
            "__ukam_inverted_index"
        )

    _register_inverted_index_meta(con, inverted_index_n)
    return "__ukam_inverted_index"


def _register_inverted_index_meta(
    con: DuckDBPyConnection,
    inverted_index_n: Optional[int],
) -> None:
    """Register ``__ukam_index_meta`` holding ``N`` for IDF weighting.

    ``N`` is the canonical address count the inverted index was built over.
    When not supplied (e.g. when ``prepare_data_for_matching`` is called
    directly in tests) it falls back to the distinct id count in the index.
    """
    if inverted_index_n is None:
        inverted_index_n = con.sql(
            "SELECT COUNT(DISTINCT u) "
            "FROM __ukam_inverted_index, unnest(unique_ids) AS t(u)"
        ).fetchone()[0]
    n_value = max(int(inverted_index_n or 0), 1)
    con.execute("DROP TABLE IF EXISTS __ukam_index_meta")
    con.execute(f"CREATE TABLE __ukam_index_meta AS SELECT {n_value}::BIGINT AS n")
