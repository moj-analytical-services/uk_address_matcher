from importlib import resources
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.steps import (
    _add_term_frequencies_to_address_tokens,
    _add_term_frequencies_to_address_tokens_using_registered_df,
    _canonicalise_postcode,
    _clean_address_string_first_pass,
    _clean_address_string_second_pass,
    _derive_original_address_concat,
    _final_column_order,
    _first_unusual_token,
    _generalised_token_aliases,
    _get_token_frequeny_table,
    _move_common_end_tokens_to_field,
    _parse_out_flat_position_and_letter,
    _parse_out_numbers,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
    _separate_unusual_tokens,
    _split_numeric_tokens_to_cols,
    _tokenise_address_without_numbers,
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
    _use_first_unusual_token_if_no_numeric_token,
)
from uk_address_matcher.cleaning.steps.tokenisation import (
    _create_tokenised_address_concat,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import RunOptions, create_sql_pipeline

QUEUE_PRE_TF = [
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
    _canonicalise_postcode,
    _clean_address_string_first_pass,
    _derive_original_address_concat,
    _parse_out_flat_position_and_letter,
    _parse_out_numbers,
    _clean_address_string_second_pass,
    _split_numeric_tokens_to_cols,
    _create_tokenised_address_concat,
    _tokenise_address_without_numbers,
]

QUEUE_PRE_TF_WITH_UNIQUE_AND_COMMON = [
    *QUEUE_PRE_TF[: QUEUE_PRE_TF.index(_derive_original_address_concat) + 1],
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
    _generalised_token_aliases,
    *QUEUE_PRE_TF[QUEUE_PRE_TF.index(_derive_original_address_concat) + 1 :],
]

QUEUE_POST_TF = [
    _move_common_end_tokens_to_field,
    _first_unusual_token,
    _use_first_unusual_token_if_no_numeric_token,
    _separate_unusual_tokens,
    _final_column_order,
]


def _materialise_output_table(
    con: DuckDBPyConnection, rel: DuckDBPyRelation, uid: str
) -> DuckDBPyRelation:
    con.register("__address_table_res", rel)
    has_source_dataset = "source_dataset" in rel.columns
    exclude_clause = "EXCLUDE (source_dataset)" if has_source_dataset else ""
    materialised_name = f"__address_table_cleaned_{uid}"
    con.execute(
        f"""
        create or replace temporary table {materialised_name} as
        select * {exclude_clause} from __address_table_res
        """
    )
    return con.table(materialised_name)


def clean_data_with_minimal_steps(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    run_options: Optional[RunOptions] = None,
) -> DuckDBPyRelation:
    pipeline = create_sql_pipeline(
        con,
        address_table,
        QUEUE_PRE_TF,
        pipeline_name="Clean data with minimal steps",
        pipeline_description="A minimal cleaning pipeline without term frequencies",
    )
    table_rel = pipeline.run(run_options)
    return _materialise_output_table(con, table_rel, _uid())


def clean_data_on_the_fly(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    run_options: Optional[RunOptions] = None,
) -> DuckDBPyRelation:
    stage_queue = (
        QUEUE_PRE_TF + [_add_term_frequencies_to_address_tokens] + QUEUE_POST_TF
    )

    pipeline = create_sql_pipeline(
        con,
        address_table,
        stage_queue,
        pipeline_name="Clean data on the fly",
        pipeline_description=(
            "Clean address data using term frequencies computed "
            "on the fly from the input data"
        ),
    )
    table_rel = pipeline.run(run_options)
    return _materialise_output_table(con, table_rel, _uid())


def clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    rel_tok_freq_table: DuckDBPyRelation | None = None,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    run_options: Optional[RunOptions] = None,
) -> DuckDBPyRelation:
    if rel_tok_freq_table is None:
        default_tf_path = (
            resources.files("uk_address_matcher")
            / "data"
            / "address_token_frequencies.parquet"
        )
        rel_tok_freq_table = con.read_parquet(str(default_tf_path))

    con.register("rel_tok_freq", rel_tok_freq_table)

    pre_queue = (
        QUEUE_PRE_TF_WITH_UNIQUE_AND_COMMON
        if derive_distinguishing_wrt_adjacent_records
        else QUEUE_PRE_TF
    )

    stage_queue = (
        pre_queue
        + [_add_term_frequencies_to_address_tokens_using_registered_df]
        + QUEUE_POST_TF
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
    result_rel = pipeline.run(run_options)
    return _materialise_output_table(con, result_rel, _uid())


def get_numeric_term_frequencies_from_address_table(
    df_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    run_options: Optional[RunOptions] = None,
) -> DuckDBPyRelation:
    stage_queue = [
        _trim_whitespace_address_and_postcode,
        _upper_case_address_and_postcode,
        _clean_address_string_first_pass,
        _parse_out_flat_position_and_letter,
        _parse_out_numbers,
    ]

    pipeline = create_sql_pipeline(
        con,
        df_address_table,
        stage_queue,
        pipeline_name="Get numeric term frequencies",
        pipeline_description=(
            "Derive numeric tokens and compute frequency distribution"
        ),
    )
    numeric_tokens_rel = pipeline.run(run_options)
    numeric_tokens_rel.show()
    con.register("numeric_tokens_df", numeric_tokens_rel)

    sql = """
    with unnested as (
        select unnest(numeric_tokens) as numeric_token
        from numeric_tokens_df
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
    run_options: Optional[RunOptions] = None,
) -> DuckDBPyRelation:
    stage_queue = [
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

    pipeline = create_sql_pipeline(
        con,
        df_address_table,
        stage_queue,
        pipeline_name="Get address token frequencies",
        pipeline_description=("Tokenise addresses and compute frequency distribution"),
    )
    return pipeline.run(run_options)
