import importlib.resources as pkg_resources
import json

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from splink import DuckDBAPI, Linker, SettingsCreator


def _get_model_settings_dict():
    with (
        pkg_resources.files("uk_address_matcher.data")
        .joinpath("splink_model.json")
        .open("r") as f
    ):
        return json.load(f)


def _get_precomputed_numeric_tf_table(con: DuckDBPyConnection):
    tf_path = pkg_resources.files("uk_address_matcher.data").joinpath(
        "numeric_token_frequencies.parquet"
    )
    return con.read_parquet(str(tf_path))


def get_linker(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    *,
    con: DuckDBPyConnection,
    additional_columns_to_retain: list[str] | None = None,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
    precomputed_numeric_tf_table: DuckDBPyRelation | None = None,
    retain_intermediate_calculation_columns=False,
    retain_matching_columns=True,
    settings: SettingsCreator | None = None,
) -> Linker:
    # Check if either input dataset contains a source_dataset column
    if (
        "source_dataset" in df_addresses_to_match.columns
        or "source_dataset" in df_addresses_to_search_within.columns
    ):
        raise ValueError(
            "Input datasets contain a 'source_dataset' column. This column should be removed "
            "before calling get_linker as it will be overwritten by the linker."
        )

    # Skim off any matches that we have already labelled as exact matches
    # Neither match_reason or resolved_canonical_id are needed for Splink processing
    if "resolved_canonical_id" in df_addresses_to_match.columns:
        df_addresses_to_match = df_addresses_to_match.filter(
            "resolved_canonical_id IS NULL"
        ).select(
            "* EXCLUDE(match_reason, resolved_canonical_id, canonical_ukam_address_id)"
        )
    unresolved_count = df_addresses_to_match.count("*").fetchall()[0][0]
    if unresolved_count == 0:
        raise ValueError(
            "No unresolved records remain after deterministic matching. Either "
            "skip Splink or provide rows with unresolved matches."
        )

    canonical_count = df_addresses_to_search_within.count("*").fetchall()[0][0]
    if canonical_count == 0:
        raise ValueError(
            "Canonical relation is empty - Splink requires at least one search record."
        )

    if settings is None:
        settings_as_dict = _get_model_settings_dict()
    else:
        settings_as_dict = settings.create_settings_dict("duckdb")

    if additional_columns_to_retain:
        settings_as_dict.setdefault("additional_columns_to_retain", [])
        settings_as_dict["additional_columns_to_retain"] += additional_columns_to_retain

    # Use ukam_address_id as unique_id column name (created as part of our cleaning process)
    settings_as_dict["unique_id_column_name"] = "ukam_address_id"
    # Also make sure we now retain unique_id from both datasets...
    settings_as_dict["additional_columns_to_retain"] += [
        "unique_id",
        "original_address_concat",
    ]

    settings_as_dict["retain_intermediate_calculation_columns"] = (
        retain_intermediate_calculation_columns
    )
    settings_as_dict["retain_matching_columns"] = retain_matching_columns
    brs = settings_as_dict["blocking_rules_to_generate_predictions"]

    # Check if both blocking rule settings are False
    if not include_full_postcode_block and not include_outside_postcode_block:
        raise ValueError(
            "At least one of 'include_full_postcode_block' or 'include_outside_postcode_block' "
            "must be True. Cannot proceed without any blocking rules."
        )

    if not include_full_postcode_block:
        brs = [br for br in brs if br["blocking_rule"] != 'l."postcode" = r."postcode"']

    if not include_outside_postcode_block:
        brs = [{"blocking_rule": "l.postcode = r.postcode"}]

    settings_as_dict["blocking_rules_to_generate_predictions"] = brs

    settings = SettingsCreator.from_path_or_dict(settings_as_dict)

    db_api = DuckDBAPI(connection=con)

    con.register("df_addresses_to_match_fix", df_addresses_to_match)
    con.register("df_addresses_to_search_within_fix", df_addresses_to_search_within)

    linker = Linker(
        [df_addresses_to_match, df_addresses_to_search_within],
        settings=settings,
        db_api=db_api,
        input_table_aliases=["m_", "c_"],
    )

    if precomputed_numeric_tf_table is None:
        precomputed_numeric_tf_table = _get_precomputed_numeric_tf_table(con)

    for i in range(1, 4):
        df_sql = f"""
            select
                numeric_token as numeric_token_{i},
                tf_numeric_token as tf_numeric_token_{i}
            from precomputed_numeric_tf_table"""

        df = con.sql(df_sql)
        linker.table_management.register_term_frequency_lookup(
            df, f"numeric_token_{i}", overwrite=True
        )

    return linker
