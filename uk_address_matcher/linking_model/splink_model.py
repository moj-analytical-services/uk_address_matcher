import importlib.resources as pkg_resources
import json
import logging
from contextlib import contextmanager

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from splink import DuckDBAPI, Linker, SettingsCreator

from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql

_SPLINK_SETTINGS_LOGGER = "splink.internals.settings"


def _get_model_settings_dict():
    with (
        pkg_resources.files("uk_address_matcher.data")
        .joinpath("splink_model.json")
        .open("r") as f
    ):
        return json.load(f)


def _sanitise_null_comparison_levels(settings_as_dict: dict) -> dict:
    """Normalise null comparison levels for Splink compatibility.

    Some Splink versions raise when serialising comparison levels flagged with
    `is_null_level` because null levels have no m-probability.

    Our bundled JSON settings include explicit "... IS NULL" levels. Some of
    those have `is_null_level: true` but also include `m_probability`/`u_probability`.

    In Splink, null levels must not have m/u probabilities (their weight is
    defined as neutral for that comparison). If m/u values are present, Splink
    can raise during settings normalisation.

    We keep `is_null_level` (to avoid Splink "No null level found" warnings),
    but strip any m/u probabilities from null levels.
    """

    comparisons = settings_as_dict.get("comparisons", [])
    if not isinstance(comparisons, list):
        return settings_as_dict

    for comparison in comparisons:
        if not isinstance(comparison, dict):
            continue
        levels = comparison.get("comparison_levels")
        if not isinstance(levels, list):
            continue
        for level in levels:
            if not isinstance(level, dict):
                continue
            if not level.get("is_null_level"):
                continue
            level.pop("m_probability", None)
            level.pop("u_probability", None)

    return settings_as_dict


@contextmanager
def _suppress_known_splink_warnings():
    logger = logging.getLogger(_SPLINK_SETTINGS_LOGGER)
    original_level = logger.level
    logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        logger.setLevel(original_level)


def _get_precomputed_numeric_tf_table(con: DuckDBPyConnection):
    read_tf_sql = package_resource_read_sql(
        "uk_address_matcher.data", "numeric_token_frequencies.parquet"
    )
    return con.sql(read_tf_sql)


def _get_linker(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    *,
    con: DuckDBPyConnection,
    additional_columns_to_retain: list[str] | None = None,
    include_full_postcode_block=False,
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
            "Input datasets contain a 'source_dataset' column. "
            "This column should be removed "
            "before calling _get_linker as it will be overwritten by the linker."
        )

    # Skim off any matches that we have already labelled as exact matches
    # Neither match_reason or resolved_canonical_id are needed for Splink processing
    if "resolved_canonical_id" in df_addresses_to_match.columns:
        excluded_columns = [
            col
            for col in [
                "match_reason",
                "resolved_canonical_id",
                "canonical_ukam_address_id",
                "original_address_concat_canonical",
                "postcode_canonical",
            ]
            if col in df_addresses_to_match.columns
        ]
        exclude_sql = ", ".join(excluded_columns)
        df_addresses_to_match = df_addresses_to_match.filter(
            "resolved_canonical_id IS NULL"
        ).select(f"* EXCLUDE({exclude_sql})")
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

    settings_as_dict = _sanitise_null_comparison_levels(settings_as_dict)

    if additional_columns_to_retain:
        settings_as_dict.setdefault("additional_columns_to_retain", [])
        settings_as_dict["additional_columns_to_retain"] += additional_columns_to_retain

    # Use ukam_address_id as unique_id column name
    # (created as part of our cleaning process).
    settings_as_dict["unique_id_column_name"] = "ukam_address_id"
    # Also make sure we now retain unique_id from both datasets...

    settings_as_dict["additional_columns_to_retain"] += [
        "unique_id",
        "original_address_concat",
    ]

    # Auto-detect ukam_label: if present in messy data, retain it for accuracy testing.
    if "ukam_label" in df_addresses_to_match.columns:
        settings_as_dict["additional_columns_to_retain"].append("ukam_label")
        if "ukam_label" not in df_addresses_to_search_within.columns:
            df_addresses_to_search_within = df_addresses_to_search_within.select(
                "*, NULL::VARCHAR AS ukam_label"
            )

    settings_as_dict["retain_intermediate_calculation_columns"] = (
        retain_intermediate_calculation_columns
    )
    settings_as_dict["retain_matching_columns"] = retain_matching_columns
    brs = settings_as_dict["blocking_rules_to_generate_predictions"]

    # Check if both blocking rule settings are False
    if not include_full_postcode_block and not include_outside_postcode_block:
        raise ValueError(
            "At least one of 'include_full_postcode_block' or "
            "'include_outside_postcode_block' "
            "must be True. Cannot proceed without any blocking rules."
        )

    if not include_full_postcode_block:
        brs = [br for br in brs if br["blocking_rule"] != 'l."postcode" = r."postcode"']

    if not include_outside_postcode_block:
        brs = [{"blocking_rule": "l.postcode = r.postcode"}]

    settings_as_dict["blocking_rules_to_generate_predictions"] = brs

    settings = SettingsCreator.from_path_or_dict(settings_as_dict)

    db_api = DuckDBAPI(connection=con)

    df_addresses_to_match_fix = df_addresses_to_match

    # See https://github.com/moj-analytical-services/uk_address_matcher/issues/253
    # con.register("df_addresses_to_search_within_fix", df_addresses_to_search_within)
    # df_addresses_to_search_within_fix = con.table("df_addresses_to_search_within_fix")
    df_addresses_to_search_within_fix = df_addresses_to_search_within

    # Drop stale Splink views/tables from any prior linker on this connection.
    messy_name, canonical_name = (
        "m_",
        "c_",
    )

    for tbl in (messy_name, canonical_name):
        con.execute(f"DROP VIEW IF EXISTS {tbl}")
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    with _suppress_known_splink_warnings():
        linker = Linker(
            [df_addresses_to_match_fix, df_addresses_to_search_within_fix],
            settings=settings,
            db_api=db_api,
            input_table_aliases=[messy_name, canonical_name],
            set_up_basic_logging=False,
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

    cols_to_select = df_addresses_to_match.columns
    select_expr = ", ".join(cols_to_select)
    messy_subquery = df_addresses_to_match_fix.sql_query()
    canonical_subquery = df_addresses_to_search_within_fix.sql_query()

    sql = f"""
    select {select_expr}, 'm_' as source_dataset
    from ({messy_subquery}) as df_addresses_to_match_fix
    UNION ALL
    select {select_expr}, 'c_' as source_dataset
    from ({canonical_subquery}) as df_addresses_to_search_within_fix

    """

    concat_with_tf = con.sql(sql)
    linker.table_management.register_table_input_nodes_concat_with_tf(concat_with_tf)

    return linker
