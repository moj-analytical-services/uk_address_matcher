from __future__ import annotations

import importlib.resources as pkg_resources
import json

from duckdb import (
    ColumnExpression,
    ConstantExpression,
    DuckDBPyConnection,
    DuckDBPyRelation,
    FunctionExpression,
    InvalidInputException,
    SQLExpression,
    StarExpression,
)
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from uk_address_matcher.cleaning.rehydration.token_views import (
    _distinguishing_lexical_tokens_expression,
    _distinguishing_token_parts_view_expressions,
)
from uk_address_matcher.post_linkage.distinguishing_features.numeric_range import (
    ensure_numeric_range_struct,
)
from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql


def _get_model_settings_dict(*, include_directional_premise_roles: bool = False):
    """Load Splink settings, keeping the directional experiment opt-in."""
    with (
        pkg_resources.files("uk_address_matcher.data")
        .joinpath("splink_model.json")
        .open("r") as f
    ):
        settings = json.load(f)
    if not include_directional_premise_roles:
        directional_labels = {
            "Explicit premise role identifiers conflict",
            "Canonical premise identifiers over-cover source",
            "Complete premise role sets agree",
            "Eligible premise role evidence is incomplete or uncertain",
        }
        for comparison in settings["comparisons"]:
            comparison["comparison_levels"] = [
                level
                for level in comparison["comparison_levels"]
                if level.get("label_for_charts") not in directional_labels
            ]
    return settings


def _get_production_model_settings_dict() -> dict:
    return _get_model_settings_dict()


def _get_production_model_settings() -> SettingsCreator:
    return SettingsCreator.from_path_or_dict(_get_production_model_settings_dict())


def _get_missing_marker_recovery_settings() -> SettingsCreator:
    """Return an ablation that keeps only novel sub-premise recovery evidence."""
    settings = _get_model_settings_dict()
    comparison = next(
        comparison
        for comparison in settings["comparisons"]
        if comparison["output_column_name"] == "sub_premise_identifier"
    )
    comparison["comparison_levels"] = [
        level
        for level in comparison["comparison_levels"]
        if level.get("label_for_charts") != "Exact known sub-premise identifier"
    ]
    return SettingsCreator.from_path_or_dict(settings)


def _get_directional_premise_role_settings() -> SettingsCreator:
    """Return the opt-in model with directional premise-role evidence."""
    return SettingsCreator.from_path_or_dict(
        _get_model_settings_dict(include_directional_premise_roles=True)
    )


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


def _get_precomputed_numeric_tf_table(con: DuckDBPyConnection):
    read_tf_sql = package_resource_read_sql(
        "uk_address_matcher.data", "numeric_token_frequencies.parquet"
    )
    return con.sql(read_tf_sql)


def _align_distinguishing_token_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Add a neutral typed token array where either Splink input lacks it."""
    column_name = "distinguishing_adj_start_tokens"
    empty_tokens = ConstantExpression([]).cast("VARCHAR[]").alias(column_name)
    if column_name not in df_addresses_to_match.columns:
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(), empty_tokens
        )
    if column_name not in df_addresses_to_search_within.columns:
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(), empty_tokens
        )
    return df_addresses_to_match, df_addresses_to_search_within


def _align_clean_address_token_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Validate that both cleaned-address representations are available to Splink."""
    required_columns = {"clean_full_address", "clean_full_address_tokens"}
    for relation_name, relation in (
        ("addresses_to_match", df_addresses_to_match),
        ("addresses_to_search_within", df_addresses_to_search_within),
    ):
        missing_columns = sorted(required_columns - set(relation.columns))
        if missing_columns:
            raise ValueError(
                f"{relation_name} is missing required cleaned address columns: "
                f"{', '.join(missing_columns)}"
            )
    return df_addresses_to_match, df_addresses_to_search_within


def _align_sub_premise_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Add nullable sub-premise fields for older prepared canonical data."""
    columns = (
        "sub_premise_marker_token",
        "sub_premise_role",
        "sub_premise_identifier",
    )
    for column in columns:
        expression = ConstantExpression(None).cast("VARCHAR").alias(column)
        if column not in df_addresses_to_match.columns:
            df_addresses_to_match = df_addresses_to_match.select(
                StarExpression(), expression
            )
        if column not in df_addresses_to_search_within.columns:
            df_addresses_to_search_within = df_addresses_to_search_within.select(
                StarExpression(), expression
            )
    return df_addresses_to_match, df_addresses_to_search_within


def _align_address_structure_feature_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Add neutral arrays for address-structure features absent from older data."""
    for relation_name, relation in (
        ("match", df_addresses_to_match),
        ("search", df_addresses_to_search_within),
    ):
        if "distinguishing_token_parts" in relation.columns:
            view_expressions = _distinguishing_token_parts_view_expressions()
            columns_to_replace = [
                "distinguishing_token_parts",
                *(column for column in view_expressions if column in relation.columns),
            ]
            relation = relation.select(
                StarExpression(exclude=columns_to_replace),
                *(SQLExpression(expression) for expression in view_expressions.values()),
            )
        if (
            "distinguishing_lexical_tokens" not in relation.columns
            and "distinguishing_adj_start_tokens" in relation.columns
        ):
            relation = relation.select(
                StarExpression(),
                SQLExpression(_distinguishing_lexical_tokens_expression()),
            )
        if relation_name == "match":
            df_addresses_to_match = relation
        else:
            df_addresses_to_search_within = relation

    array_columns = (
        "distinguishing_adj_start_tokens",
        "common_adj_start_tokens",
        "distinguishing_lexical_tokens",
        "distinguishing_structural_tokens",
        "numeric_tokens",
        "numeric_specific_markers",
    )
    for column in array_columns:
        expression = ConstantExpression([]).cast("VARCHAR[]").alias(column)
        if column not in df_addresses_to_match.columns:
            df_addresses_to_match = df_addresses_to_match.select(
                StarExpression(), expression
            )
        if column not in df_addresses_to_search_within.columns:
            df_addresses_to_search_within = df_addresses_to_search_within.select(
                StarExpression(), expression
            )

    premise_aliases = (
        ("address_structure_premise_type", "commercial_premise_type", "VARCHAR"),
        ("address_structure_premise_id", "commercial_premise_id", "VARCHAR"),
        ("has_address_structure_premise", "has_commercial_premise", "BOOLEAN"),
    )

    def add_missing_premise_aliases(relation: DuckDBPyRelation) -> DuckDBPyRelation:
        for generalized, legacy, data_type in premise_aliases:
            if generalized not in relation.columns:
                if legacy in relation.columns:
                    expression = (
                        ColumnExpression(legacy).cast(data_type).alias(generalized)
                    )
                else:
                    expression = (
                        ConstantExpression(None).cast(data_type).alias(generalized)
                    )
                relation = relation.select(StarExpression(), expression)
            if legacy not in relation.columns:
                expression = ColumnExpression(generalized).cast(data_type).alias(legacy)
                relation = relation.select(StarExpression(), expression)
        return relation

    df_addresses_to_match = add_missing_premise_aliases(df_addresses_to_match)
    df_addresses_to_search_within = add_missing_premise_aliases(
        df_addresses_to_search_within
    )
    return df_addresses_to_match, df_addresses_to_search_within


def _align_commercial_feature_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Compatibility wrapper for the address-structure feature alignment."""
    return _align_address_structure_feature_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )


def _align_numeric_range_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Normalise both inputs to one nullable numeric-range struct."""

    def align_relation(relation: DuckDBPyRelation) -> DuckDBPyRelation:
        relation = ensure_numeric_range_struct(relation)
        aliases = [
            ColumnExpression("numeric_range", field).alias(f"numeric_range_{field}")
            for field in ("lower", "upper")
            if f"numeric_range_{field}" not in relation.columns
        ]
        return relation.select(StarExpression(), *aliases) if aliases else relation

    return align_relation(df_addresses_to_match), align_relation(
        df_addresses_to_search_within
    )


def _align_numeric_tf_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
    *,
    numeric_tf_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Lazily restore numeric TF columns omitted from prepared canonical data."""

    def add_missing_columns(relation: DuckDBPyRelation) -> DuckDBPyRelation:
        missing = [
            index
            for index in range(1, 4)
            if f"tf_numeric_token_{index}" not in relation.columns
        ]
        if not missing:
            return relation

        numeric_columns = set(relation.columns)
        for index in missing:
            tf_column = f"tf_numeric_token_{index}"
            numeric_column = f"numeric_token_{index}"
            if numeric_column not in numeric_columns:
                relation = relation.select(
                    StarExpression(),
                    ConstantExpression(None).cast("DOUBLE").alias(tf_column),
                )
                continue
            base = relation.set_alias("base")
            lookup = numeric_tf_table.set_alias("numeric_tf")
            relation = base.join(
                lookup,
                ColumnExpression("base", numeric_column)
                == ColumnExpression("numeric_tf", "numeric_token"),
                how="left",
            ).select(
                *(ColumnExpression("base", column) for column in base.columns),
                ColumnExpression("numeric_tf", "tf_numeric_token").alias(tf_column),
            )

        return relation

    return add_missing_columns(df_addresses_to_match), add_missing_columns(
        df_addresses_to_search_within
    )


def _align_road_key_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Add the nullable road key required by the packaged blocking rule."""
    column_name = "road_1_norm"
    if column_name not in df_addresses_to_match.columns:
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(), ConstantExpression(None).cast("VARCHAR").alias(column_name)
        )
    if column_name not in df_addresses_to_search_within.columns:
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(), ConstantExpression(None).cast("VARCHAR").alias(column_name)
        )
    return df_addresses_to_match, df_addresses_to_search_within


def _align_postcode_name_columns(
    df_addresses_to_match: DuckDBPyRelation,
    df_addresses_to_search_within: DuckDBPyRelation,
) -> tuple[DuckDBPyRelation, DuckDBPyRelation]:
    """Add neutral typed evidence fields where prepared inputs lack them."""
    for column, value in (
        ("lk_level_by_alias", FunctionExpression("map").cast("MAP(VARCHAR, INTEGER)")),
        ("lk_anchor_uprns", ConstantExpression([]).cast("VARCHAR[]")),
        ("lk_eligible", ConstantExpression(False)),
    ):
        expression = value.alias(column)
        if column not in df_addresses_to_match.columns:
            df_addresses_to_match = df_addresses_to_match.select(
                StarExpression(), expression
            )
        if column not in df_addresses_to_search_within.columns:
            df_addresses_to_search_within = df_addresses_to_search_within.select(
                StarExpression(), expression
            )
    return df_addresses_to_match, df_addresses_to_search_within


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
    owned_splink_frames: list | None = None,
    additional_blocking_rules: list[str] | None = None,
) -> Linker:
    """Build a configured Splink linker for the given relations.

    If ``owned_splink_frames`` is provided, every ``SplinkDataFrame`` this
    function registers on the connection (the three numeric-token term
    frequency lookups and the concat-with-tf input) is appended to it so the
    caller can later drop them via Splink's public
    ``drop_table_from_database_and_remove_from_cache`` API.
    """
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
        df_addresses_to_match = df_addresses_to_match.filter(
            ColumnExpression("resolved_canonical_id").isnull()
        ).select(StarExpression(exclude=excluded_columns))

    if "original_address_concat" in df_addresses_to_match.columns:
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(exclude=["original_address_concat"])
        )
    if "original_address_concat" in df_addresses_to_search_within.columns:
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(exclude=["original_address_concat"])
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

    # TODO(ThomasHepworth): these can all be removed in a 2.0 release
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_distinguishing_token_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_clean_address_token_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_sub_premise_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_address_structure_feature_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_numeric_range_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
    )
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_road_key_columns(df_addresses_to_match, df_addresses_to_search_within)

    if precomputed_numeric_tf_table is None:
        precomputed_numeric_tf_table = _get_precomputed_numeric_tf_table(con)
    (
        df_addresses_to_match,
        df_addresses_to_search_within,
    ) = _align_numeric_tf_columns(
        df_addresses_to_match,
        df_addresses_to_search_within,
        numeric_tf_table=precomputed_numeric_tf_table,
        con=con,
    )

    if settings is None:
        settings_as_dict = _get_model_settings_dict()
    else:
        settings_as_dict = settings.create_settings_dict("duckdb")
        requires_premise_role_fields = any(
            "premise_keys" in str(level.get("sql_condition", ""))
            for comparison in settings_as_dict.get("comparisons", [])
            for level in comparison.get("comparison_levels", [])
        )
        if requires_premise_role_fields:
            missing_fields = [
                f"{side}.{column}"
                for side, relation in (
                    ("incoming", df_addresses_to_match),
                    ("prepared canonical", df_addresses_to_search_within),
                )
                for column in ("premise_keys", "premise_scope_uncertain")
                if column not in relation.columns
            ]
            if missing_fields:
                raise ValueError(
                    "Directional premise-role settings require premise_keys and "
                    "premise_scope_uncertain on both Splink inputs; missing "
                    f"{', '.join(missing_fields)}. Rebuild the prepared canonical "
                    "snapshot with the current cleaning pipeline before enabling "
                    "this model."
                )

    df_addresses_to_match, df_addresses_to_search_within = _align_postcode_name_columns(
        df_addresses_to_match, df_addresses_to_search_within
    )
    if (
        "ukam_address_id" not in df_addresses_to_match.columns
        and "unique_id" in df_addresses_to_match.columns
    ):
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(),
            ColumnExpression("unique_id").cast("VARCHAR").alias("ukam_address_id"),
        )
    if (
        "ukam_address_id" not in df_addresses_to_search_within.columns
        and "unique_id" in df_addresses_to_search_within.columns
    ):
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(),
            ColumnExpression("unique_id").cast("VARCHAR").alias("ukam_address_id"),
        )

    settings_as_dict["linker_uid"] = None
    settings_as_dict = _sanitise_null_comparison_levels(settings_as_dict)

    available_columns = set(df_addresses_to_match.columns).intersection(
        df_addresses_to_search_within.columns
    )
    retained_columns = [
        column
        for column in settings_as_dict.get("additional_columns_to_retain", [])
        if column not in {"original_address_concat", "original_address_concat_canonical"}
        and column in available_columns
    ]
    if additional_columns_to_retain:
        retained_columns.extend(
            column
            for column in additional_columns_to_retain
            if column
            not in {"original_address_concat", "original_address_concat_canonical"}
            and column in available_columns
        )
    settings_as_dict["additional_columns_to_retain"] = list(
        dict.fromkeys(retained_columns)
    )

    # Use ukam_address_id as unique_id column name
    # (created as part of our cleaning process).
    settings_as_dict["unique_id_column_name"] = "ukam_address_id"
    # Also make sure we now retain unique_id from both datasets...

    settings_as_dict["additional_columns_to_retain"] = list(
        dict.fromkeys(settings_as_dict["additional_columns_to_retain"] + ["unique_id"])
    )

    # Align the signature evidence score map across both inputs. Live messy
    # cleaning always emits `signature_score_map`, but a prepared canonical
    # built before this column existed will not have it. The linker concat
    # selects the messy column set from BOTH frames, so add an empty,
    # type-compatible map to whichever side is missing it to avoid binder
    # errors. Only retain it for the comparison when present on the messy side.
    _empty_score_map = (
        FunctionExpression("map")
        .cast("MAP(VARCHAR, DOUBLE)")
        .alias("signature_score_map")
    )
    messy_has_score_map = "signature_score_map" in df_addresses_to_match.columns
    canonical_has_score_map = (
        "signature_score_map" in df_addresses_to_search_within.columns
    )
    if messy_has_score_map and not canonical_has_score_map:
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(), _empty_score_map
        )
    elif canonical_has_score_map and not messy_has_score_map:
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(), _empty_score_map
        )
    if messy_has_score_map or canonical_has_score_map:
        settings_as_dict["additional_columns_to_retain"] = list(
            dict.fromkeys(
                settings_as_dict["additional_columns_to_retain"] + ["signature_score_map"]
            )
        )

    # Align the parallel unique-hit count map the same way. It carries, per
    # candidate canonical id, the number of shared inverted-index keys whose
    # posting list has size 1 (i.e. a key unique to that canonical) so the
    # signature comparison can reward unique rare-span hits.
    _empty_hits_map = (
        FunctionExpression("map")
        .cast("MAP(VARCHAR, BIGINT)")
        .alias("signature_unique_hits_map")
    )
    messy_has_hits_map = "signature_unique_hits_map" in df_addresses_to_match.columns
    canonical_has_hits_map = (
        "signature_unique_hits_map" in df_addresses_to_search_within.columns
    )
    if messy_has_hits_map and not canonical_has_hits_map:
        df_addresses_to_search_within = df_addresses_to_search_within.select(
            StarExpression(), _empty_hits_map
        )
    elif canonical_has_hits_map and not messy_has_hits_map:
        df_addresses_to_match = df_addresses_to_match.select(
            StarExpression(), _empty_hits_map
        )
    if messy_has_hits_map or canonical_has_hits_map:
        settings_as_dict["additional_columns_to_retain"] = list(
            dict.fromkeys(
                settings_as_dict["additional_columns_to_retain"]
                + ["signature_unique_hits_map"]
            )
        )

    # Auto-detect ukam_label: if present in messy data, retain it for accuracy testing.
    if "ukam_label" in df_addresses_to_match.columns:
        settings_as_dict["additional_columns_to_retain"] = list(
            dict.fromkeys(
                settings_as_dict["additional_columns_to_retain"] + ["ukam_label"]
            )
        )
        if "ukam_label" not in df_addresses_to_search_within.columns:
            df_addresses_to_search_within = df_addresses_to_search_within.select(
                StarExpression(),
                ConstantExpression(None).cast("VARCHAR").alias("ukam_label"),
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
        brs = [
            br
            for br in brs
            if br["blocking_rule"]
            != block_on("postcode").get_blocking_rule("duckdb").blocking_rule_sql
        ]

    if not include_outside_postcode_block:
        brs = [block_on("postcode").get_blocking_rule("duckdb").as_dict()]

    if additional_blocking_rules:
        brs.extend(
            {"blocking_rule": rule, "sql_dialect": "duckdb"}
            for rule in additional_blocking_rules
        )

    settings_as_dict["blocking_rules_to_generate_predictions"] = brs

    settings = SettingsCreator.from_path_or_dict(settings_as_dict)

    df_addresses_to_match_fix = df_addresses_to_match

    # See https://github.com/moj-analytical-services/uk_address_matcher/issues/253
    # con.register("df_addresses_to_search_within_fix", df_addresses_to_search_within)
    # df_addresses_to_search_within_fix = con.table("df_addresses_to_search_within_fix")
    df_addresses_to_search_within_fix = df_addresses_to_search_within

    # Splink expects the prediction-side dataset labels to be `m_` and `c_`.
    # Register the matching relations under those names for the duration of the
    # Splink run instead of dropping any user table that happens to share the
    # name. The caller (SplinkStage) is responsible for unregistering both
    # names once Splink has materialised its outputs, restoring any shadowed
    # user tables. Registration order: unregister first so a stale
    # registration from a previous (possibly failed) run is replaced.
    messy_name, canonical_name = "m_", "c_"
    for table_name, relation in (
        (messy_name, df_addresses_to_match_fix),
        (canonical_name, df_addresses_to_search_within_fix),
    ):
        try:
            con.unregister(table_name)
        except InvalidInputException:
            pass
        con.register(table_name, relation)

    db_api = DuckDBAPI(connection=con)

    linker = Linker(
        [messy_name, canonical_name],
        settings=settings,
        db_api=db_api,
        input_table_aliases=[messy_name, canonical_name],
        set_up_basic_logging=False,
    )

    for i in range(1, 4):
        df = precomputed_numeric_tf_table.select(
            ColumnExpression("numeric_token").alias(f"numeric_token_{i}"),
            ColumnExpression("tf_numeric_token").alias(f"tf_numeric_token_{i}"),
        )
        tf_frame = linker.table_management.register_term_frequency_lookup(
            df, f"numeric_token_{i}", overwrite=True
        )
        if owned_splink_frames is not None:
            owned_splink_frames.append(tf_frame)

    cols_to_select = [
        column
        for column in df_addresses_to_match.columns
        if column != "original_address_concat"
        and column in df_addresses_to_search_within.columns
    ]
    columns = [ColumnExpression(column) for column in cols_to_select]
    concat_with_tf = df_addresses_to_match.select(
        *columns, ConstantExpression(messy_name).alias("source_dataset")
    ).union(
        df_addresses_to_search_within.select(
            *columns, ConstantExpression(canonical_name).alias("source_dataset")
        )
    )
    concat_frame = linker.table_management.register_table_input_nodes_concat_with_tf(
        concat_with_tf
    )
    if owned_splink_frames is not None:
        owned_splink_frames.append(concat_frame)

    return linker
