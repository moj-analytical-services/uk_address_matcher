from __future__ import annotations

import logging
import time
from types import MethodType
from typing import TYPE_CHECKING

from splink.internals.blocking import (
    _columns_needed_for_blocking,
    block_using_rules_sqls,
    materialise_exploded_id_tables,
)
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.misc import threshold_args_to_match_weight
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import _combine_prior_and_bfs
from splink.internals.settings import Settings
from splink.internals.vertically_concatenate import (
    compute_df_concat_with_tf,
    select_two_dataset_link_only_input_tables_sqls,
)

from .base import PreSplinkIntegration, extend_unique_columns

if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.linker_components.inference import LinkerInference
    from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

_DF_CONCAT_WITH_TF = "__splink__df_concat_with_tf"
_DF_CONCAT_WITH_TF_LEFT = "__splink__df_concat_with_tf_left"
_DF_CONCAT_WITH_TF_RIGHT = "__splink__df_concat_with_tf_right"
_DF_COMPARISON_VECTORS = "__splink__df_comparison_vectors"
_DF_MATCH_WEIGHT_PARTS = "__splink__df_match_weight_parts"
_DF_PREDICT = "__splink__df_predict"


def _prepare_blocking_pipeline(
    self: LinkerInference,
    *,
    materialise_blocked_pairs: bool,
) -> tuple[CTEPipeline, list, SplinkDataFrame | None, list[PreSplinkIntegration], float]:
    pipeline = CTEPipeline()
    df_concat_with_tf = compute_df_concat_with_tf(self._linker, pipeline)
    pipeline = CTEPipeline([df_concat_with_tf])

    integrations = list(getattr(self._linker, "_ukam_splink_integrations", []))
    settings_obj = self._linker._settings_obj
    link_type = settings_obj._link_type
    input_tablename_l = _DF_CONCAT_WITH_TF
    input_tablename_r = _DF_CONCAT_WITH_TF

    if len(self._linker._input_tables_dict) == 2 and link_type == "link_only":
        input_columns = _columns_needed_for_blocking(
            settings_obj._blocking_rules_to_generate_predictions,
            source_dataset_input_column=(
                settings_obj.column_info_settings.source_dataset_input_column
            ),
            unique_id_input_column=(
                settings_obj.column_info_settings.unique_id_input_column
            ),
        )
        left_sql, right_sql = select_two_dataset_link_only_input_tables_sqls(
            self._linker._input_tables_dict,
            input_columns=input_columns,
            source_dataset_input_column=(
                settings_obj.column_info_settings.source_dataset_input_column
            ),
        )
        pipeline.enqueue_sql(left_sql, _DF_CONCAT_WITH_TF_LEFT)
        pipeline.enqueue_sql(right_sql, _DF_CONCAT_WITH_TF_RIGHT)
        input_tablename_l = _DF_CONCAT_WITH_TF_LEFT
        input_tablename_r = _DF_CONCAT_WITH_TF_RIGHT
        link_type = "two_dataset_link_only"

    exploding_br_with_id_tables = materialise_exploded_id_tables(
        link_type=link_type,
        blocking_rules=settings_obj._blocking_rules_to_generate_predictions,
        db_api=self._linker._db_api,
        splink_df_dict=self._linker._input_tables_dict,
        source_dataset_input_column=(
            settings_obj.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=settings_obj.column_info_settings.unique_id_input_column,
    )
    pipeline.enqueue_list_of_sqls(
        block_using_rules_sqls(
            input_tablename_l=input_tablename_l,
            input_tablename_r=input_tablename_r,
            blocking_rules=settings_obj._blocking_rules_to_generate_predictions,
            link_type=link_type,
            source_dataset_input_column=(
                settings_obj.column_info_settings.source_dataset_input_column
            ),
            unique_id_input_column=(
                settings_obj.column_info_settings.unique_id_input_column
            ),
        )
    )

    start_time = time.perf_counter()
    blocked_pairs = None
    if materialise_blocked_pairs:
        blocked_pairs = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
        pipeline = CTEPipeline([blocked_pairs, df_concat_with_tf])
        logger.info("Blocking time: %.2f seconds", time.perf_counter() - start_time)
        start_time = time.perf_counter()

    comparison_vector_sqls = compute_comparison_vector_values_from_id_pairs_sqls(
        settings_obj._columns_to_select_for_blocking,
        settings_obj._columns_to_select_for_comparison_vector_values,
        input_tablename_l=_DF_CONCAT_WITH_TF,
        input_tablename_r=_DF_CONCAT_WITH_TF,
        source_dataset_input_column=(
            settings_obj.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=settings_obj.column_info_settings.unique_id_input_column,
        link_type=link_type,
        sql_dialect_str=self._linker._sql_dialect_str,
    )
    pipeline.enqueue_list_of_sqls(comparison_vector_sqls[:-1])

    return pipeline, exploding_br_with_id_tables, blocked_pairs, integrations, start_time


def _enqueue_integration_pipeline(
    pipeline: CTEPipeline,
    *,
    settings_obj: Settings,
    integrations: list[PreSplinkIntegration],
) -> list[str]:
    blocked_pair_source = "blocked_with_cols"
    for integration in integrations:
        blocked_pair_source = integration.enqueue_blocked_pair_feature_sql(
            pipeline,
            input_table=blocked_pair_source,
            nodes_table=_DF_CONCAT_WITH_TF,
        )

    comparison_vector_columns = list(
        settings_obj._columns_to_select_for_comparison_vector_values
    )
    for integration in integrations:
        extend_unique_columns(
            comparison_vector_columns,
            integration.retained_feature_columns,
        )

    pipeline.enqueue_sql(
        f"""
        SELECT
            {",\n            ".join(comparison_vector_columns)}
        FROM {blocked_pair_source}
        """,
        _DF_COMPARISON_VECTORS,
    )

    comparison_vector_source = _DF_COMPARISON_VECTORS
    for integration in integrations:
        comparison_vector_source = integration.enqueue_comparison_vector_sql(
            pipeline,
            input_table=comparison_vector_source,
        )

    match_weight_part_columns = Settings.columns_to_select_for_bayes_factor_parts(
        unique_id_input_columns=settings_obj.column_info_settings.unique_id_input_columns,
        comparisons=settings_obj.core_model_settings.comparisons,
        retain_matching_columns=settings_obj._retain_matching_columns,
        retain_intermediate_calculation_columns=(
            settings_obj._retain_intermediate_calculation_columns
        ),
        additional_columns_to_retain=settings_obj._additional_columns_to_retain,
    )
    integration_bf_terms: list[str] = []
    for integration in integrations:
        integration_bf_terms.extend(
            integration.extend_match_weight_part_columns(match_weight_part_columns)
        )

    pipeline.enqueue_sql(
        f"""
        SELECT
            {",\n            ".join(match_weight_part_columns)}
        FROM {comparison_vector_source}
        """,
        _DF_MATCH_WEIGHT_PARTS,
    )
    return integration_bf_terms


def _enqueue_predict_sql(
    self: LinkerInference,
    pipeline: CTEPipeline,
    *,
    settings_obj: Settings,
    integrations: list[PreSplinkIntegration],
    integration_bf_terms: list[str],
    threshold_match_probability: float | None,
    threshold_match_weight: float | None,
) -> None:
    predict_columns = Settings.columns_to_select_for_predict(
        unique_id_input_columns=settings_obj.column_info_settings.unique_id_input_columns,
        comparisons=settings_obj.core_model_settings.comparisons,
        retain_matching_columns=settings_obj._retain_matching_columns,
        retain_intermediate_calculation_columns=(
            settings_obj._retain_intermediate_calculation_columns
        ),
        training_mode=False,
        additional_columns_to_retain=settings_obj._additional_columns_to_retain,
    )
    for integration in integrations:
        integration.extend_predict_columns(predict_columns, settings=settings_obj)

    bf_terms: list[str] = []
    for comparison in settings_obj.core_model_settings.comparisons:
        bf_terms.extend(comparison._match_weight_columns_to_multiply)
    bf_terms.extend(integration_bf_terms)

    bayes_factor_expr, match_probability_expr = _combine_prior_and_bfs(
        settings_obj.core_model_settings.probability_two_random_records_match,
        bf_terms,
        self._linker._infinity_expression,
        self._linker._sql_dialect,
    )
    threshold_as_match_weight = threshold_args_to_match_weight(
        threshold_match_probability,
        threshold_match_weight,
    )
    threshold_sql = ""
    if threshold_as_match_weight is not None:
        threshold_sql = f"WHERE log2({bayes_factor_expr}) >= {threshold_as_match_weight}"

    pipeline.enqueue_sql(
        f"""
        SELECT
            log2({bayes_factor_expr}) AS match_weight,
            {match_probability_expr} AS match_probability,
            {",\n            ".join(predict_columns)}
        FROM {_DF_MATCH_WEIGHT_PARTS}
        {threshold_sql}
        """,
        _DF_PREDICT,
    )


def _predict_with_pre_splink_integrations(
    self: LinkerInference,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    materialise_after_computing_term_frequencies: bool = True,
    materialise_blocked_pairs: bool = True,
) -> SplinkDataFrame:
    del materialise_after_computing_term_frequencies

    settings_obj = self._linker._settings_obj
    pipeline, exploding_br_with_id_tables, blocked_pairs, integrations, start_time = (
        _prepare_blocking_pipeline(
            self,
            materialise_blocked_pairs=materialise_blocked_pairs,
        )
    )

    integration_bf_terms = _enqueue_integration_pipeline(
        pipeline,
        settings_obj=settings_obj,
        integrations=integrations,
    )
    _enqueue_predict_sql(
        self,
        pipeline,
        settings_obj=settings_obj,
        integrations=integrations,
        integration_bf_terms=integration_bf_terms,
        threshold_match_probability=threshold_match_probability,
        threshold_match_weight=threshold_match_weight,
    )

    predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    predict_time = time.perf_counter() - start_time
    logger.info("Predict time: %.2f seconds", predict_time)

    self._linker._predict_warning()

    for exploded_table in exploding_br_with_id_tables:
        exploded_table.drop_materialised_id_pairs_dataframe()
    if blocked_pairs is not None:
        blocked_pairs.drop_table_from_database_and_remove_from_cache()

    return predictions


def patch_linker_inference_predict(linker: Linker) -> None:
    inference = linker.inference
    if getattr(inference, "_ukam_pre_splink_predict_patched", False):
        return

    inference.predict = MethodType(_predict_with_pre_splink_integrations, inference)
    inference._ukam_pre_splink_predict_patched = True
