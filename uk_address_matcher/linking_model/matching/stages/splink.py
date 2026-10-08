from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from duckdb import InvalidInputException

from uk_address_matcher.cleaning.local_keys import add_local_key_features
from uk_address_matcher.cleaning.steps.roadlike_places import (
    add_road_blocking_features,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.post_linkage.distinguishing_features.numeric_range import (
    NumericRangeRerankerConfig,
    ensure_numeric_range_struct,
    project_splink_predictions,
)
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb
    from splink import SettingsCreator

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


def _local_key_input_aliases(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in con.sql("""
            SELECT view_name FROM duckdb_views()
            WHERE view_name LIKE 'local_key_source%'
                OR view_name LIKE 'local_key_canonical%'
        """).fetchall()
    }


@pipeline_stage(name="preserve_anchored_name_aliases", materialized=True)
def _anchored_name_aliases() -> list[CTEStep]:
    return [
        CTEStep(
            "alias_levels",
            """
            SELECT predictions.*,
                coalesce(map_extract_value(lk_level_by_alias_r,
                    ukam_address_id_l::VARCHAR), 0) AS spacing_alias_level
            FROM {local_key_source_predictions} predictions
            """,
        ),
        CTEStep(
            "alias_alternatives",
            """
            SELECT levels.unique_id_l, levels.unique_id_r,
                max(levels.match_weight) AS nonspacing_alias_weight
            FROM {alias_levels} levels JOIN (
                SELECT DISTINCT unique_id_l, unique_id_r FROM {alias_levels}
                WHERE spacing_alias_level = 2 AND __spacing_name_factor > 1
                    AND len(lk_anchor_uprns_r) = 1
                    AND list_contains(lk_anchor_uprns_r, unique_id_l::VARCHAR)
            ) affected USING (unique_id_l, unique_id_r)
            WHERE levels.spacing_alias_level <> 2
            GROUP BY levels.unique_id_l, levels.unique_id_r
            """,
        ),
        CTEStep(
            "preserved_aliases",
            """
            SELECT levels.* EXCLUDE (spacing_alias_level, __spacing_name_factor)
            FROM {alias_levels} levels
            LEFT JOIN {alias_alternatives} alternatives USING (unique_id_l, unique_id_r)
            WHERE NOT coalesce(
                spacing_alias_level = 2 AND nonspacing_alias_weight IS NOT NULL
                AND __spacing_name_factor > 1
                AND len(lk_anchor_uprns_r) = 1
                AND list_contains(lk_anchor_uprns_r, unique_id_l::VARCHAR)
                AND match_weight >= nonspacing_alias_weight
                AND match_weight - CASE WHEN __spacing_name_factor > 1
                    THEN log2(__spacing_name_factor) ELSE 0 END
                    < nonspacing_alias_weight,
                FALSE)
            """,
        ),
    ]


def _preserve_anchored_name_aliases(
    con: duckdb.DuckDBPyConnection, predictions: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    if not {
        "lk_level_by_alias_r",
        "lk_anchor_uprns_r",
        "bf_postcode_distinguishing_name",
    }.issubset(predictions.columns):
        return predictions
    factor = "bf_postcode_distinguishing_name"
    if "bf_tf_postcode_distinguishing_name" in predictions.columns:
        factor += " * coalesce(bf_tf_postcode_distinguishing_name, 1.0)"
    if (
        predictions.filter(f"""
        ({factor}) > 1
        AND map_extract_value(lk_level_by_alias_r, ukam_address_id_l::VARCHAR) = 2
        AND len(lk_anchor_uprns_r) = 1
        AND list_contains(lk_anchor_uprns_r, unique_id_l::VARCHAR)
    """)
        .limit(1)
        .fetchone()
        is None
    ):
        return predictions
    return create_sql_pipeline(
        con,
        [
            InputBinding(
                "local_key_source_predictions",
                predictions.select(f"*, ({factor}) AS __spacing_name_factor"),
            )
        ],
        stage_specs=[_anchored_name_aliases],
        pipeline_name="Preserve exact anchored name aliases",
    ).run()


def _prepare_inferred_road_scoring_features(
    con: duckdb.DuckDBPyConnection,
    df_unmatched: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    canonical_road_keys_path: str | None = None,
    roadlike_places: duckdb.DuckDBPyRelation | None = None,
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    # TODO(ThomasHepworth): remove in 2.0; support the legacy explicit road-key file.
    if "road_1_norm" not in df_canonical.columns and canonical_road_keys_path:
        escaped_path = canonical_road_keys_path.replace("'", "''")
        df_canonical = con.sql(
            "SELECT canonical.*, road.road_1_norm "
            f"FROM ({df_canonical.sql_query()}) AS canonical "
            f"LEFT JOIN read_parquet('{escaped_path}') AS road "
            "USING (ukam_address_id)"
        )
    elif "road_1_norm" not in df_canonical.columns and roadlike_places is not None:
        df_canonical = add_road_blocking_features(
            con,
            df_canonical,
            roadlike_places=roadlike_places,
        )

    if "road_1_norm" in df_canonical.columns:
        if "road_1_norm" not in df_unmatched.columns:
            if roadlike_places is not None:
                df_unmatched = add_road_blocking_features(
                    con,
                    df_unmatched,
                    roadlike_places=roadlike_places,
                )
            else:
                # TODO(ThomasHepworth): remove in 2.0; keep legacy callers neutral.
                df_unmatched = df_unmatched.select("*, NULL::VARCHAR AS road_1_norm")
    else:
        if "road_1_norm" not in df_unmatched.columns:
            # TODO(ThomasHepworth): remove in 2.0; keep legacy callers neutral.
            df_unmatched = df_unmatched.select("*, NULL::VARCHAR AS road_1_norm")
        # TODO(ThomasHepworth): remove in 2.0; keep legacy callers neutral.
        df_canonical = df_canonical.select("*, NULL::VARCHAR AS road_1_norm")

    return df_unmatched, df_canonical


@dataclass(repr=False)
class SplinkStage(MatchingStage):
    """Probabilistic matching stage built on Splink.

    This stage is usually placed last because it is the only stage that emits a
    score and therefore requires threshold tuning. Earlier deterministic stages
    should remove the obvious high-precision matches first, leaving Splink to
    handle the harder residual cases.

    The stage returns the standard match columns plus two key diagnostics:

    - ``match_weight``: the strength of evidence for the selected canonical
      candidate. Higher is better.
    - ``distinguishability``: the gap in ``match_weight`` between the best
      candidate and the next best candidate for the same messy record. Higher
      means the winner is clearer. ``NULL`` usually means there was only one
      candidate left after blocking.

    Setting ``final_match_weight_threshold=-20`` and
    ``final_distinguishability_threshold=0.0`` is a permissive configuration
    that keeps almost all top-ranked Splink candidates. Raising either
    threshold filters out more weak or ambiguous matches, typically improving
    precision at the cost of recall.

    Args:
        predict_threshold_match_weight: Initial minimum score passed to
            ``linker.inference.predict()``. Lower values retain more candidate
            pairs for later refinement.
        improve_threshold_match_weight: Minimum score considered when applying
            the token-based score adjustment step.
        improve_top_n_matches: Number of top candidate pairs per messy address
            to retain for the token-based score adjustment step.
        improve_use_bigrams: Whether the token-based improvement step should
            use bigrams as well as single tokens.
        use_relation_marker_reranker: Whether to apply the optional relation-
            marker score adjustment after token-based reranking.
        reranker_token_reward_multiplier: Multiplier for distinctive token
            agreement in the local reranker.
        reranker_bigram_reward_multiplier: Multiplier for distinctive bigram
            agreement in the local reranker.
        final_match_weight_threshold: Minimum ``match_weight`` required for a
            Splink match to be emitted in the final results.
        final_distinguishability_threshold: Minimum distinguishability required
            for a Splink match to be emitted. Set to ``None`` to disable this
            filter.
        include_full_postcode_block: Whether to include a strict full-postcode
            blocking rule when generating Splink candidate pairs.
        include_outside_postcode_block: Whether to include broader blocking
            rules that can generate candidate pairs across postcode boundaries.
        additional_columns_to_retain: Extra columns to keep in the Splink
            predictions and downstream inspection output.
        settings: Optional custom Splink settings object. Leave as ``None`` to
            use the library defaults.
        retain_intermediate_calculation_columns: Retain Splink comparison
            columns needed for debugging and waterfall charts.
    """

    # Prediction threshold for initial Splink predict() call
    predict_threshold_match_weight: float = -20

    # Threshold for improve_predictions_using_distinguishing_tokens
    improve_threshold_match_weight: float = -20
    improve_top_n_matches: int = 5
    improve_use_bigrams: bool = True
    use_relation_marker_reranker: bool = False
    reranker_token_reward_multiplier: float = 3.0
    reranker_bigram_reward_multiplier: float = 2.2

    # Thresholds for final candidate selection
    final_match_weight_threshold: float = -20.0
    final_distinguishability_threshold: float | None = 0.0

    # Blocking configuration
    include_full_postcode_block: bool = False
    include_outside_postcode_block: bool = True

    # Additional columns to retain through Splink
    additional_columns_to_retain: list[str] | None = field(default=None)

    # Advanced: supply custom Splink settings
    settings: SettingsCreator | None = field(default=None, repr=False)

    # Whether to retain intermediate calculation columns (for debugging)
    retain_intermediate_calculation_columns: bool = False

    canonical_road_keys_path: str | None = None
    roadlike_places: duckdb.DuckDBPyRelation | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    # Populated after find_matches runs — used by MatchResult for inspection
    linker: Any = field(default=None, init=False, repr=False)
    predictions_table: str | None = field(default=None, init=False, repr=False)
    improved_predictions_table: str | None = field(default=None, init=False, repr=False)
    best_matches_table: str | None = field(default=None, init=False, repr=False)

    _owned_splink_frames: tuple = field(default=(), init=False, repr=False)

    def _release_splink_tables(self) -> None:
        """Drop this run's Splink tables via Splink's public API."""
        for frame in self._owned_splink_frames:
            frame.drop_table_from_database_and_remove_from_cache(
                force_non_splink_table=True
            )
        self._owned_splink_frames = ()

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: DebugOptions | None = None,
        explain: bool = False,
    ) -> duckdb.DuckDBPyRelation | None:
        from uk_address_matcher.linking_model.splink_model import _get_linker
        from uk_address_matcher.post_linkage.analyse_results import (
            best_matches_with_distinguishability,
        )
        from uk_address_matcher.post_linkage.distinguishing_features import (
            relation_markers,
        )
        from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
            improve_predictions_using_distinguishing_tokens,
        )
        from uk_address_matcher.sql_pipeline.helpers import (
            _drop_table_and_registered_aliases,
            _uid,
        )
        from uk_address_matcher.sql_pipeline.match_reasons import MatchReason

        if explain:
            return None

        unmatched_count = df_unmatched.count("*").fetchone()[0]
        if unmatched_count == 0:
            return None

        local_key_aliases_before = _local_key_input_aliases(con)
        owned_frames: list = []
        reranker_matches_table: str | None = None
        self.linker = None
        self.predictions_table = None
        self.improved_predictions_table = None
        self.best_matches_table = None
        self._owned_splink_frames = ()
        completed = False
        try:
            df_unmatched, df_canonical = _prepare_inferred_road_scoring_features(
                con,
                df_unmatched,
                df_canonical,
                canonical_road_keys_path=self.canonical_road_keys_path,
                roadlike_places=self.roadlike_places,
            )

            df_unmatched, df_canonical = add_local_key_features(
                con, df_unmatched, df_canonical
            )

            numeric_range_reranker = NumericRangeRerankerConfig()
            range_metadata_available = (
                "numeric_range" in df_canonical.columns
                and "numeric_range" in df_unmatched.columns
                and "numeric_tokens" in df_canonical.columns
                and "numeric_tokens" in df_unmatched.columns
                and "flat_identity" in df_canonical.columns
                and "flat_identity" in df_unmatched.columns
            )
            if range_metadata_available:
                df_unmatched = ensure_numeric_range_struct(df_unmatched)
                df_canonical = ensure_numeric_range_struct(df_canonical)
                range_input_columns = [
                    "numeric_range",
                    "numeric_tokens",
                    "flat_identity",
                ]
            else:
                numeric_range_reranker = None
                range_input_columns = []
            linker_columns = [
                "common_end_tokens_hist",
                "clean_full_address",
                "postcode",
                "lk_level_by_alias",
                "lk_anchor_uprns",
                "lk_eligible",
            ]
            linker_columns.extend(self.additional_columns_to_retain or [])
            linker_columns.extend(range_input_columns)
            linker_columns = list(dict.fromkeys(linker_columns))

            # Step 1: Build linker
            linker = _get_linker(
                df_addresses_to_match=df_unmatched,
                df_addresses_to_search_within=df_canonical,
                con=con,
                include_full_postcode_block=self.include_full_postcode_block,
                include_outside_postcode_block=self.include_outside_postcode_block,
                additional_columns_to_retain=linker_columns or None,
                retain_intermediate_calculation_columns=True,
                settings=self.settings,
                owned_splink_frames=owned_frames,
                retain_matching_columns=False,
            )

            self.linker = linker

            # Step 2: Predict
            df_predict = linker.inference.predict(
                threshold_match_weight=self.predict_threshold_match_weight
            )
            owned_frames.append(df_predict)
            raw_prediction_ddb = df_predict.as_duckdbpyrelation()

            prediction_output = project_splink_predictions(
                con,
                raw_prediction_ddb,
                retain_intermediate_calculation_columns=(
                    self.retain_intermediate_calculation_columns
                ),
            )
            table_name = f"__ukam__splink__predictions__{_uid()}"
            con.execute(
                "CREATE OR REPLACE TEMP TABLE "
                + table_name
                + " AS SELECT * FROM ("
                + prediction_output.sql_query()
                + ")"
            )
            self.predictions_table = table_name
            df_predict_ddb = con.table(table_name)
            df_predict_for_improvement = _preserve_anchored_name_aliases(
                con, raw_prediction_ddb
            )
            if numeric_range_reranker is None:
                df_predict_for_improvement = project_splink_predictions(
                    con,
                    df_predict_for_improvement,
                    retain_intermediate_calculation_columns=(
                        self.retain_intermediate_calculation_columns
                    ),
                )
            df_improved = improve_predictions_using_distinguishing_tokens(
                df_predict=df_predict_for_improvement,
                con=con,
                match_weight_threshold=self.improve_threshold_match_weight,
                top_n_matches=self.improve_top_n_matches,
                use_bigrams=self.improve_use_bigrams,
                REWARD_MULTIPLIER=self.reranker_token_reward_multiplier,
                BIGRAM_REWARD_MULTIPLIER=self.reranker_bigram_reward_multiplier,
                additional_columns_to_retain=[
                    column
                    for column in linker_columns
                    if column not in range_input_columns
                    if f"{column}_l" in df_predict_ddb.columns
                    and f"{column}_r" in df_predict_ddb.columns
                ]
                or None,
                numeric_range_reranker=numeric_range_reranker,
                df_addresses_to_match=df_unmatched,
                df_addresses_to_search_within=df_canonical,
            )
            reranker_matches_table = df_improved.alias
            if self.use_relation_marker_reranker:
                df_improved = relation_markers.improve_predictions_using_relation_markers(
                    df_predict=df_improved,
                    con=con,
                )
            improved_table_name = f"__ukam__splink__improved_predictions__{_uid()}"
            con.execute(
                "CREATE OR REPLACE TEMP TABLE "
                + improved_table_name
                + " AS SELECT * FROM ("
                + df_improved.sql_query()
                + ")"
            )
            self.improved_predictions_table = improved_table_name
            df_improved = con.table(improved_table_name)

            # Step 4: Compute distinguishability and select best match per record
            # This returns an unmaterialised relation
            df_best = best_matches_with_distinguishability(
                df_predict=df_improved,
                df_addresses_to_match=df_unmatched,
                con=con,
                best_match_only=False,
            )

            df_best_name = f"__ukam__splink__best_matches__{_uid()}"
            con.execute(
                "CREATE OR REPLACE TEMP TABLE "
                + df_best_name
                + " AS SELECT * FROM ("
                + df_best.sql_query()
                + ")"
            )
            self.best_matches_table = df_best_name

            # Step 5: Apply thresholds and project to standard columns
            splink_label = MatchReason.SPLINK.value

            dist_filter = ""
            if self.final_distinguishability_threshold is not None:
                dist_filter = (
                    "AND (distinguishability IS NULL "
                    f"OR distinguishability >= {self.final_distinguishability_threshold})"
                )

            range_audit_projection = ""
            range_audit_projection = "".join(
                f", best_match.{column}"
                for column in (
                    "legacy_numeric_bits",
                    "numeric_range_relationship",
                    "numeric_range_guard_passed",
                    "numeric_range_guard_reason",
                    "numeric_range_base_bits",
                    "numeric_range_tf_bits",
                    "numeric_range_adjustment",
                )
                if column in df_best.columns
            )

            result = con.sql(f"""
                SELECT
                    best_match.ukam_address_id_r AS ukam_address_id,
                    best_match.unique_id_l AS resolved_canonical_id,
                    best_match.ukam_address_id_l AS canonical_ukam_address_id,
                    '{splink_label}' AS match_reason,
                    best_match.match_weight,
                    best_match.distinguishability
                    {range_audit_projection}
                FROM (
                    SELECT *
                    FROM {df_best_name}
                    WHERE candidate_rank = 1
                ) AS best_match
                WHERE best_match.match_weight >= {self.final_match_weight_threshold}
                {dist_filter}
                AND best_match.unique_id_l IS NOT NULL
            """)
            completed = True
            return result
        finally:
            # Unregister both aliases even if linker setup failed before registration.
            for input_name in ("m_", "c_"):
                with suppress(InvalidInputException):
                    con.unregister(input_name)
            for input_name in _local_key_input_aliases(con) - local_key_aliases_before:
                with suppress(InvalidInputException):
                    con.unregister(input_name)
            self._owned_splink_frames = tuple(owned_frames)
            if reranker_matches_table is not None:
                _drop_table_and_registered_aliases(con, reranker_matches_table)
            if not completed:
                self._release_splink_tables()
                for table_name in (
                    self.predictions_table,
                    self.improved_predictions_table,
                    self.best_matches_table,
                ):
                    if table_name is not None:
                        _drop_table_and_registered_aliases(con, table_name)
                self.linker = None
                self.predictions_table = None
                self.improved_predictions_table = None
                self.best_matches_table = None
