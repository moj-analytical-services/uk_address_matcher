from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import TYPE_CHECKING, Any, Optional

from uk_address_matcher.cleaning.steps.roadlike_places import (
    ROAD_FEATURE_COLUMNS,
    add_road_blocking_features,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.post_linkage.distinguishing_features.numeric_range import (
    NumericRangeRerankerConfig,
    ensure_numeric_range_struct,
    project_splink_predictions,
)

if TYPE_CHECKING:
    import duckdb
    from splink import SettingsCreator

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


ROAD_NUMERIC_BLOCKING_RULES = (
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_2 = r.numeric_token_2",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_2",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_2 = r.numeric_token_1",
)
ROAD_OUTWARD_BLOCKING_RULE = (
    "l.road_1_norm = r.road_1_norm "
    "AND l.outward_postcode = r.outward_postcode"
)
ROAD_EXACT_RANGE_BLOCKING_RULE = (
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_range_lower = r.numeric_range_lower "
    "AND l.numeric_range_upper = r.numeric_range_upper"
)
ROAD_UNUSUAL_1_BLOCKING_RULE = (
    "l.road_1_norm = r.road_1_norm "
    "AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)"
)
ROAD_UNUSUAL_1_CROSS_BLOCKING_RULE = (
    "l.road_1_norm = r.road_1_norm "
    "AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2)"
)
ROAD_EXTREMELY_UNUSUAL_1_BLOCKING_RULE = (
    "l.road_1_norm = r.road_1_norm "
    "AND list_extract(l.extremely_unusual_tokens_arr, 1) "
    "= list_extract(r.extremely_unusual_tokens_arr, 1)"
)
ROAD_ONLY_BLOCKING_RULE = "l.road_1_norm = r.road_1_norm"
SELECTIVE_ROAD_BLOCKING_RULES = (
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND r.road_frequency_lte_1000",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND r.road_n1_block_size_lte_32",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND l.flat_letter = r.flat_letter",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND l.numeric_token_2 = r.numeric_token_2",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND list_extract(l.unusual_tokens_arr, 1) "
    "= list_extract(r.unusual_tokens_arr, 1)",
    "l.road_1_norm = r.road_1_norm "
    "AND l.numeric_token_1 = r.numeric_token_1 "
    "AND list_extract(l.unusual_tokens_arr, 2) "
    "= list_extract(r.unusual_tokens_arr, 2)",
)


def _required_canonical_road_blocking_columns(
    road_blocking_rules: tuple[str, ...],
) -> set[str]:
    eligibility_columns = {
        "road_frequency_lte_1000",
        "road_n1_block_size_lte_8",
        "road_n1_block_size_lte_32",
    }
    return {
        column
        for column in eligibility_columns
        if any(f"r.{column}" in rule for rule in road_blocking_rules)
    }


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
        road_blocking_rules: Scalar equality rules to append for candidate
            generation. This derives the road blocking key but does not score it.
            Use ``SELECTIVE_ROAD_BLOCKING_RULES`` for the screened profile.
    """

    # Prediction threshold for initial Splink predict() call
    predict_threshold_match_weight: float = -50

    # Threshold for improve_predictions_using_distinguishing_tokens
    improve_threshold_match_weight: float = -20
    improve_top_n_matches: int = 5
    improve_use_bigrams: bool = True

    # Thresholds for final candidate selection
    final_match_weight_threshold: float = -20.0
    final_distinguishability_threshold: Optional[float] = 0.0

    # Blocking configuration
    include_full_postcode_block: bool = False
    include_outside_postcode_block: bool = True

    # Additional columns to retain through Splink
    additional_columns_to_retain: Optional[list[str]] = field(default=None)

    # Advanced: supply custom Splink settings
    settings: Optional[SettingsCreator] = field(default=None, repr=False)

    # Whether to retain intermediate calculation columns (for debugging)
    retain_intermediate_calculation_columns: bool = False

    road_blocking_rules: tuple[str, ...] = ()
    canonical_road_keys_path: str | None = None
    canonical_road_cardinality_path: str | None = None

    # Populated after find_matches runs — used by MatchResult for inspection
    linker: Any = field(default=None, init=False, repr=False)
    predictions_table: str | None = field(default=None, init=False, repr=False)
    improved_predictions_table: str | None = field(default=None, init=False, repr=False)
    best_matches_table: str | None = field(default=None, init=False, repr=False)
    phase_timings: dict[str, float] = field(
        default_factory=dict, init=False, repr=False
    )

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
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
        from uk_address_matcher.sql_pipeline.helpers import _uid
        from uk_address_matcher.sql_pipeline.match_reasons import MatchReason

        if explain:
            return None

        unmatched_count = df_unmatched.count("*").fetchone()[0]
        if unmatched_count == 0:
            return None

        self.phase_timings = {}
        phase_started = perf_counter()
        if self.road_blocking_rules:
            df_unmatched = add_road_blocking_features(con, df_unmatched)
            if self.canonical_road_cardinality_path is not None:
                df_unmatched = df_unmatched.select(
                    "*, TRUE AS road_n1_block_size_lte_8"
                )
            required_canonical_columns = _required_canonical_road_blocking_columns(
                self.road_blocking_rules
            )
            if self.canonical_road_keys_path is None:
                df_canonical = add_road_blocking_features(con, df_canonical)
            else:
                retained_columns = [
                    column
                    for column in df_unmatched.columns
                    if column not in set(ROAD_FEATURE_COLUMNS).difference({"road_1_norm"})
                ]
                df_unmatched = df_unmatched.select(", ".join(retained_columns))
                escaped_road_keys_path = self.canonical_road_keys_path.replace(
                    "'", "''"
                )
                df_canonical = con.sql(
                    "SELECT canonical.*, road.road_1_norm, road.outward_postcode "
                    f"FROM ({df_canonical.sql_query()}) AS canonical "
                    f"LEFT JOIN read_parquet('{escaped_road_keys_path}') AS road "
                    "USING (ukam_address_id)"
                )
            if self.canonical_road_cardinality_path is not None:
                escaped_cardinality_path = self.canonical_road_cardinality_path.replace(
                    "'", "''"
                )
                df_canonical = con.sql(
                    "SELECT canonical.*, "
                    "coalesce(cardinality.road_n1_block_size <= 8, FALSE) "
                    "AS road_n1_block_size_lte_8 "
                    f"FROM ({df_canonical.sql_query()}) AS canonical "
                    f"LEFT JOIN read_parquet('{escaped_cardinality_path}') "
                    "AS cardinality USING (road_1_norm, numeric_token_1)"
                )
            missing_canonical_columns = sorted(
                required_canonical_columns.difference(df_canonical.columns)
            )
            if missing_canonical_columns:
                raise ValueError(
                    "Selective road blocking requires canonical eligibility fields "
                    f"{missing_canonical_columns}. Re-run canonical preparation."
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
        linker_columns = list(self.additional_columns_to_retain or [])
        linker_columns.extend(range_input_columns)
        linker_columns = list(dict.fromkeys(linker_columns))
        self.phase_timings["road_and_reranker_preparation"] = (
            perf_counter() - phase_started
        )

        # Step 1: Build linker
        phase_started = perf_counter()
        linker = _get_linker(
            df_addresses_to_match=df_unmatched,
            df_addresses_to_search_within=df_canonical,
            con=con,
            include_full_postcode_block=self.include_full_postcode_block,
            include_outside_postcode_block=self.include_outside_postcode_block,
            additional_columns_to_retain=linker_columns or None,
            retain_intermediate_calculation_columns=True,
            settings=self.settings,
            additional_blocking_rules=list(self.road_blocking_rules),
        )

        self.linker = linker
        self.phase_timings["linker_setup"] = perf_counter() - phase_started

        # Step 2: Predict
        phase_started = perf_counter()
        df_predict = linker.inference.predict(
            threshold_match_weight=self.predict_threshold_match_weight
        )
        df_predict_ddb = df_predict.as_duckdbpyrelation()

        prediction_output = project_splink_predictions(
            con,
            df_predict_ddb,
            retain_intermediate_calculation_columns=(
                self.retain_intermediate_calculation_columns
            ),
        )

        table_name = f"__ukam__splink__predictions__{_uid()}"
        con.execute(
            "CREATE OR REPLACE TEMP VIEW "
            + table_name
            + " AS SELECT * FROM ("
            + prediction_output.sql_query()
            + ")"
        )
        self.predictions_table = table_name
        self.phase_timings["raw_prediction"] = perf_counter() - phase_started

        # Step 3: Improve predictions using distinguishing tokens
        phase_started = perf_counter()
        df_improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_predict_ddb,
            con=con,
            match_weight_threshold=self.improve_threshold_match_weight,
            top_n_matches=self.improve_top_n_matches,
            use_bigrams=self.improve_use_bigrams,
            additional_columns_to_retain=[
                column
                for column in linker_columns
                if column not in range_input_columns
                if f"{column}_l" in df_predict_ddb.columns
                and f"{column}_r" in df_predict_ddb.columns
            ]
            or None,
            numeric_range_reranker=numeric_range_reranker,
        )
        df_improved = relation_markers.improve_predictions_using_relation_markers(
            df_predict=df_improved,
            con=con,
        )
        self.improved_predictions_table = getattr(df_improved, "alias", None)
        self.phase_timings["post_linkage_reranking"] = perf_counter() - phase_started

        # Step 4: Compute distinguishability and select best match per record
        # This returns an unmaterialised relation
        phase_started = perf_counter()
        df_best = best_matches_with_distinguishability(
            df_predict=df_improved,
            df_addresses_to_match=df_unmatched,
            con=con,
            best_match_only=False,
        )

        df_best_name = f"__ukam__splink__best_matches__{_uid()}"
        df_best.create(df_best_name)
        self.best_matches_table = df_best_name
        self.phase_timings["best_match_materialisation"] = (
            perf_counter() - phase_started
        )

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

        return con.sql(f"""
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
