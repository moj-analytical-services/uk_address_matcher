from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage

if TYPE_CHECKING:
    import duckdb

    from splink import SettingsCreator

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True)
class SplinkStage(MatchingStage):
    """Splink probabilistic matching stage.

    Encapsulates the full Splink pipeline:

    1. ``_get_linker()`` — builds the Splink Linker
    2. ``linker.inference.predict()`` — generates pairwise predictions
    3. ``improve_predictions_using_distinguishing_tokens()`` — refines scores
    4. ``best_matches_with_distinguishability()`` — picks the best candidate
    5. Threshold filtering to select the top match per messy record

    ``find_matches()`` returns a relation with the standard match columns
    plus ``match_weight`` and ``distinguishability``.
    """

    # Prediction threshold for initial Splink predict() call
    predict_threshold_match_weight: float = -50

    # Threshold for improve_predictions_using_distinguishing_tokens
    improve_threshold_match_weight: float = -20
    improve_top_n_matches: int = 5
    improve_use_bigrams: bool = True

    # Thresholds for final candidate selection
    final_match_weight_threshold: float = 10.0
    final_distinguishability_threshold: Optional[float] = 5.0

    # Blocking configuration
    include_full_postcode_block: bool = True
    include_outside_postcode_block: bool = True

    # Additional columns to retain through Splink
    additional_columns_to_retain: Optional[list[str]] = field(default=None)

    # Advanced: supply custom Splink settings
    settings: Optional[SettingsCreator] = field(default=None, repr=False)

    # Whether to retain intermediate calculation columns (for debugging)
    retain_intermediate_calculation_columns: bool = False

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
        from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
            improve_predictions_using_distinguishing_tokens,
        )
        from uk_address_matcher.sql_pipeline.match_reasons import MatchReason

        if explain:
            return None

        unmatched_count = df_unmatched.count("*").fetchone()[0]
        if unmatched_count == 0:
            return None

        # Step 1: Build linker
        linker = _get_linker(
            df_addresses_to_match=df_unmatched,
            df_addresses_to_search_within=df_canonical,
            con=con,
            include_full_postcode_block=self.include_full_postcode_block,
            include_outside_postcode_block=self.include_outside_postcode_block,
            additional_columns_to_retain=self.additional_columns_to_retain,
            retain_intermediate_calculation_columns=(
                self.retain_intermediate_calculation_columns
            ),
            settings=self.settings,
        )

        # Step 2: Predict
        df_predict = linker.inference.predict(
            threshold_match_weight=self.predict_threshold_match_weight
        )
        df_predict_ddb = df_predict.as_duckdbpyrelation()

        # Step 3: Improve predictions using distinguishing tokens
        df_improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_predict_ddb,
            con=con,
            match_weight_threshold=self.improve_threshold_match_weight,
            top_n_matches=self.improve_top_n_matches,
            use_bigrams=self.improve_use_bigrams,
            additional_columns_to_retain=self.additional_columns_to_retain,
        )

        # Step 4: Compute distinguishability and select best match per record
        df_best = best_matches_with_distinguishability(
            df_predict=df_improved,
            df_addresses_to_match=df_unmatched,
            con=con,
            best_match_only=True,
        )

        # Step 5: Apply thresholds and project to standard columns
        splink_label = MatchReason.SPLINK.value

        dist_filter = ""
        if self.final_distinguishability_threshold is not None:
            dist_filter = (
                "AND (distinguishability IS NULL "
                f"OR distinguishability >= {self.final_distinguishability_threshold})"
            )

        return con.sql(f"""
            SELECT
                ukam_address_id_r AS ukam_address_id,
                unique_id_l AS resolved_canonical_id,
                ukam_address_id_l AS canonical_ukam_address_id,
                '{splink_label}' AS match_reason,
                match_weight,
                distinguishability
            FROM ({df_best.sql_query()})
            WHERE match_weight >= {self.final_match_weight_threshold}
            {dist_filter}
            AND unique_id_l IS NOT NULL
        """)
