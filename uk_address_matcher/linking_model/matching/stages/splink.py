from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage

if TYPE_CHECKING:
    import duckdb
    from splink import SettingsCreator

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


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

    # Populated after find_matches runs — used by MatchResult for inspection
    linker: Any = field(default=None, init=False, repr=False)
    predictions_table: str | None = field(default=None, init=False, repr=False)
    improved_predictions_table: str | None = field(default=None, init=False, repr=False)
    best_matches_table: str | None = field(default=None, init=False, repr=False)

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        """Score and select canonical matches in two stages.

        Stage 1 (Splink probabilistic scoring). Build a Splink linker over the
        unmatched messy records and the canonical gazetteer, use postcode-based
        blocking to generate candidate pairs, and score every pair with the
        trained probabilistic model. The output is a ``match_weight`` for each
        (messy, canonical) candidate pair.

        Stage 2 (distinguishing-token reranking and selection). Keep the top
        ``improve_top_n_matches`` candidates per messy record and adjust each
        candidate's ``match_weight`` using token-level evidence drawn from that
        shortlist: reward shared tokens that are rare across the shortlist,
        punish tokens the messy record carries that this candidate lacks but a
        rival candidate supplies, and apply a structured penalty when sub-premise
        positional descriptors (for example LEFT versus RIGHT) conflict.
        ``distinguishability`` (the match-weight gap to the next-best candidate)
        is then recomputed, the top-ranked candidate per messy record is
        selected, and it is emitted only if it clears
        ``final_match_weight_threshold`` and
        ``final_distinguishability_threshold``.
        """
        from uk_address_matcher.linking_model.splink_model import _get_linker
        from uk_address_matcher.post_linkage.analyse_results import (
            best_matches_with_distinguishability,
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

        # Stage 1 - Splink probabilistic scoring.
        # Build a linker over the unmatched messy records and the canonical
        # gazetteer. Postcode-based blocking proposes candidate pairs and the
        # trained model scores each pair, producing a match_weight per pair.
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

        self.linker = linker

        # Score every blocked candidate pair and keep those above the permissive
        # predict threshold so Stage 2 has a full shortlist to rerank.
        df_predict = linker.inference.predict(
            threshold_match_weight=self.predict_threshold_match_weight
        )
        df_predict_ddb = df_predict.as_duckdbpyrelation()

        table_name = f"__ukam__splink__predictions__{_uid()}"
        con.execute(
            "CREATE OR REPLACE TEMP VIEW "
            + table_name
            + " AS SELECT * FROM ("
            + df_predict_ddb.sql_query()
            + ")"
        )
        self.predictions_table = table_name

        # Stage 2 - distinguishing-token reranking and selection.
        # Take the top-N candidates per messy record and adjust each
        # candidate's match_weight using token-level evidence from the
        # shortlist (rewarding rare shared tokens, punishing tokens a rival
        # candidate explains better, and penalising positional conflicts).
        df_improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_predict_ddb,
            con=con,
            match_weight_threshold=self.improve_threshold_match_weight,
            top_n_matches=self.improve_top_n_matches,
            use_bigrams=self.improve_use_bigrams,
            additional_columns_to_retain=self.additional_columns_to_retain,
        )
        self.improved_predictions_table = getattr(df_improved, "alias", None)

        # Recompute distinguishability (the match-weight gap to the next-best
        # candidate) on the reranked weights and rank candidates per record.
        # This returns an unmaterialised relation.
        df_best = best_matches_with_distinguishability(
            df_predict=df_improved,
            df_addresses_to_match=df_unmatched,
            con=con,
            best_match_only=False,
        )

        df_best_name = f"__ukam__splink__best_matches__{_uid()}"
        df_best.create(df_best_name)
        self.best_matches_table = df_best_name

        # Emit the top-ranked candidate per record, keeping only matches that
        # clear the final match-weight and distinguishability thresholds.
        splink_label = MatchReason.SPLINK.value

        dist_filter = ""
        if self.final_distinguishability_threshold is not None:
            dist_filter = (
                "AND (distinguishability IS NULL "
                f"OR distinguishability >= {self.final_distinguishability_threshold})"
            )

        return con.sql(f"""
            SELECT
                best_match.ukam_address_id_r AS ukam_address_id,
                best_match.unique_id_l AS resolved_canonical_id,
                best_match.ukam_address_id_l AS canonical_ukam_address_id,
                '{splink_label}' AS match_reason,
                best_match.match_weight,
                best_match.distinguishability
            FROM (
                SELECT *
                FROM {df_best_name}
                WHERE candidate_rank = 1
            ) AS best_match
            WHERE best_match.match_weight >= {self.final_match_weight_threshold}
            {dist_filter}
            AND best_match.unique_id_l IS NOT NULL
        """)
