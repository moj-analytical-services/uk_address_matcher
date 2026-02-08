__version__ = "0.0.4"

from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
    derive_inverted_index,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    SplinkStage,
    SplinkStageConfig,
    StageName,
    TrigramStage,
    available_deterministic_stages,
    run_deterministic_match_pass,
    run_matching,
)
from uk_address_matcher.linking_model.splink_model import get_linker
from uk_address_matcher.post_linkage.accuracy_from_labels import (
    evaluate_predictions_against_labels,
    inspect_match_results_vs_labels,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
    best_matches_with_distinguishability,
    calculate_match_metrics,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

__all__ = [
    # Unified matching API
    "run_matching",
    "StageName",
    "ExactMatchStage",
    "TrigramStage",
    "SplinkStage",
    "SplinkStageConfig",
    # Data preparation
    "prepare_data_for_matching",
    "derive_inverted_index",
    "derive_term_frequencies_table",
    "clean_data_pre_term_frequencies",
    # Analysis
    "calculate_match_metrics",
    "improve_predictions_using_distinguishing_tokens",
    "best_matches_with_distinguishability",
    "best_matches_summary",
    "inspect_match_results_vs_labels",
    "evaluate_predictions_against_labels",
    # Legacy multi-step API (still functional)
    "run_deterministic_match_pass",
    "available_deterministic_stages",
    "get_linker",
]
