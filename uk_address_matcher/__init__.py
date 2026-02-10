__version__ = "0.0.4"

from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
    derive_inverted_index,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.cleaning.steps.trigram_blocking import (
    BIGRAM_STRATEGY,
    DEFAULT_BLOCKING_INDEX_STRATEGIES,
    NUMERIC_PLUS_FIRST_NON_NUMERIC_STRATEGY,
    TRIGRAM_STRATEGY,
    BlockingIndexStrategy,
)
from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
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
    "get_linker",
    "prepare_data_for_matching",
    "derive_inverted_index",
    "derive_term_frequencies_table",
    "clean_data_pre_term_frequencies",
    # Blocking index strategies
    "BlockingIndexStrategy",
    "TRIGRAM_STRATEGY",
    "BIGRAM_STRATEGY",
    "NUMERIC_PLUS_FIRST_NON_NUMERIC_STRATEGY",
    "DEFAULT_BLOCKING_INDEX_STRATEGIES",
    "calculate_match_metrics",
    "improve_predictions_using_distinguishing_tokens",
    "best_matches_with_distinguishability",
    "best_matches_summary",
    "inspect_match_results_vs_labels",
    "evaluate_predictions_against_labels",
    # Matching
    "run_matching",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
]
