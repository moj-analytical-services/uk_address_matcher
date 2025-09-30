__version__ = "1.0.0.dev19"

from uk_address_matcher.cleaning.pipelines import (
    clean_data_on_the_fly,
    clean_data_using_precomputed_rel_tok_freq,
    get_address_token_frequencies_from_address_table,
    get_numeric_term_frequencies_from_address_table,
)
from uk_address_matcher.linking_model.exact_matching import run_deterministic_match_pass
from uk_address_matcher.linking_model.splink_model import get_linker
from uk_address_matcher.post_linkage.accuracy_from_labels import (
    evaluate_predictions_against_labels,
    inspect_match_results_vs_labels,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
    best_matches_with_distinguishability,
    calculate_exact_match_metrics,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

__all__ = [
    "get_linker",
    "clean_data_on_the_fly",
    "clean_data_using_precomputed_rel_tok_freq",
    "get_numeric_term_frequencies_from_address_table",
    "get_address_token_frequencies_from_address_table",
    "calculate_exact_match_metrics",
    "run_deterministic_match_pass",
    "improve_predictions_using_distinguishing_tokens",
    "best_matches_with_distinguishability",
    "best_matches_summary",
    "inspect_match_results_vs_labels",
    "evaluate_predictions_against_labels",
]
