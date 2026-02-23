__version__ = "v1.0.0.dev24"

# === Primary API ===
from uk_address_matcher.address_matcher import AddressMatcher

# === Data preparation helpers ===
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
    derive_inverted_index,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.linking_model.address_record import AddressRecord

# === Matching stages and runner ===
from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
)

# === Post-linkage analysis ===
from uk_address_matcher.post_linkage.accuracy_from_labels import (
    inspect_match_results_vs_labels,
)
from uk_address_matcher.post_linkage.analyse_results import calculate_match_metrics
from uk_address_matcher.prepare_canonical import (
    prepare_canonical_folder,
)

__all__ = [
    # Primary API
    "AddressMatcher",
    "AddressRecord",
    "prepare_canonical_folder",
    # Data preparation helpers
    "prepare_data_for_matching",
    "derive_inverted_index",
    "derive_term_frequencies_table",
    "clean_data_pre_term_frequencies",
    # Matching stages and runner
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
    # Post-linkage analysis
    "calculate_match_metrics",
    "inspect_match_results_vs_labels",
]
