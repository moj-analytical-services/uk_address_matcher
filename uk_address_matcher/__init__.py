__version__ = "v1.0.1"

# === Primary API ===
from uk_address_matcher.address_matcher import AddressMatcher

# === Data preparation helpers ===
from uk_address_matcher.cleaning.chunking_strategies import (
    prepare_data_for_matching,
)
from uk_address_matcher.datasets import ukam_datasets
from uk_address_matcher.linking_model.address_record import AddressRecord

# === Matching stages and runner ===
from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    NgramJaccardStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
)
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
    "ukam_datasets",
    # Matching stages and runner
    "ExactMatchStage",
    "NgramJaccardStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
]
