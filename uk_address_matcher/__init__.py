__version__ = "1.2.4"

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
    DistinguishingTokenStage,
    ExactMatchStage,
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
    "DistinguishingTokenStage",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
]
