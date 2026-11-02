__version__ = "1.5.3"

# === Primary API ===
from uk_address_matcher.address_matcher import AddressMatcher
from uk_address_matcher.linking_model.address_record import AddressRecord

# === Matching stages and runner ===
from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
)
from uk_address_matcher.post_linkage.match_result import MatchResult
from uk_address_matcher.prepare_canonical import (
    prepare_canonical_folder,
)

__all__ = [
    # Primary API
    "AddressMatcher",
    "AddressRecord",
    "MatchResult",
    "prepare_canonical_folder",
    # Matching stages and runner
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
]
