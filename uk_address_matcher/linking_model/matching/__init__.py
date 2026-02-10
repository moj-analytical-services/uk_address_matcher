from uk_address_matcher.linking_model.matching.runner import run_matching
from uk_address_matcher.linking_model.matching.stages import (
    ExactMatchStage,
    MatchingStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
)

__all__ = [
    "MatchingStage",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "SplinkStage",
    "run_matching",
]
