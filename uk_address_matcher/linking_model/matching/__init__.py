from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.matching.stages import (
    ExactMatchStage,
    MatchingStage,
    PeeledAddressStage,
    PostcodeFreeMatchStage,
    SplinkStage,
    UniqueTrigramStage,
)

__all__ = [
    "MatchingStage",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "PeeledAddressStage",
    "PostcodeFreeMatchStage",
    "SplinkStage",
    "_run_matching",
]
