from uk_address_matcher.linking_model.matching.runner import run_matching
from uk_address_matcher.linking_model.matching.stages import (
    DEFAULT_SIGNATURE_TEMPLATES,
    ExactMatchStage,
    MatchingStage,
    PeeledAddressStage,
    SignatureTemplate,
    SplinkStage,
    UniqueSignatureStage,
    UniqueTrigramStage,
)

__all__ = [
    "MatchingStage",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "UniqueSignatureStage",
    "SignatureTemplate",
    "DEFAULT_SIGNATURE_TEMPLATES",
    "PeeledAddressStage",
    "SplinkStage",
    "run_matching",
]
