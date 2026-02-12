from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.stages.exact_match import ExactMatchStage
from uk_address_matcher.linking_model.matching.stages.peeled import PeeledAddressStage
from uk_address_matcher.linking_model.matching.stages.signature import (
    DEFAULT_SIGNATURE_TEMPLATES,
    SignatureTemplate,
    UniqueSignatureStage,
)
from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage
from uk_address_matcher.linking_model.matching.stages.trigram import UniqueTrigramStage

__all__ = [
    "MatchingStage",
    "ExactMatchStage",
    "UniqueTrigramStage",
    "UniqueSignatureStage",
    "SignatureTemplate",
    "DEFAULT_SIGNATURE_TEMPLATES",
    "PeeledAddressStage",
    "SplinkStage",
]
