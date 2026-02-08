from __future__ import annotations

from uk_address_matcher.linking_model.matching.registry import StageName
from uk_address_matcher.linking_model.matching.runner import run_matching
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.stages.exact_match import ExactMatchStage
from uk_address_matcher.linking_model.matching.stages.splink import (
    SplinkStage,
    SplinkStageConfig,
)
from uk_address_matcher.linking_model.matching.stages.trigram import TrigramStage

__all__ = [
    # Primary API
    "run_matching",
    "StageName",
    "MatchingStage",
    "ExactMatchStage",
    "TrigramStage",
    "SplinkStage",
    "SplinkStageConfig",
]
