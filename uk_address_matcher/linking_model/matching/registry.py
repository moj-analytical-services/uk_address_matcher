from __future__ import annotations

from enum import Enum
from typing import Iterable, Optional, Union

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.stages.exact_match import ExactMatchStage
from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage
from uk_address_matcher.linking_model.matching.stages.trigram import TrigramStage


class StageName(str, Enum):
    """Available matching stages."""

    EXACT_MATCHES = "exact_matches"
    UNIQUE_TRIGRAM = "unique_trigram"
    SPLINK = "splink"


_STAGE_REGISTRY: dict[StageName, MatchingStage] = {
    StageName.EXACT_MATCHES: ExactMatchStage(),
    StageName.UNIQUE_TRIGRAM: TrigramStage(),
    StageName.SPLINK: SplinkStage(),
}

_ALWAYS_ON: tuple[StageName, ...] = (StageName.EXACT_MATCHES,)
_DEFAULT_STAGES: tuple[StageName, ...] = (StageName.EXACT_MATCHES, StageName.SPLINK)


StageNameInput = Union[StageName, str]
StageInput = Union[StageName, str, MatchingStage]


def _stage_name_for_instance(stage: MatchingStage) -> StageName:
    for name, registered in _STAGE_REGISTRY.items():
        if isinstance(stage, registered.__class__):
            return name

    allowed = ", ".join(s.value for s in StageName)
    raise ValueError(f"Unknown stage instance: {stage!r}. Available stages: {allowed}")


def _resolve_stage_name(item: StageNameInput) -> StageName:
    try:
        return item if isinstance(item, StageName) else StageName(item)
    except ValueError as e:
        allowed = ", ".join(s.value for s in StageName)
        raise ValueError(
            f"Unknown matching stage: {item!r}. Available stages: {allowed}"
        ) from e


def _normalise_stage_list(
    stages: Optional[Iterable[StageInput]],
) -> list[StageName | MatchingStage]:
    """Validate and normalise user-provided stages.

    If None, returns _DEFAULT_STAGES.
    Ensures _ALWAYS_ON stages are included and come first.
    """
    if stages is None:
        return list(_DEFAULT_STAGES)

    requested: list[StageName | MatchingStage] = []
    seen: set[StageName] = set()

    for item in stages:
        if isinstance(item, MatchingStage):
            name = _stage_name_for_instance(item)
            if name in seen:
                raise ValueError(f"Duplicate stage specified: {name.value}")
            seen.add(name)
            requested.append(item)
            continue

        name = _resolve_stage_name(item)

        if name in seen:
            raise ValueError(f"Duplicate stage specified: {name.value}")
        seen.add(name)
        requested.append(name)

    # Ensure always-on stages are present and come first
    ordered: list[StageName | MatchingStage] = [
        ao for ao in _ALWAYS_ON if ao not in seen
    ]
    ordered.extend(requested)

    return ordered
