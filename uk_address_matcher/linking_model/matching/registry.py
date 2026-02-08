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

# Stages that are not always-on and not Splink (for legacy API)
_NON_SPLINK_OPTIONAL: tuple[StageName, ...] = (StageName.UNIQUE_TRIGRAM,)


StageNameInput = Union[StageName, str]
StageInput = Union[StageName, str, MatchingStage]


def available_deterministic_stages() -> list[StageName]:
    """Get a list of optional non-Splink stages.

    Returns stages that can be enabled via enabled_stage_names
    in the legacy ``run_deterministic_match_pass()`` API.
    EXACT_MATCHES is always on and excluded from this list.
    """
    return list(_NON_SPLINK_OPTIONAL)


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


def _normalise_enabled_stages(
    enabled: Optional[Iterable[StageNameInput]],
) -> list[StageName]:
    """Validate optional stage configuration while preserving order.

    Legacy function used by run_deterministic_match_pass().
    """
    if enabled is None:
        return []

    out: list[StageName] = []
    seen: set[StageName] = set()

    for item in enabled:
        try:
            name = item if isinstance(item, StageName) else StageName(item)
        except ValueError as e:
            allowed = ", ".join(s.value for s in available_deterministic_stages())
            raise ValueError(
                f"Unknown exact matching stage: {item!r}. Available stages: {allowed}"
            ) from e

        if name in _ALWAYS_ON:
            raise ValueError(
                f"{name.value} is always enabled and should not be provided."
            )

        if name in seen:
            raise ValueError(f"Duplicate exact matching stage specified: {name.value}")

        seen.add(name)
        out.append(name)

    return out
