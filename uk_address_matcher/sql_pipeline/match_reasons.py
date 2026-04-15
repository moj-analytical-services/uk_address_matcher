from __future__ import annotations

from enum import Enum


class MatchReason(Enum):
    """Canonical set of match reason values shared between Python and DuckDB."""

    EXACT = "exact: full match"
    EXACT_FLAT_RETRACTION = "exact_flat_retraction: match after removing FLAT keyword"
    PEELED_ADDRESS = "peeled_address: match after removing common UK end tokens"
    SPLINK = "splink: probabilistic match"
    UNIQUE_TRIGRAM = "unique_trigram: unique trigram match"

    def __str__(self) -> str:  # pragma: no cover - for convenience only
        return self.value

    @property
    def stage_name(self) -> str:
        """Simplified stage name used in reporting tables."""
        if self is MatchReason.EXACT:
            return "exact_matches"
        if self is MatchReason.EXACT_FLAT_RETRACTION:
            return "exact_flat_retraction"
        if self is MatchReason.PEELED_ADDRESS:
            return "peeled_address"
        if self is MatchReason.SPLINK:
            return "splink"
        if self is MatchReason.UNIQUE_TRIGRAM:
            return "unique_trigram"
        return self.name.lower()

    @classmethod
    def label_for(cls, key: str) -> str:
        """Return the ENUM label for a given short *key*."""

        return cls.from_key(key).value

    @classmethod
    def enum_values(cls) -> tuple[str, ...]:
        """Return values in definition order for use with DuckDB ENUMs."""

        return tuple(member.value for member in cls)
