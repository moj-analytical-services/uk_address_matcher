from __future__ import annotations

from enum import Enum


class MatchReason(Enum):
    """Canonical set of match reason values shared between Python and DuckDB."""

    EXACT = "exact: full match"
    EXACT_WITHOUT_POSTCODE = "exact_without_postcode: full match ignoring postcode"
    SPLINK = "splink: probabilistic match"
    UNIQUE_TRIGRAM = "unique_trigram: unique trigram match"

    def __str__(self) -> str:  # pragma: no cover - for convenience only
        return self.value

    @classmethod
    def label_for(cls, key: str) -> str:
        """Return the ENUM label for a given short *key*."""

        return cls.from_key(key).value

    @classmethod
    def enum_values(cls) -> tuple[str, ...]:
        """Return values in definition order for use with DuckDB ENUMs."""

        return tuple(member.value for member in cls)
