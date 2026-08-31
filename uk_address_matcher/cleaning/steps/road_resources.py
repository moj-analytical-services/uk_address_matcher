"""Packaged resources used by production road candidate extraction."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from functools import lru_cache
from importlib.resources import files


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def facility_clause_removal_sql(address_expression: str) -> str:
    """Remove known facility clauses that cannot be emitted as road values."""
    return (
        "trim(regexp_replace(regexp_replace("
        f"{address_expression}, "
        "'SHOPPING CENTRE|INDUSTRIAL ESTATE|INDUSTRIAL PARK', "
        "' ', 'g'), '\\s+', ' ', 'g'))"
    )


def token_pattern(tokens: Sequence[str]) -> str:
    return "(^| )(" + "|".join(re.escape(token) for token in tokens) + ")( |$)"


@lru_cache(maxsize=1)
def token_policy() -> dict[str, object]:
    policy_path = files("uk_address_matcher.data").joinpath(
        "road_candidate_token_policy.json"
    )
    return json.loads(policy_path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def suffix_peel_regex_sql_literal() -> str:
    """Build the established UKAM end-token suffix-chain regex."""
    end_tokens_path = files("uk_address_matcher.data").joinpath(
        "common_uk_end_tokens.json"
    )
    data = json.loads(end_tokens_path.read_text(encoding="utf-8"))
    aliases = data.get("aliases", {}) or {}
    values = [
        *data.get("single_tokens", []),
        *data.get("multi_tokens", []),
        *aliases.keys(),
        *aliases.values(),
    ]
    tokens = sorted(
        {
            " ".join(value.strip().upper().split())
            for value in values
            if isinstance(value, str)
        },
        key=lambda value: (-len(value.split()), -len(value), value),
    )
    escaped = "|".join(re.escape(token).replace(r"\ ", " ") for token in tokens)
    return rf"(?:^|\s+)(?:{escaped})(?:\s+(?:{escaped}))*\s*$".replace(
        "'", "''"
    )
