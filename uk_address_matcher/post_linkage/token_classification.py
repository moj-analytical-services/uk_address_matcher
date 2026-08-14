from __future__ import annotations


def _sql_varchar_tuple(tokens: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{token}'" for token in tokens) + ")"


POSITIONAL_TOKENS = ("LEFT", "RIGHT", "CENTRE", "FRONT")

# These tokens describe unit, floor, or layout structure already represented by
# dedicated address comparisons. Rewarding bigrams made only from this class
# would count the same correlated evidence twice.
STRUCTURAL_TOKENS = (
    "FLAT",
    "UNIT",
    "APARTMENT",
    "ROOM",
    "SUITE",
    "FLOOR",
    "GROUND",
    "FIRST",
    "SECOND",
    "THIRD",
    "FOURTH",
    "BASEMENT",
    "LOWER",
    "UPPER",
    "LEFT",
    "RIGHT",
    "CENTRE",
    "FRONT",
    "REAR",
)

# Generic address grammar is not identifying evidence by itself. This broader
# class is used only when asking whether two addresses share a substantive name.
NON_IDENTITY_TOKENS = (
    "A",
    "AN",
    "AND",
    "AT",
    "OF",
    "ON",
    "THE",
    *STRUCTURAL_TOKENS,
    "ROAD",
    "STREET",
    "LANE",
    "AVENUE",
    "DRIVE",
    "CLOSE",
    "WAY",
    "PLACE",
    "TERRACE",
    "GARDENS",
    "COURT",
    "HOUSE",
    "BUILDING",
    "BLOCK",
)

POSITIONAL_TOKENS_SQL = _sql_varchar_tuple(POSITIONAL_TOKENS)
STRUCTURAL_TOKENS_SQL = _sql_varchar_tuple(STRUCTURAL_TOKENS)
NON_IDENTITY_TOKENS_SQL = _sql_varchar_tuple(NON_IDENTITY_TOKENS)


def structural_token_sql(expression: str) -> str:
    """Return SQL identifying numeric, unit, floor, and layout tokens."""
    return (
        f"({expression} IN {STRUCTURAL_TOKENS_SQL} "
        f"OR regexp_full_match({expression}, '[0-9]+'))"
    )


def substantive_identity_token_sql(expression: str) -> str:
    """Return SQL identifying tokens that can carry address identity."""
    return (
        f"({expression} != '' "
        f"AND {expression} NOT IN {NON_IDENTITY_TOKENS_SQL} "
        "AND NOT regexp_full_match("
        f"{expression}, '[0-9]+|[A-Z]?[0-9]+[A-Z]?'))"
    )
