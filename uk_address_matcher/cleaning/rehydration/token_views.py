"""Rebuild logical token views from compact canonical storage."""

from __future__ import annotations

from duckdb import DuckDBPyRelation

from uk_address_matcher.cleaning.steps.token_parsing import (
    _DISTINGUISHING_MARKER_VALUES,
)


# TODO(ThomasHepworth): remove these views when we release v2.0.0 with a warning to users
def _clean_full_address_tokens_expression(
    address_column: str = "clean_full_address",
    token_column: str = "clean_full_address_tokens",
) -> str:
    return f"string_split({address_column}, ' ')::VARCHAR[] AS {token_column}"


def _clean_full_address_expression(
    token_column: str = "clean_full_address_tokens",
    address_column: str = "clean_full_address",
) -> str:
    return f"array_to_string({token_column}, ' ') AS {address_column}"


def _ensure_clean_full_address_views(
    relation: DuckDBPyRelation,
) -> DuckDBPyRelation:
    """Ensure the cleaned address string and token array are both available."""
    columns = set(relation.columns)
    has_address = "clean_full_address" in columns
    has_tokens = "clean_full_address_tokens" in columns
    if not has_address and not has_tokens:
        return relation

    if not has_address:
        relation = relation.select(f"*, {_clean_full_address_expression()}")
        columns = set(relation.columns)

    if "clean_full_address_tokens" not in columns:
        relation = relation.select(f"*, {_clean_full_address_tokens_expression()}")

    return relation


def _ensure_sub_premise_views(relation: DuckDBPyRelation) -> DuckDBPyRelation:
    required = {
        "sub_premise_role",
        "sub_premise_identifier",
        "sub_premise_marker_token",
        "flat_positional",
    }
    if not required.issubset(relation.columns):
        return relation
    flat_parts = [
        "CASE WHEN sub_premise_role = 'FLAT' THEN "
        f"nullif(regexp_extract(sub_premise_identifier, '{pattern}', 0), '') "
        f"END AS {column}"
        for column, pattern in (("flat_number", "^[0-9]+"), ("flat_letter", "[A-Z]$"))
        if column not in relation.columns
    ]
    if flat_parts:
        relation = relation.select("*, " + ", ".join(flat_parts))
    business_marker = (
        "sub_premise_marker_token IN "
        "('UNIT', 'SUITE', 'OFFICE', 'WORKSHOP', 'WAREHOUSE', 'STUDIO')"
    )
    business_identifier = (
        "sub_premise_explicit_identifier"
        if "sub_premise_explicit_identifier" in relation.columns
        else "sub_premise_identifier"
    )
    expressions = {
        "business_unit_type": (
            f"CASE WHEN {business_marker} THEN sub_premise_marker_token END"
        ),
        "business_unit_id": (
            f"CASE WHEN {business_marker} THEN {business_identifier} END"
        ),
        "has_business_unit": f"coalesce({business_marker}, false)",
        "has_flat_indicator": (
            "flat_number IS NOT NULL OR flat_letter IS NOT NULL "
            "OR flat_positional IS NOT NULL "
            "OR list_has_any(clean_full_address_tokens, ['FLAT', 'FFLAT'])"
        ),
        "flat_identity": (
            "CASE WHEN flat_number IS NOT NULL "
            "OR flat_letter IS NOT NULL OR flat_positional IS NOT NULL "
            "THEN concat_ws('_', coalesce(flat_number, ''), coalesce(flat_letter, ''), "
            "coalesce(flat_positional, '')) END"
        ),
        "flat_floor_prefixes": (
            "list_distinct(regexp_extract_all(regexp_extract("
            "array_to_string(clean_full_address_tokens, ' '), '^[^0-9]*', 0), "
            "'BASEMENT|GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH', 0))"
        ),
    }
    missing = [
        f"{expression} AS {column}"
        for column, expression in expressions.items()
        if column not in relation.columns
    ]
    return relation.select("*, " + ", ".join(missing)) if missing else relation


def _distinguishing_lexical_tokens_expression(
    token_column: str = "distinguishing_adj_start_tokens",
) -> str:
    marker_values = ", ".join(f"'{value}'" for value in _DISTINGUISHING_MARKER_VALUES)
    return f"""
        list_filter(
            {token_column},
            (token, position) -> NOT (
                token IN ({marker_values})
                OR (
                    position > 1
                    AND list_extract({token_column}, position - 1)
                        IN ({marker_values})
                    AND regexp_matches(
                        token,
                        '^[A-Z]?[0-9]{{1,4}}[A-Z]?$|^[A-Z]$'
                    )
                )
            )
        ) AS distinguishing_lexical_tokens
    """


def _distinguishing_token_parts_view_expressions(
    parts_column: str = "distinguishing_token_parts",
) -> dict[str, str]:
    return {
        "distinguishing_adj_start_tokens": (
            f"list_transform({parts_column}, part -> part.token) "
            "AS distinguishing_adj_start_tokens"
        ),
        "distinguishing_lexical_tokens": (
            f"list_transform(list_filter({parts_column}, part -> part.is_lexical), "
            "part -> part.token) AS distinguishing_lexical_tokens"
        ),
    }
