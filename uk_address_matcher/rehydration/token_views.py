"""Rebuild logical token views from compact canonical storage."""

from __future__ import annotations

from uk_address_matcher.cleaning.steps.token_parsing import (
    _DISTINGUISHING_MARKER_VALUES,
)


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
