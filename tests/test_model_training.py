from __future__ import annotations

from uk_address_matcher.cleaning.steps.road_resources import (
    facility_clause_removal_sql,
    suffix_peel_regex_sql_literal,
    token_policy,
)


def test_road_candidate_resources_are_packaged() -> None:
    policy = token_policy()

    assert policy["road_syntax_terminal_tokens"]
    assert policy["residence_or_non_road_any_token"]
    assert "INDUSTRIAL ESTATE" in facility_clause_removal_sql("address")
    assert suffix_peel_regex_sql_literal()
