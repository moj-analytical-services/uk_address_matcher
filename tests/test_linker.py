import pytest

from uk_address_matcher.linking_model.splink_model import get_linker
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def resolved_only_matches(duck_con):
    reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (1::BIGINT, 'ADDRESS 1'::VARCHAR, 'POSTCODE 1'::VARCHAR, '{reason}'::VARCHAR, 100::BIGINT, NULL::BIGINT)
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
        )
        """
    )


@pytest.fixture
def unresolved_matches(duck_con):
    reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (2::BIGINT, 'ADDRESS 2'::VARCHAR, 'POSTCODE 2'::VARCHAR, '{reason}'::VARCHAR, NULL::BIGINT, NULL::BIGINT)
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
        )
        """
    )


@pytest.fixture
def canonical_non_empty(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (100::BIGINT, 'CANONICAL 1'::VARCHAR, 'POSTCODE 1'::VARCHAR)
        ) AS t(unique_id, original_address_concat, postcode)
        """
    )


@pytest.fixture
def canonical_empty(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            SELECT
                CAST(NULL AS BIGINT) AS unique_id,
                CAST(NULL AS VARCHAR) AS original_address_concat,
                CAST(NULL AS VARCHAR) AS postcode
        )
        WHERE 1 = 0
        """
    )


def test_get_linker_raises_when_no_unresolved_rows(
    duck_con,
    resolved_only_matches,
    canonical_non_empty,
):
    with pytest.raises(ValueError, match="No unresolved records remain"):
        get_linker(
            df_addresses_to_match=resolved_only_matches,
            df_addresses_to_search_within=canonical_non_empty,
            con=duck_con,
        )


def test_get_linker_raises_when_canonical_empty(
    duck_con,
    unresolved_matches,
    canonical_empty,
):
    with pytest.raises(ValueError, match="Canonical relation is empty"):
        get_linker(
            df_addresses_to_match=unresolved_matches,
            df_addresses_to_search_within=canonical_empty,
            con=duck_con,
        )
