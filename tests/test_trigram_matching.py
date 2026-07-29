import duckdb
import pytest

from uk_address_matcher import UniqueTrigramStage
from uk_address_matcher.linking_model.matching.input_filters import (
    _numeric_tokens_from_scalar_columns_sql,
)
from uk_address_matcher.linking_model.matching.runner import _run_matching


@pytest.mark.parametrize(
    ("token_1", "token_2", "token_3", "expected"),
    [
        (None, None, None, []),
        ("12", None, None, ["12"]),
        ("12", "34", None, ["12", "34"]),
        ("12", "34", "56", ["12", "34", "56"]),
        ("1", "2", "3", ["1", "2", "3"]),
        ("82", "83", None, ["82", "83"]),
        ("20", None, None, ["20"]),
    ],
)
def test_numeric_tokens_from_scalar_columns(
    duck_con, token_1, token_2, token_3, expected
):
    relation = duck_con.sql(
        f"""
        SELECT {_numeric_tokens_from_scalar_columns_sql("address")} AS numeric_tokens
        FROM (
            VALUES (
                {"NULL" if token_1 is None else f"'{token_1}'"}::VARCHAR,
                {"NULL" if token_2 is None else f"'{token_2}'"}::VARCHAR,
                {"NULL" if token_3 is None else f"'{token_3}'"}::VARCHAR
            )
        ) AS address(numeric_token_1, numeric_token_2, numeric_token_3)
        """
    )

    assert relation.fetchone()[0] == expected


def _matching_relation(con: duckdb.DuckDBPyConnection, *, address_id: int):
    return con.sql(
        f"""
        SELECT *
        FROM (
            VALUES (
                'M1',
                '12 HIGH STREET LONDON',
                {address_id},
                '12 HIGH STREET LONDON',
                'AA1 1AA',
                '12'::VARCHAR,
                NULL::VARCHAR,
                NULL::VARCHAR,
                FALSE,
                NULL::VARCHAR,
                NULL::VARCHAR,
                NULL::VARCHAR,
                FALSE,
                NULL::VARCHAR,
                NULL::VARCHAR
            )
        ) AS address(
            unique_id,
            original_address_concat,
            ukam_address_id,
            clean_full_address,
            postcode,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id
        )
        """
    )


def test_unique_trigram_matches_canonical_without_numeric_tokens(duck_con):
    messy = _matching_relation(duck_con, address_id=1)
    canonical = duck_con.sql(
        """
        SELECT *
        FROM (VALUES (
            'C1',
            '12 HIGH STREET LONDON',
            '12 HIGH STREET LONDON',
            'AA1 1AA',
            '12'::VARCHAR,
            NULL::VARCHAR,
            NULL::VARCHAR,
            FALSE,
            NULL::VARCHAR,
            NULL::VARCHAR,
            NULL::VARCHAR,
            NULL::VARCHAR,
            FALSE,
            NULL::VARCHAR,
            NULL::VARCHAR,
            100::BIGINT
        )) AS address(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            has_flat_indicator,
            flat_positional,
            sub_premise_location,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    assert "numeric_tokens" not in canonical.columns

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=messy,
        df_canonical_clean=canonical,
        stages=[UniqueTrigramStage()],
    )

    assert results.fetchdf().iloc[0]["resolved_canonical_id"] == "C1"
