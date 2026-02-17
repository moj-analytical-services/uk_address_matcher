import pytest

from uk_address_matcher import AddressMatcher, AddressRecord, ExactMatchStage


@pytest.fixture
def canonical_relation(duck_con):
    return duck_con.sql(
        """
        SELECT
            1::BIGINT AS unique_id,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )


def _run_matcher(duck_con, canonical_relation, addresses_to_match):
    matcher = AddressMatcher(
        canonical_addresses=canonical_relation,
        addresses_to_match=addresses_to_match,
        con=duck_con,
        stages=[ExactMatchStage()],
    )
    match_result = matcher.match()
    row = match_result.matches.fetchone()
    assert row is not None


def test_addresses_to_match_duckdb_relation(duck_con, canonical_relation):
    messy_relation = duck_con.sql(
        """
        SELECT
            10::BIGINT AS unique_id,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )

    _run_matcher(duck_con, canonical_relation, messy_relation)


def test_addresses_to_match_address_records(duck_con, canonical_relation):
    records = [
        AddressRecord(
            unique_id="m_1",
            address_concat="10 downing street",
            postcode="SW1A 2AA",
        )
    ]

    _run_matcher(duck_con, canonical_relation, records)


def test_addresses_to_match_dicts(duck_con, canonical_relation):
    records = [
        {
            "unique_id": "m_1",
            "address_concat": "10 downing street",
            "postcode": "SW1A 2AA",
        }
    ]

    _run_matcher(duck_con, canonical_relation, records)
