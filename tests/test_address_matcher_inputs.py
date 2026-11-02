import pytest

from uk_address_matcher import AddressMatcher, AddressRecord, ExactMatchStage, SplinkStage
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
)


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
    row = match_result.matches().fetchone()
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


def test_addresses_to_match_without_postcode_column(duck_con, canonical_relation):
    """addresses_to_match with no postcode column should work; postcode is derived
    from address_concat by the pipeline."""
    messy_relation = duck_con.sql(
        """
        SELECT
            10::BIGINT AS unique_id,
            '10 DOWNING STREET SW1A 2AA'::VARCHAR AS address_concat
        """
    )
    _run_matcher(duck_con, canonical_relation, messy_relation)


@pytest.mark.parametrize("dataset", ["canonical", "messy"])
def test_dual_id_columns_are_rejected(duck_con, canonical_relation, dataset):
    both_ids = canonical_relation.project("*, unique_id AS address_id")
    matcher = AddressMatcher(
        both_ids if dataset == "canonical" else canonical_relation,
        both_ids if dataset == "messy" else canonical_relation,
        con=duck_con,
        stages=[ExactMatchStage()],
    )
    with pytest.raises(ValueError) as error:
        matcher.match()
    assert str(error.value) == (
        f"Input {dataset} contained both `address_id` and `unique_id`. "
        "Ensure only one is present, not both."
    )


def test_record_alias_validation_and_legacy_constructor():
    record = AddressRecord("10 DOWNING STREET", None, "M1")
    assert record.address_id == "M1"
    record = AddressRecord.from_dict(
        {"address_id": "M2", "address_concat": "12 HIGH STREET"}
    )
    assert record.postcode is None
    for data in (
        {"address_id": "M1", "unique_id": "M1"},
        {"address_id": "M1", "unique_id": None},
    ):
        with pytest.raises(ValueError, match="Input messy contained both"):
            AddressRecord(address_concat="10 DOWNING STREET", **data)
    with pytest.raises(ValueError, match="address_concat"):
        AddressRecord.from_dict({"address_id": "M1"})


@pytest.mark.parametrize(
    "source_postcode",
    [None, "NOT A POSTCODE"],
    ids=["missing", "invalid"],
)
def test_canonical_without_valid_postcode_still_uses_roads(duck_con, source_postcode):
    postcode_projection = (
        "" if source_postcode is None else f", '{source_postcode}'::VARCHAR AS postcode"
    )
    canonical_relation = duck_con.sql(f"""
        SELECT
            1::BIGINT AS unique_id,
            '12 HIGH STREET'::VARCHAR AS address_concat
            {postcode_projection}
    """)
    messy_relation = duck_con.sql("""
        SELECT
            10::BIGINT AS unique_id,
            '12 HIGH STREET'::VARCHAR AS address_concat
    """)
    matcher = AddressMatcher(
        canonical_addresses=canonical_relation,
        addresses_to_match=messy_relation,
        con=duck_con,
        stages=[SplinkStage()],
        show_progress="off",
    )

    matcher._resolve_canonical_data()

    assert matcher._roadlike_places is not None
    assert matcher._canonical_clean.project("road_1_norm").fetchone() == ("HIGH STREET",)


@pytest.mark.parametrize("missing_column", ["unique_id", "address_concat"])
def test_canonical_relation_missing_required_columns_raises(duck_con, missing_column):
    select_list = {
        "unique_id": "1::BIGINT AS unique_id",
        "address_concat": "'10 DOWNING STREET'::VARCHAR AS address_concat",
        "postcode": "'SW1A 2AA'::VARCHAR AS postcode",
    }
    select_list.pop(missing_column)
    canonical_relation = duck_con.sql(
        "SELECT\n            " + ",\n            ".join(select_list.values())
    )

    messy_relation = duck_con.sql(
        """
        SELECT
            10::BIGINT AS unique_id,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical_relation,
        addresses_to_match=messy_relation,
        con=duck_con,
        stages=[ExactMatchStage()],
    )

    with pytest.raises(ValueError, match="missing required columns"):
        matcher.match()


def test_canonical_relation_precleaned_pre_tf_still_matches(duck_con, canonical_relation):
    precleaned_canonical = clean_data_pre_term_frequencies(
        canonical_relation,
        duck_con,
        num_of_chunks=1,
    )

    messy_relation = duck_con.sql(
        """
        SELECT
            10::BIGINT AS unique_id,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=precleaned_canonical,
        addresses_to_match=messy_relation,
        con=duck_con,
        stages=[ExactMatchStage()],
    )

    match_result = matcher.match()
    row = match_result.matches().fetchone()
    assert row is not None
