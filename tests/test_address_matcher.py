import tempfile
from pathlib import Path

import duckdb
import pyarrow
import pytest

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
    UniqueTrigramStage,
    prepare_canonical_folder,
)
from uk_address_matcher.post_linkage.match_result import MatchResult

CANONICAL_RECORDS = [
    {
        "unique_id": "C1",
        "address_concat": "1 high street london",
        "postcode": "SW1A 1AA",
    },
    {
        "unique_id": "C2",
        "address_concat": "2 low street manchester",
        "postcode": "M1 1AA",
    },
    {
        "unique_id": "C3",
        "address_concat": "3 middle road birmingham",
        "postcode": "B1 1AA",
    },
]

MESSY_RECORDS = [
    {
        "unique_id": "M1",
        "address_concat": "1 high street london",
        "postcode": "SW1A 1AA",
    },
    {
        "unique_id": "M2",
        "address_concat": "2 low st manchester",
        "postcode": "M1 1AA",
    },
]


def _make_addresses(con, records):
    return con.from_arrow(pyarrow.Table.from_pylist(records))


@pytest.fixture
def con():
    return duckdb.connect(database=":memory:")


@pytest.fixture
def canonical_data(con):
    return _make_addresses(con, CANONICAL_RECORDS)


@pytest.fixture
def messy_data(con):
    return _make_addresses(con, MESSY_RECORDS)


def test_match_with_default_stages(con, canonical_data, messy_data):
    """Default stages (ExactMatchStage + SplinkStage) should produce results."""
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
    )
    result = matcher.match()

    assert isinstance(result, MatchResult)
    assert isinstance(result.matches, duckdb.DuckDBPyRelation)
    assert result.matches.count("*").fetchone()[0] > 0


def test_match_with_explicit_stages(con, canonical_data, messy_data):
    """Passing explicit stages should work identically."""
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )
    result = matcher.match()

    assert isinstance(result, MatchResult)
    assert isinstance(result.matches, duckdb.DuckDBPyRelation)
    assert result.matches.count("*").fetchone()[0] > 0


def test_match_result_has_expected_columns(con, canonical_data, messy_data):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )
    result = matcher.match()
    cols = result.matches.columns

    assert "unique_id_l" in cols or "unique_id" in cols
    assert "match_reason" in cols


def test_match_with_custom_splink_stage(con, canonical_data, messy_data):
    """SplinkStage parameters should be passable directly."""
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[
            ExactMatchStage(),
            SplinkStage(
                final_match_weight_threshold=5.0,
                retain_intermediate_calculation_columns=True,
            ),
        ],
    )
    result = matcher.match()
    assert isinstance(result, MatchResult)
    assert isinstance(result.matches, duckdb.DuckDBPyRelation)


def test_match_from_prepared_folder(con, canonical_data, messy_data):
    """Loading canonical data from a prepared folder should work end-to-end."""
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        matcher = AddressMatcher(
            canonical_addresses=tmp,
            addresses_to_match=messy_data,
            con=con,
            stages=[ExactMatchStage()],
        )
        result = matcher.match()

        assert isinstance(result, MatchResult)
        assert isinstance(result.matches, duckdb.DuckDBPyRelation)
        assert result.matches.count("*").fetchone()[0] > 0


def test_match_from_prepared_folder_path_object(con, canonical_data, messy_data):
    """Same as above but passing a Path rather than a string."""
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        matcher = AddressMatcher(
            canonical_addresses=Path(tmp),
            addresses_to_match=messy_data,
            con=con,
            stages=[ExactMatchStage()],
        )
        result = matcher.match()
        assert result.matches.count("*").fetchone()[0] > 0


def test_stage_repr_is_concise_and_informative():
    exact_repr = repr(ExactMatchStage())
    peeled_repr = repr(PeeledAddressStage())
    splink_repr = repr(SplinkStage(final_match_weight_threshold=7.0))
    trigram_repr = repr(UniqueTrigramStage(min_unique_hits=2))

    assert exact_repr.startswith("ExactMatchStage()")
    assert "\n  Purpose:" in exact_repr
    assert "Exact hash-join matching on clean_full_address + postcode" in exact_repr
    assert "\n  Import:  from uk_address_matcher import ExactMatchStage" in exact_repr

    assert peeled_repr.startswith("PeeledAddressStage()")
    assert "\n  Purpose:" in peeled_repr
    assert "peeling common UK locality suffix tokens" in peeled_repr
    assert "\n  Import:  from uk_address_matcher import PeeledAddressStage" in peeled_repr

    assert splink_repr.startswith("SplinkStage(final_match_weight_threshold=7.0)")
    assert "\n  Purpose:" in splink_repr
    assert "Splink probabilistic matching stage" in splink_repr
    assert "\n  Import:  from uk_address_matcher import SplinkStage" in splink_repr

    assert trigram_repr.startswith("UniqueTrigramStage(min_unique_hits=2)")
    assert "\n  Purpose:" in trigram_repr
    assert "unique trigram evidence" in trigram_repr
    assert (
        "\n  Import:  from uk_address_matcher import UniqueTrigramStage" in trigram_repr
    )


def test_available_stages_prints_human_guidance():
    text = str(AddressMatcher.available_stages())

    assert text.startswith("Available matching stages (import from uk_address_matcher):")
    assert "ExactMatchStage" in text
    assert "PeeledAddressStage" in text
    assert "SplinkStage" in text
    assert "UniqueTrigramStage" in text
    assert "Usage:" in text
    assert "from uk_address_matcher import" in text
