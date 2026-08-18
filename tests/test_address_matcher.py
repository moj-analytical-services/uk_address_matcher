import json
import logging
import re
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
from uk_address_matcher.cleaning.pipelines import _register_inverted_index_table
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


def _make_large_records(count: int, *, prefix: str) -> list[dict[str, str]]:
    return [
        {
            "unique_id": f"{prefix}{index}",
            "address_concat": f"{index} high street london",
            "postcode": "SW1A 1AA",
        }
        for index in range(count)
    ]


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
    assert isinstance(result.matches(), duckdb.DuckDBPyRelation)
    assert result.matches().count("*").fetchone()[0] > 0


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
    assert isinstance(result.matches(), duckdb.DuckDBPyRelation)
    assert result.matches().count("*").fetchone()[0] > 0


def test_stage_timing_logs_at_debug_level(con, canonical_data, messy_data, caplog):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        matcher.match()

    debug_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG
    ]
    timing_messages = [
        message for message in debug_messages if message.startswith("Stage '")
    ]

    assert timing_messages, "Expected at least one stage timing debug message"
    assert any(
        re.fullmatch(r"Stage '.+' completed in \d+m \d{2}s\.", message)
        for message in timing_messages
    )


def test_match_with_relations_from_different_connection(con):
    source_con = duckdb.connect(database=":memory:")
    canonical_data = _make_addresses(source_con, CANONICAL_RECORDS)
    messy_data = _make_addresses(source_con, MESSY_RECORDS)

    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )

    result = matcher.match()

    assert isinstance(result, MatchResult)
    assert result.matches().count("*").fetchone()[0] > 0


def test_match_result_has_expected_columns(con, canonical_data, messy_data):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )
    result = matcher.match()
    cols = result.matches().columns

    assert "unique_id_l" in cols or "unique_id" in cols
    assert "match_reason" in cols


def test_lean_and_debug_prepared_canonical_matching_is_identical(
    con, canonical_data, messy_data, tmp_path
):
    lean_folder = tmp_path / "lean"
    debug_folder = tmp_path / "debug"
    prepare_canonical_folder(
        canonical_data,
        output_folder=lean_folder,
        con=con,
        overwrite=True,
    )
    prepare_canonical_folder(
        canonical_data,
        output_folder=debug_folder,
        con=con,
        add_debug_features=True,
        overwrite=True,
    )

    results = []
    for canonical_folder in (lean_folder, debug_folder):
        variant_con = duckdb.connect(database=":memory:")
        try:
            variant_messy_data = _make_addresses(variant_con, MESSY_RECORDS)
            result = AddressMatcher(
                canonical_addresses=canonical_folder,
                addresses_to_match=variant_messy_data,
                con=variant_con,
            ).match()
            all_matches = result.matches(all_columns=True)
            results.append(
                (
                    set(all_matches.columns),
                    all_matches.select(
                        "unique_id, resolved_canonical_id, "
                        "canonical_ukam_address_id, match_reason, match_weight, "
                        "distinguishability"
                    )
                    .order("unique_id")
                    .fetchall(),
                )
            )
        finally:
            variant_con.close()

    lean_columns, lean_rows = results[0]
    debug_columns, debug_rows = results[1]

    assert lean_rows == debug_rows
    assert {"original_address_concat", "clean_full_address_canonical"} <= lean_columns
    assert "original_address_concat_canonical" not in lean_columns
    assert {
        "original_address_concat",
        "clean_full_address_canonical",
        "original_address_concat_canonical",
    } <= debug_columns


def test_lean_splink_inspection_uses_clean_canonical_address(
    con, canonical_data, messy_data, tmp_path
):
    prepared_folder = tmp_path / "lean"
    prepare_canonical_folder(
        canonical_data,
        output_folder=prepared_folder,
        con=con,
        overwrite=True,
    )

    result = AddressMatcher(
        canonical_addresses=prepared_folder,
        addresses_to_match=messy_data,
        con=con,
    ).match()

    candidates = result._splink_results_for_messy_id("M2")

    assert "original_address_canonical" in candidates.columns
    assert candidates.fetchall()[0][3] == "2 LOW STREET MANCHESTER"


def test_legacy_prepared_canonical_keeps_canonical_raw_result_column(
    con, canonical_data, messy_data, tmp_path
):
    prepared_folder = tmp_path / "legacy"
    prepare_canonical_folder(
        canonical_data,
        output_folder=prepared_folder,
        con=con,
        add_debug_features=True,
        overwrite=True,
    )

    canonical_path = prepared_folder / "ukam_canonical_addresses.parquet"
    legacy_path = prepared_folder / "ukam_canonical_addresses.legacy.parquet"
    con.execute(
        f"""
        COPY (
            SELECT *, []::VARCHAR[] AS very_unusual_tokens_arr
            FROM read_parquet('{canonical_path}')
        ) TO '{legacy_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    canonical_path.unlink()
    legacy_path.rename(canonical_path)

    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("preparation_options")
    manifest_path.write_text(json.dumps(manifest))

    result = AddressMatcher(
        canonical_addresses=prepared_folder,
        addresses_to_match=messy_data,
        con=con,
    ).match()

    assert "original_address_concat_canonical" in result.matches().columns
    assert (
        result.matches()
        .filter("unique_id = 'M2'")
        .select("original_address_concat_canonical")
        .fetchone()[0]
        == "2 low street manchester"
    )
    assert "original_address_concat_canonical" in result.matches(all_columns=True).columns


def test_legacy_prepared_canonical_skips_numeric_range_reranking(
    con, canonical_data, messy_data, tmp_path
):
    prepared_folder = tmp_path / "legacy_without_numeric_range"
    prepare_canonical_folder(
        canonical_data,
        output_folder=prepared_folder,
        con=con,
        overwrite=True,
    )

    canonical_path = prepared_folder / "ukam_canonical_addresses.parquet"
    legacy_path = prepared_folder / "ukam_canonical_addresses.legacy.parquet"
    con.execute(
        f"""
        COPY (
            SELECT * EXCLUDE (
                numeric_range_attributes
            )
            FROM read_parquet('{canonical_path}')
        ) TO '{legacy_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    canonical_path.unlink()
    legacy_path.rename(canonical_path)

    result = AddressMatcher(
        canonical_addresses=prepared_folder,
        addresses_to_match=messy_data,
        con=con,
        stages=[SplinkStage()],
        show_progress=False,
    ).match()

    assert result.matches().count("*").fetchone()[0] > 0


def test_cleaning_num_chunks_is_propagated_to_cleaning_steps(
    con,
    caplog,
):
    canonical_data = _make_addresses(
        con,
        _make_large_records(20_500, prefix="C"),
    )
    messy_data = _make_addresses(
        con,
        _make_large_records(20_500, prefix="M"),
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
        cleaning_num_chunks=2,
    )

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        matcher._resolve_canonical_data()
        matcher._resolve_messy_data()

    info_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.INFO
    ]
    debug_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG
    ]

    cleaned_info_logs = [
        message
        for message in info_messages
        if message.startswith("Cleaning and preprocessing")
    ]
    tf_info_logs = [
        message
        for message in info_messages
        if message.startswith("Applying term frequencies")
    ]
    assert any(
        "Cleaning and preprocessing: 20,500 records across 2 chunks" == message
        for message in cleaned_info_logs
    )
    assert any(
        message.startswith("Cleaning and preprocessing completed:")
        for message in cleaned_info_logs
    )
    assert any(
        "Applying term frequencies: 20,500 records across 2 chunks" == message
        for message in tf_info_logs
    )
    assert any(
        message.startswith("Applying term frequencies completed:")
        for message in tf_info_logs
    )
    assert any("chunk 1/2" in message for message in cleaned_info_logs)
    assert any("chunk 2/2" in message for message in cleaned_info_logs)
    assert any("chunk 1/2" in message for message in tf_info_logs)
    assert any("chunk 2/2" in message for message in tf_info_logs)
    progress_glyphs = ("█", "░", "▕", "▏", "▮", "▯")
    assert all(
        all(glyph not in message for glyph in progress_glyphs)
        for message in info_messages
    )
    assert all(
        all(glyph not in message for glyph in progress_glyphs)
        for message in debug_messages
    )


def test_matcher_progress_stages_logs_boundaries_without_chunk_updates(
    con, canonical_data, messy_data, caplog
):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
        show_progress="stages",
    )

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        matcher._resolve_canonical_data()
        matcher._resolve_messy_data()

    messages = [record.getMessage() for record in caplog.records]

    assert any(message.startswith("Cleaning for TF derivation:") for message in messages)
    assert any(message.startswith("Applying term frequencies:") for message in messages)
    assert not any("chunk 1/" in message for message in messages)


def test_matcher_progress_off_suppresses_stage_status_logs(
    con, canonical_data, messy_data, caplog
):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
        show_progress="off",
    )

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        matcher._resolve_canonical_data()
        matcher._resolve_messy_data()

    stage_prefixes = (
        "Cleaning for TF derivation",
        "Cleaning and preprocessing",
        "Applying term frequencies",
        "Building inverted index",
    )
    assert not any(
        record.getMessage().startswith(stage_prefix)
        and (
            " records across " in record.getMessage()
            or " completed:" in record.getMessage()
        )
        for record in caplog.records
        for stage_prefix in stage_prefixes
    )


def test_matcher_rejects_unknown_progress_mode(con, canonical_data, messy_data):
    with pytest.raises(ValueError, match="show_progress must be one of"):
        AddressMatcher(
            canonical_addresses=canonical_data,
            addresses_to_match=messy_data,
            con=con,
            show_progress="verbose",
        )


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
    assert isinstance(result.matches(), duckdb.DuckDBPyRelation)


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
        assert isinstance(result.matches(), duckdb.DuckDBPyRelation)
        assert result.matches().count("*").fetchone()[0] > 0


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
        assert result.matches().count("*").fetchone()[0] > 0


def test_cleaning_reuses_existing_inverted_index_table(con):
    con.execute(
        """
        CREATE TABLE prepared_inverted_index AS
        SELECT
            'SW1A' AS key,
            ['C1']::VARCHAR[] AS unique_ids,
            'postcode' AS index_strategy
        """
    )

    table_name = _register_inverted_index_table(
        con,
        con.table("prepared_inverted_index"),
    )

    assert table_name == "__ukam_inverted_index"
    tables = {
        row[0]: row[1]
        for row in con.execute(
            "SELECT table_name, table_type FROM information_schema.tables"
        ).fetchall()
    }
    assert tables["prepared_inverted_index"] == "BASE TABLE"
    assert tables["__ukam_inverted_index"] == "VIEW"


def test_inverted_index_property_returns_registered_relation(
    con,
    canonical_data,
    messy_data,
):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )

    assert matcher._inverted_index is None

    matcher._resolve_canonical_data()
    inverted_index = matcher._inverted_index

    assert inverted_index is not None
    assert {"key", "unique_ids", "index_strategy"}.issubset(inverted_index.columns)
    assert inverted_index.count("*").fetchone()[0] > 0


@pytest.mark.parametrize("output_chunk_count", [1, 5])
def test_match_from_prepared_folder_e2e_chunked_output(
    con,
    canonical_data,
    messy_data,
    tmp_path,
    output_chunk_count,
):
    """Prepare canonical artefacts, load from folder path, then run match()."""
    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=output_chunk_count,
        overwrite=True,
    )

    matcher = AddressMatcher(
        canonical_addresses=tmp_path,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage()],
    )

    result = matcher.match()

    assert isinstance(result, MatchResult)
    assert isinstance(result.matches(), duckdb.DuckDBPyRelation)
    assert result.matches().count("*").fetchone()[0] > 0


def test_canonical_address_filter_applies_to_prepared_folder(con):
    canonical_records = [
        {
            "unique_id": "C1",
            "address_concat": "1 high street london",
            "postcode": "SW1A 1AA",
            "classificationcode": "RD06",
            "lowertierlocalauthoritygsscode": "E07000219",
        },
        {
            "unique_id": "C2",
            "address_concat": "2 low street manchester",
            "postcode": "M1 1AA",
            "classificationcode": "RD07",
            "lowertierlocalauthoritygsscode": "E07000219",
        },
    ]
    messy_records = [
        {
            "unique_id": "M1",
            "address_concat": "1 high street london",
            "postcode": "SW1A 1AA",
        },
        {
            "unique_id": "M2",
            "address_concat": "2 low street manchester",
            "postcode": "M1 1AA",
        },
    ]

    canonical_data = _make_addresses(con, canonical_records)
    messy_data = _make_addresses(con, messy_records)

    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp,
            con=con,
            overwrite=True,
        )

        matcher = AddressMatcher(
            canonical_addresses=tmp,
            addresses_to_match=messy_data,
            canonical_address_filter=(
                "classificationcode = 'RD06' "
                "AND lowertierlocalauthoritygsscode = 'E07000219'"
            ),
            con=con,
            stages=[ExactMatchStage()],
        )
        result = matcher.match().matches().order("unique_id")
        rows = result.select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", "C1"), ("M2", None)]


def test_canonical_address_filter_applies_to_relation(con):
    canonical_records = [
        {
            "unique_id": "C1",
            "address_concat": "1 high street london",
            "postcode": "SW1A 1AA",
            "classificationcode": "RD06",
            "lowertierlocalauthoritygsscode": "E07000219",
        },
        {
            "unique_id": "C2",
            "address_concat": "2 low street manchester",
            "postcode": "M1 1AA",
            "classificationcode": "RD07",
            "lowertierlocalauthoritygsscode": "E07000219",
        },
    ]
    messy_records = [
        {
            "unique_id": "M1",
            "address_concat": "1 high street london",
            "postcode": "SW1A 1AA",
        },
        {
            "unique_id": "M2",
            "address_concat": "2 low street manchester",
            "postcode": "M1 1AA",
        },
    ]

    canonical_data = _make_addresses(con, canonical_records)
    messy_data = _make_addresses(con, messy_records)

    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        canonical_address_filter=(
            "classificationcode = 'RD06' AND lowertierlocalauthoritygsscode = 'E07000219'"
        ),
        con=con,
        stages=[ExactMatchStage()],
    )
    result = matcher.match().matches().order("unique_id")
    rows = result.select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", "C1"), ("M2", None)]


def test_stage_repr_is_concise_and_informative():
    exact_repr = repr(ExactMatchStage())
    peeled_repr = repr(PeeledAddressStage())
    splink_repr = repr(SplinkStage(final_match_weight_threshold=7.0))
    trigram_repr = repr(UniqueTrigramStage(min_unique_hits=2))

    assert exact_repr.startswith("ExactMatchStage()")
    assert "\n  Purpose:" in exact_repr
    assert (
        "Deterministic exact matching on `clean_full_address` and `postcode`."
        in exact_repr
    )  # noqa: E501
    assert "\n  Import:  from uk_address_matcher import ExactMatchStage" in exact_repr

    assert peeled_repr.startswith("PeeledAddressStage()")
    assert "\n  Purpose:" in peeled_repr
    assert (
        "Deterministic matching after peeling common UK locality suffixes." in peeled_repr
    )  # noqa: E501
    assert "\n  Import:  from uk_address_matcher import PeeledAddressStage" in peeled_repr

    assert splink_repr.startswith("SplinkStage(final_match_weight_threshold=7.0)")
    assert "\n  Purpose:" in splink_repr
    assert "Probabilistic matching stage built on Splink." in splink_repr
    assert "\n  Import:  from uk_address_matcher import SplinkStage" in splink_repr

    assert trigram_repr.startswith("UniqueTrigramStage(min_unique_hits=2)")
    assert "\n  Purpose:" in trigram_repr
    assert (
        "Deterministic matching using n-grams that identify one canonical row."
        in trigram_repr
    )  # noqa: E501
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


def _normalise_hash_suffix(name: str) -> str:
    """Normalise dynamic hash-like suffixes used in DuckDB temp object names."""
    return re.sub(r"_[a-z0-9]{8,32}$", "", name)


def test_matching_does_not_leak_unnamed_relations(con, canonical_data, messy_data):
    matcher = AddressMatcher(
        canonical_addresses=canonical_data,
        addresses_to_match=messy_data,
        con=con,
        stages=[ExactMatchStage(), SplinkStage()],
    )
    matcher.match()

    table_names = [
        row[0] for row in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    ]

    unnamed_relations = [
        name for name in table_names if re.fullmatch(r"unnamed_relation_[0-9a-f]+", name)
    ]
    assert not unnamed_relations, (
        "Unexpected unnamed_relation tables leaked into the connection: "
        f"{sorted(unnamed_relations)}"
    )

    splink_numeric_tf = [
        name for name in table_names if name.startswith("__splink__df_tf_numeric_token_")
    ]
    normalised = {_normalise_hash_suffix(name) for name in splink_numeric_tf}
    assert len(normalised) == len(splink_numeric_tf), (
        "Splink numeric TF table signatures collided after hash normalisation: "
        f"raw={sorted(splink_numeric_tf)}, normalised={sorted(normalised)}"
    )

    root_tables = [name for name in table_names if re.fullmatch(r"root_[a-z0-9]+", name)]
    assert len(root_tables) <= 2, (
        "Expected at most two root_* tables for canonical/messy inputs, "
        f"found {len(root_tables)}: {sorted(root_tables)}"
    )

    processed_tables = [
        name
        for name in table_names
        if re.fullmatch(r"(?:__ukam__|ukam__)processed_(messy|canonical)_[a-z0-9]+", name)
    ]
    assert len(processed_tables) == 2, (
        "Expected one processed table each for messy/canonical inputs, "
        f"found {len(processed_tables)}: {sorted(processed_tables)}"
    )

    legacy_processed_tables = [
        name for name in table_names if name.startswith("__ukam_addresses_processed_")
    ]
    assert not legacy_processed_tables, (
        "Legacy processed table names should no longer be created: "
        f"{sorted(legacy_processed_tables)}"
    )
