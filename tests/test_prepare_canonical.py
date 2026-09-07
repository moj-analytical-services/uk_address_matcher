import io
import json
import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pyarrow
import pyarrow.parquet as pyarrow_parquet
import pytest

from uk_address_matcher import prepare_canonical_folder
from uk_address_matcher.cleaning import chunking_strategies
from uk_address_matcher.logging import progress as progress_helpers
from uk_address_matcher.logging.progress import _ProgressBar
from uk_address_matcher.prepare_canonical import (
    MAX_CHUNK_COUNT,
    _coerce_prepare_input_to_relation,
    _PreparedCanonical,
    load_prepared_canonical_data,
)

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


class _FakeStream(io.StringIO):
    def __init__(self, *, isatty_value: bool) -> None:
        super().__init__()
        self._isatty_value = isatty_value

    def isatty(self) -> bool:
        return self._isatty_value


class _AsciiStream(_FakeStream):
    @property
    def encoding(self) -> str:
        return "ascii"


class _BrokenWriteStream(_FakeStream):
    def write(self, s: str) -> int:
        raise UnicodeEncodeError("ascii", s, 0, 1, "cannot encode")


def _write_raw_canonical_csv(path: Path, records: list[dict[str, str]]) -> None:
    rows = ["unique_id,address_concat,postcode"]
    for record in records:
        rows.append(
            f"{record['unique_id']},{record['address_concat']},{record['postcode']}"
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _fake_relation(*, columns: list[str], row_count: int) -> MagicMock:
    relation = MagicMock()
    relation.columns = columns
    relation.count.return_value.fetchone.return_value = (row_count,)
    return relation


@pytest.fixture
def con():
    return duckdb.connect(database=":memory:")


@pytest.fixture
def canonical_data(con):
    return con.from_arrow(pyarrow.Table.from_pylist(CANONICAL_RECORDS))


@pytest.fixture
def prepared_folder(con, canonical_data, tmp_path):
    """A ready-made prepared folder for tests that only need to read."""
    prepare_canonical_folder(
        canonical_data, output_folder=tmp_path, con=con, overwrite=True
    )
    return tmp_path


def test_prepare_creates_expected_files(prepared_folder):
    assert (prepared_folder / "ukam_canonical_addresses.parquet").exists()
    assert (prepared_folder / "ukam_term_frequencies.parquet").exists()
    assert (prepared_folder / "ukam_inverted_index.parquet").exists()
    assert (prepared_folder / "ukam_manifest.json").exists()


def test_progress_bar_disabled_writes_nothing():
    stream = _FakeStream(isatty_value=True)
    progress = _ProgressBar(label="Testing", total=10, enabled=False, stream=stream)

    progress.update(5)
    progress.close()

    assert stream.getvalue() == ""


def test_progress_bar_enabled_writes_carriage_return_and_newline():
    stream = _FakeStream(isatty_value=True)
    progress = _ProgressBar(
        label="Testing",
        total=10,
        total_units=4,
        enabled=True,
        stream=stream,
    )

    progress.update(5, completed_units=2)
    progress.close()

    output = stream.getvalue()
    assert "\rTesting:  50% ▕" in output
    assert "▮" * 12 in output
    assert "▯" * 12 in output
    assert "5/10 records" in output
    assert output.endswith("\n")


def test_progress_bar_caps_progress_at_total():
    stream = _FakeStream(isatty_value=True)
    progress = _ProgressBar(
        label="Testing",
        total=10,
        total_units=4,
        enabled=True,
        stream=stream,
    )

    progress.update(25, completed_units=8)

    assert "10/10 records" in stream.getvalue()
    assert f"▕{'▮' * 24}▏" in stream.getvalue()


def test_progress_bar_disables_live_output_for_non_tty_stream():
    stream = _FakeStream(isatty_value=False)
    progress = _ProgressBar(label="Testing", total=10, stream=stream)

    progress.update(5)

    assert progress.enabled is False
    assert stream.getvalue() == ""


def test_progress_bar_disables_itself_when_stream_cannot_render():
    stream = _BrokenWriteStream(isatty_value=True)
    progress = _ProgressBar(
        label="Testing",
        total=10,
        total_units=4,
        enabled=True,
        stream=stream,
    )

    progress.update(5, completed_units=2)
    progress.close()

    assert progress.enabled is False


def test_progress_bar_ascii_fallback_uses_plain_glyphs():
    stream = _AsciiStream(isatty_value=True)
    progress = _ProgressBar(
        label="Testing",
        total=10,
        total_units=4,
        enabled=True,
        stream=stream,
    )

    progress.update(5, completed_units=2)
    progress.close()

    output = stream.getvalue()
    assert "[" in output and "]" in output
    assert "#" in output and "-" in output
    assert "▕" not in output and "▮" not in output


def test_progress_bar_close_without_render_writes_nothing():
    stream = _FakeStream(isatty_value=True)
    progress = _ProgressBar(label="Testing", total=10, enabled=True, stream=stream)

    progress.close()

    assert stream.getvalue() == ""


def test_progress_bar_clears_leftover_characters_on_shorter_render():
    stream = _FakeStream(isatty_value=True)
    progress = _ProgressBar(
        label="Testing",
        total=10,
        total_units=4,
        enabled=True,
        stream=stream,
    )

    progress.update(10, completed_units=4)
    progress.update(1, completed_units=1)

    output = stream.getvalue()
    assert "\rTesting: 100%" in output
    assert "\rTesting:  10%" in output
    assert output.endswith(" " * 2) or "  " in output.split("\r")[-1]


def test_prepare_accepts_local_csv_path_input(con, tmp_path):
    input_csv = tmp_path / "canonical.csv"
    _write_raw_canonical_csv(input_csv, CANONICAL_RECORDS)

    prepare_canonical_folder(input_csv, output_folder=tmp_path / "prepared", con=con)

    assert (tmp_path / "prepared" / "ukam_canonical_addresses.parquet").exists()
    assert (tmp_path / "prepared" / "ukam_term_frequencies.parquet").exists()
    assert (tmp_path / "prepared" / "ukam_inverted_index.parquet").exists()
    assert (tmp_path / "prepared" / "ukam_manifest.json").exists()


def test_prepare_accepts_list_of_local_csv_paths(con, tmp_path):
    first_csv = tmp_path / "canonical_part_1.csv"
    second_csv = tmp_path / "canonical_part_2.csv"
    _write_raw_canonical_csv(first_csv, CANONICAL_RECORDS[:2])
    _write_raw_canonical_csv(second_csv, CANONICAL_RECORDS[2:])

    prepare_canonical_folder(
        [first_csv, second_csv],
        output_folder=tmp_path / "prepared",
        con=con,
        output_chunk_count=2,
    )

    chunk_files = sorted(
        (tmp_path / "prepared" / "ukam_canonical_addresses_chunks").glob("*.parquet")
    )
    assert len(chunk_files) == 2
    assert (tmp_path / "prepared" / "ukam_manifest.json").exists()


def test_prepare_can_create_chunked_canonical_output(con, canonical_data, tmp_path):
    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=3,
        overwrite=True,
    )

    chunk_dir = tmp_path / "ukam_canonical_addresses_chunks"
    chunk_files = sorted(chunk_dir.glob("*.parquet"))

    assert chunk_dir.exists()
    assert len(chunk_files) == 3
    assert chunk_files[0].name == "canonical_addresses_chunk_00001_of_00003.parquet"
    assert chunk_files[1].name == "canonical_addresses_chunk_00002_of_00003.parquet"
    assert chunk_files[2].name == "canonical_addresses_chunk_00003_of_00003.parquet"
    assert not (tmp_path / "ukam_canonical_addresses.parquet").exists()


@pytest.mark.parametrize("output_chunk_count", [1, 3])
@pytest.mark.parametrize("add_debug_features", [False, True])
def test_prepared_canonical_schema_matches_debug_option(
    con,
    canonical_data,
    tmp_path,
    output_chunk_count,
    add_debug_features,
):
    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=output_chunk_count,
        add_debug_features=add_debug_features,
        overwrite=True,
    )

    if output_chunk_count == 1:
        canonical_paths = [tmp_path / "ukam_canonical_addresses.parquet"]
    else:
        canonical_paths = sorted(
            (tmp_path / "ukam_canonical_addresses_chunks").glob("*.parquet")
        )

    canonical_relation = con.read_parquet([str(path) for path in canonical_paths])
    columns = set(canonical_relation.columns)
    manifest = json.loads((tmp_path / "ukam_manifest.json").read_text())

    assert "very_unusual_tokens_arr" not in columns
    assert ("original_address_concat" in columns) is add_debug_features
    assert "clean_full_address" in columns
    assert canonical_relation.count("*").fetchone()[0] == 3
    assert manifest["preparation_options"] == {"add_debug_features": add_debug_features}

    for canonical_path in canonical_paths:
        relative_name = str(canonical_path.relative_to(tmp_path))
        physical_columns = set(con.read_parquet(str(canonical_path)).columns)
        assert set(manifest["files"][relative_name]["columns"]) == physical_columns


def test_old_manifest_without_preparation_options_still_loads(con, prepared_folder):
    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("preparation_options")
    manifest_path.write_text(json.dumps(manifest))

    loaded = load_prepared_canonical_data(prepared_folder, con=con)

    assert loaded.addresses.count("*").fetchone()[0] == 3


def test_prepare_show_progress_false_suppresses_live_output(
    con, canonical_data, tmp_path, monkeypatch
):
    stream = _FakeStream(isatty_value=True)
    monkeypatch.setattr(progress_helpers.sys, "stderr", stream)

    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path / "prepared",
        con=con,
        overwrite=True,
        show_progress=False,
    )

    assert stream.getvalue() == ""


def test_prepare_progress_stages_logs_boundaries_without_chunk_updates(
    con, canonical_data, tmp_path, monkeypatch, caplog
):
    stream = _FakeStream(isatty_value=True)
    monkeypatch.setattr(progress_helpers.sys, "stderr", stream)

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path / "prepared",
            con=con,
            overwrite=True,
            show_progress="stages",
        )

    messages = [record.getMessage() for record in caplog.records]

    assert any(message.startswith("Cleaning and preprocessing:") for message in messages)
    assert any(
        message.startswith("Cleaning and preprocessing completed:")
        for message in messages
    )
    assert not any("chunk 1/" in message for message in messages)
    assert stream.getvalue() == ""


def test_prepare_progress_off_suppresses_stage_status_logs(
    con, canonical_data, tmp_path, caplog
):
    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path / "prepared",
            con=con,
            overwrite=True,
            show_progress="off",
        )

    stage_prefixes = (
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


def test_prepare_default_show_progress_enables_live_output(
    con, canonical_data, tmp_path, monkeypatch
):
    enabled_values: list[bool] = []

    class _RecordingProgressBar:
        def __init__(
            self,
            *,
            label: str,
            total: int,
            total_units: int | None = None,
            enabled: bool = True,
            stream: object | None = None,
        ) -> None:
            del label, total, total_units, stream
            enabled_values.append(enabled)

        def update(
            self,
            current: int,
            *,
            completed_units: int | None = None,
        ) -> None:
            del current, completed_units

        def close(self) -> None:
            return None

    monkeypatch.setattr(chunking_strategies, "_ProgressBar", _RecordingProgressBar)

    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path / "prepared",
        con=con,
        overwrite=True,
    )

    assert enabled_values
    assert all(value is True for value in enabled_values)


def test_prepare_show_progress_true_emits_live_output(
    con, canonical_data, tmp_path, monkeypatch
):
    enabled_values: list[bool] = []

    class _RecordingProgressBar:
        def __init__(
            self,
            *,
            label: str,
            total: int,
            total_units: int | None = None,
            enabled: bool = True,
            stream: object | None = None,
        ) -> None:
            del label, total, total_units, stream
            enabled_values.append(enabled)

        def update(
            self,
            current: int,
            *,
            completed_units: int | None = None,
        ) -> None:
            del current, completed_units

        def close(self) -> None:
            return None

    monkeypatch.setattr(chunking_strategies, "_ProgressBar", _RecordingProgressBar)

    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path / "prepared",
        con=con,
        overwrite=True,
        show_progress="auto",
    )

    assert enabled_values
    assert all(value is True for value in enabled_values)


def test_prepare_logs_stage_and_batch_progress(con, canonical_data, tmp_path, caplog):
    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path / "prepared",
            con=con,
            overwrite=True,
            show_progress=True,
        )

    info_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.INFO
    ]
    assert any(
        message.startswith("Cleaning and preprocessing:") and "records across" in message
        for message in info_messages
    )
    assert any(
        message.startswith("Cleaning and preprocessing completed:")
        for message in info_messages
    )
    assert any(
        message.startswith("Applying term frequencies:") and "records across" in message
        for message in info_messages
    )
    assert any(
        message.startswith("Applying term frequencies completed:")
        for message in info_messages
    )
    assert any(
        message.startswith("Cleaning and preprocessing:") and "chunk 1/1" in message
        for message in info_messages
    )
    progress_glyphs = ("█", "░", "▕", "▏", "▮", "▯")
    assert all(
        all(glyph not in message for glyph in progress_glyphs)
        for message in info_messages
    )


@pytest.mark.parametrize(
    ("input_value", "reader_name", "expected_argument"),
    [
        (
            "s3://bucket/input/os_fake.csv",
            "read_csv",
            "s3://bucket/input/os_fake.csv",
        ),
        (
            "gs://bucket/input/data.parquet?version=1",
            "read_parquet",
            "gs://bucket/input/data.parquet?version=1",
        ),
        (
            [
                "abfs://container/input/part-1.csv",
                "abfs://container/input/part-2.csv",
            ],
            "read_csv",
            [
                "abfs://container/input/part-1.csv",
                "abfs://container/input/part-2.csv",
            ],
        ),
    ],
)
def test_coerce_prepare_input_reads_cloud_paths_with_duckdb(
    input_value, reader_name, expected_argument
):
    con = MagicMock()
    relation = _fake_relation(
        columns=["unique_id", "address_concat", "postcode"],
        row_count=3,
    )
    getattr(con, reader_name).return_value = relation

    result = _coerce_prepare_input_to_relation(input_value, con=con)

    assert result is relation
    getattr(con, reader_name).assert_called_once_with(expected_argument)


def test_coerce_prepare_input_projects_original_address_concat():
    con = MagicMock()
    original_relation = MagicMock()
    projected_relation = _fake_relation(
        columns=["unique_id", "original_address_concat", "address_concat"],
        row_count=3,
    )
    original_relation.columns = ["unique_id", "original_address_concat"]
    original_relation.project.return_value = projected_relation
    con.read_csv.return_value = original_relation

    result = _coerce_prepare_input_to_relation("s3://bucket/input/raw.csv", con=con)

    assert result is projected_relation
    original_relation.project.assert_called_once_with(
        "*, original_address_concat AS address_concat"
    )


@pytest.mark.parametrize("invalid_chunk_count", [0, -1, -10])
def test_prepare_rejects_non_positive_output_chunk_count(
    con, canonical_data, tmp_path, invalid_chunk_count
):
    with pytest.raises(ValueError, match="output_chunk_count must be at least 1"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path,
            con=con,
            output_chunk_count=invalid_chunk_count,
            overwrite=True,
        )


def test_prepare_rejects_output_chunk_count_above_supported_digits(
    con, canonical_data, tmp_path
):
    with pytest.raises(ValueError, match="output_chunk_count must be at most"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path,
            con=con,
            output_chunk_count=MAX_CHUNK_COUNT + 1,
            overwrite=True,
        )


@pytest.mark.parametrize("invalid_chunk_count", [0, -1, -10])
def test_prepare_rejects_non_positive_num_of_chunks(
    con, canonical_data, tmp_path, invalid_chunk_count
):
    with pytest.raises(ValueError, match="num_of_chunks must be at least 1"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path,
            con=con,
            num_of_chunks=invalid_chunk_count,
            overwrite=True,
        )


def test_prepare_rejects_num_of_chunks_above_supported_digits(
    con, canonical_data, tmp_path
):
    with pytest.raises(ValueError, match="num_of_chunks must be at most"):
        prepare_canonical_folder(
            canonical_data,
            output_folder=tmp_path,
            con=con,
            num_of_chunks=MAX_CHUNK_COUNT + 1,
            overwrite=True,
        )


def test_prepare_overwrite_false_raises(con, canonical_data):
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )
        with pytest.raises(FileExistsError):
            prepare_canonical_folder(
                canonical_data, output_folder=tmp, con=con, overwrite=False
            )


def test_prepare_overwrite_true_succeeds(con, canonical_data):
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )
        # Should not raise on second write
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )


@pytest.mark.parametrize("add_debug_features", [False, True])
def test_prepare_remote_csv_input_writes_remote_output(monkeypatch, add_debug_features):
    from uk_address_matcher.cleaning import chunking_strategies

    con = MagicMock()
    raw_relation = _fake_relation(
        columns=["unique_id", "address_concat", "postcode"],
        row_count=3,
    )
    con.read_csv.return_value = raw_relation
    con.read_parquet.side_effect = FileNotFoundError("missing")

    tf_relation = _fake_relation(columns=["token", "count"], row_count=4)
    inverted_relation = _fake_relation(columns=["token", "address_id"], row_count=5)
    clean_relation = _fake_relation(
        columns=["unique_id", "postcode", "clean_full_address", "ukam_address_id"],
        row_count=3,
    )

    monkeypatch.setattr(
        chunking_strategies,
        "clean_data_pre_term_frequencies",
        lambda data, con, num_of_chunks, show_progress=True: clean_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "_derive_term_frequencies_from_precleaned",
        lambda data, con: tf_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "prepare_data_for_matching",
        lambda *args, **kwargs: clean_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "derive_inverted_index",
        lambda df_clean, con, num_of_chunks, show_progress=True: inverted_relation,
    )

    prepare_canonical_folder(
        "s3://bucket/input/canonical.csv",
        "s3://bucket/output/prepared",
        con=con,
        add_debug_features=add_debug_features,
    )

    con.read_csv.assert_called_once_with("s3://bucket/input/canonical.csv")

    copy_sql = [
        call.args[0]
        for call in con.execute.call_args_list
        if call.args and "COPY" in call.args[0]
    ]

    def _written(path: str) -> bool:
        return any(f"TO '{path}'" in sql for sql in copy_sql)

    parquet_copies = [sql for sql in copy_sql if "FORMAT PARQUET" in sql]
    assert parquet_copies
    assert all("PARQUET_VERSION V2" in sql for sql in parquet_copies)
    assert all("COMPRESSION ZSTD" in sql for sql in parquet_copies)
    inverted_index_copies = [
        sql for sql in parquet_copies if "ukam_inverted_index.parquet" in sql
    ]
    other_copies = [
        sql for sql in parquet_copies if "ukam_inverted_index.parquet" not in sql
    ]
    assert len(inverted_index_copies) == 1
    assert "COMPRESSION_LEVEL 22" in inverted_index_copies[0]
    assert (
        "ORDER BY index_strategy, left(key, 1), unique_ids, key"
        in (inverted_index_copies[0])
    )
    assert all("COMPRESSION_LEVEL 6" in sql for sql in other_copies)
    assert all("ROW_GROUP_SIZE 122880" in sql for sql in parquet_copies)

    assert _written("s3://bucket/output/prepared/ukam_term_frequencies.parquet")
    assert _written("s3://bucket/output/prepared/ukam_inverted_index.parquet")
    assert _written("s3://bucket/output/prepared/ukam_canonical_addresses.parquet")
    assert any(
        "ukam_manifest.json" in call.args[0]
        for call in con.execute.call_args_list
        if call.args
    )
    manifest_parameters = [
        call.args[1]
        for call in con.execute.call_args_list
        if len(call.args) > 1 and "preparation_options" in str(call.args[0])
    ]
    assert manifest_parameters[0][4] == json.dumps(
        {"add_debug_features": add_debug_features}
    )


@pytest.mark.parametrize("add_debug_features", [False, True])
def test_prepare_remote_output_writes_chunked_paths(monkeypatch, add_debug_features):
    from uk_address_matcher.cleaning import chunking_strategies

    con = MagicMock()
    raw_relation = _fake_relation(
        columns=["unique_id", "address_concat", "postcode"],
        row_count=3,
    )
    con.read_csv.return_value = raw_relation
    con.read_parquet.side_effect = FileNotFoundError("missing")

    tf_relation = _fake_relation(columns=["token", "count"], row_count=4)
    inverted_relation = _fake_relation(columns=["token", "address_id"], row_count=5)
    clean_relation = _fake_relation(
        columns=["unique_id", "postcode", "clean_full_address", "ukam_address_id"],
        row_count=3,
    )

    chunk_queries = []
    for row_count in (2, 1):
        chunk_query = _fake_relation(
            columns=["unique_id", "address_concat", "postcode"],
            row_count=row_count,
        )
        chunk_queries.append(chunk_query)
    con.sql.side_effect = chunk_queries

    monkeypatch.setattr(
        chunking_strategies,
        "clean_data_pre_term_frequencies",
        lambda data, con, num_of_chunks, show_progress=True: clean_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "_derive_term_frequencies_from_precleaned",
        lambda data, con: tf_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "prepare_data_for_matching",
        lambda *args, **kwargs: clean_relation,
    )
    monkeypatch.setattr(
        chunking_strategies,
        "derive_inverted_index",
        lambda df_clean, con, num_of_chunks, show_progress=True: inverted_relation,
    )

    prepare_canonical_folder(
        "s3://bucket/input/canonical.csv",
        "s3://bucket/output/prepared",
        con=con,
        output_chunk_count=2,
        add_debug_features=add_debug_features,
    )

    copy_sql = [
        call.args[0]
        for call in con.execute.call_args_list
        if call.args and "COPY" in call.args[0]
    ]

    def _written(path: str) -> bool:
        return any(f"TO '{path}'" in sql for sql in copy_sql)

    assert _written(
        "s3://bucket/output/prepared/ukam_canonical_addresses_chunks/"
        "canonical_addresses_chunk_00001_of_00002.parquet"
    )
    assert _written(
        "s3://bucket/output/prepared/ukam_canonical_addresses_chunks/"
        "canonical_addresses_chunk_00002_of_00002.parquet"
    )
    manifest_parameters = [
        call.args[1]
        for call in con.execute.call_args_list
        if len(call.args) > 1 and "preparation_options" in str(call.args[0])
    ]
    assert manifest_parameters[0][4] == json.dumps(
        {"add_debug_features": add_debug_features}
    )


def test_prepared_canonical_chunks_are_globally_ordered_and_use_parquet_v2(con, tmp_path):
    canonical = con.sql("""
        SELECT * FROM (VALUES
            (2::BIGINT, '2 BETA STREET', 'B2 2BB', 'z.parquet'),
            (1::BIGINT, '1 ALPHA STREET', 'A1 1AA', 'z.parquet'),
            (1::BIGINT, '1 ALPHA STREET', 'A1 1AA', 'a.parquet'),
            (3::BIGINT, '3 ALPHA STREET', 'A1 1AA', 'a.parquet')
        ) AS source(unique_id, address_concat, postcode, filename)
    """)

    prepare_canonical_folder(
        canonical,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=2,
        overwrite=True,
    )

    chunk_paths = sorted((tmp_path / "ukam_canonical_addresses_chunks").glob("*.parquet"))
    addresses = con.read_parquet([str(path) for path in chunk_paths])
    physical_ids = addresses.select("ukam_address_id").fetchall()

    assert addresses.count("*").fetchone()[0] == 4
    assert physical_ids == [(1,), (2,), (3,), (4,)]
    assert addresses.columns == con.read_parquet(str(chunk_paths[0])).columns
    assert (
        addresses.select("ukam_address_id").fetchall()
        == addresses.order(
            "postcode, unique_id, clean_full_address, filename, ukam_address_id"
        )
        .select("ukam_address_id")
        .fetchall()
    )

    for path in chunk_paths:
        parquet_file = pyarrow_parquet.ParquetFile(path)
        assert parquet_file.metadata.format_version.startswith("2.")
        metadata = con.execute(
            """
            SELECT row_group_num_rows, path_in_schema, compression, encodings
            FROM parquet_metadata(?)
            """,
            [str(path)],
        ).fetchall()
        assert metadata
        assert {row[2] for row in metadata} == {"ZSTD"}
        assert max(row[0] for row in metadata) <= 122_880
        id_encodings = [row[3] for row in metadata if row[1] == "ukam_address_id"]
        assert id_encodings
        assert all("DELTA_BINARY_PACKED" in encodings for encodings in id_encodings)

    id_type = con.execute(
        "DESCRIBE SELECT ukam_address_id FROM read_parquet(?)",
        [str(chunk_paths[0])],
    ).fetchone()[1]
    assert id_type == "INTEGER"

    inverted_index_path = tmp_path / "ukam_inverted_index.parquet"
    inverted_index = con.read_parquet(str(inverted_index_path))
    assert inverted_index.columns == ["key", "unique_ids", "index_strategy"]
    assert [str(value) for value in inverted_index.types] == [
        "VARCHAR",
        "BIGINT[]",
        "VARCHAR",
    ]
    inverted_index_row_count = inverted_index.count("*").fetchone()[0]
    assert inverted_index_row_count > 0
    assert (
        inverted_index.select("key, index_strategy").distinct().count("*").fetchone()[0]
        == inverted_index_row_count
    )
    assert (
        inverted_index.fetchall()
        == inverted_index.order(
            "index_strategy, left(key, 1), unique_ids, key"
        ).fetchall()
    )

    index_parquet_file = pyarrow_parquet.ParquetFile(inverted_index_path)
    assert index_parquet_file.metadata.format_version.startswith("2.")
    index_metadata = con.execute(
        """
        SELECT row_group_num_rows, path_in_schema, compression, encodings
        FROM parquet_metadata(?)
        """,
        [str(inverted_index_path)],
    ).fetchall()
    assert index_metadata
    assert {row[2] for row in index_metadata} == {"ZSTD"}
    assert max(row[0] for row in index_metadata) <= 122_880
    unique_ids_encodings = [
        row[3] for row in index_metadata if row[1] == "unique_ids, list, element"
    ]
    assert unique_ids_encodings
    assert all("DELTA_BINARY_PACKED" in encodings for encodings in unique_ids_encodings)


def test_overwrite_clears_stale_files(con, canonical_data):
    """overwrite=True should remove temp files left by a previous interrupted run."""
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        stale = Path(tmp) / "ukam_manifest.json.tmp"
        stale.write_text("stale")
        assert stale.exists()

        stale_chunk_dir = Path(tmp) / "ukam_canonical_addresses_chunks"
        stale_chunk_dir.mkdir(parents=True, exist_ok=True)
        (stale_chunk_dir / "old_chunk.parquet").write_text("stale")

        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        assert not stale.exists()
        assert not stale_chunk_dir.exists()
        assert (Path(tmp) / "ukam_manifest.json").exists()
        assert (Path(tmp) / "ukam_canonical_addresses.parquet").exists()


def test_manifest_contains_expected_fields(prepared_folder):
    manifest = json.loads((prepared_folder / "ukam_manifest.json").read_text())

    assert "ukam_version" in manifest
    assert "created_at" in manifest
    assert "created_with_duckdb_version" in manifest
    assert manifest["row_counts"]["canonical_addresses"] == 3
    assert manifest["row_counts"]["canonical_output_chunks"] == 1
    assert manifest["preparation_options"] == {"add_debug_features": False}

    # Per-file metadata
    assert "files" in manifest
    addr_meta = manifest["files"]["ukam_canonical_addresses.parquet"]
    assert "size_bytes" in addr_meta
    assert "sha256" in addr_meta
    assert "columns" in addr_meta
    assert isinstance(addr_meta["columns"], list)
    assert len(addr_meta["columns"]) > 0


def test_manifest_version_mismatch_warns(con, prepared_folder):
    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["ukam_version"] = "0.0.0"
    manifest_path.write_text(json.dumps(manifest))

    with pytest.warns(UserWarning, match="v0.0.0"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_missing_manifest_warns(con, prepared_folder):
    """Loading from a folder with no manifest should warn."""
    (prepared_folder / "ukam_manifest.json").unlink()

    with pytest.warns(UserWarning, match="No manifest file found"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_file_size_mismatch_warns(con, prepared_folder):
    """A recorded size that doesn't match the actual file should warn."""
    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"]["ukam_canonical_addresses.parquet"]["size_bytes"] = 1
    manifest_path.write_text(json.dumps(manifest))

    with pytest.warns(UserWarning, match="size.*bytes"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_load_returns_prepared_canonical(con, prepared_folder):
    result = load_prepared_canonical_data(prepared_folder, con=con)

    assert isinstance(result, _PreparedCanonical)
    assert isinstance(result.addresses, duckdb.DuckDBPyRelation)
    assert isinstance(result.term_frequencies, duckdb.DuckDBPyRelation)
    assert isinstance(result.inverted_index, duckdb.DuckDBPyRelation)


def test_load_prepared_data_has_expected_row_counts(con, prepared_folder):
    result = load_prepared_canonical_data(prepared_folder, con=con)

    assert result.addresses.count("*").fetchone()[0] == 3
    assert result.term_frequencies.count("*").fetchone()[0] > 0
    assert result.inverted_index.count("*").fetchone()[0] > 0


@pytest.mark.parametrize("output_chunk_count", [1, 2])
def test_prepare_persists_distinguishing_tokens_with_array_types(
    con,
    tmp_path,
    output_chunk_count,
):
    canonical = con.sql(
        """
        SELECT * FROM (VALUES
            ('C1', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            ('C2', '1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            ('C3', '9 SOLO ROAD YORK', 'Y1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    prepare_canonical_folder(
        canonical,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=output_chunk_count,
        overwrite=True,
        show_progress=False,
    )

    loaded = load_prepared_canonical_data(tmp_path, con=con).addresses
    column_types = dict(zip(loaded.columns, map(str, loaded.types)))
    actual = {
        unique_id: (distinguishing, common)
        for unique_id, distinguishing, common in loaded.project(
            """
            unique_id,
            distinguishing_adj_start_tokens,
            common_adj_start_tokens
            """
        ).fetchall()
    }

    assert column_types["distinguishing_adj_start_tokens"] == "VARCHAR[]"
    assert column_types["common_adj_start_tokens"] == "VARCHAR[]"
    assert (
        loaded.filter(
            "distinguishing_adj_start_tokens IS NULL OR common_adj_start_tokens IS NULL"
        )
        .count("*")
        .fetchone()[0]
        == 0
    )
    assert actual == {
        "C1": (
            ["FLAT", "A"],
            ["1", "HIGH", "STREET", "CAMDEN", "LONDON"],
        ),
        "C2": ([], ["1", "HIGH", "STREET", "CAMDEN", "LONDON"]),
        "C3": ([], ["9", "SOLO", "ROAD", "YORK"]),
    }


def test_prepare_can_disable_distinguishing_token_derivation(con, tmp_path):
    prepare_canonical_folder(
        con.sql(
            """
            SELECT * FROM (VALUES
                ('C1', '1 HIGH STREET CAMDEN LONDON', 'N1 1AA')
            ) AS t(unique_id, address_concat, postcode)
            """
        ),
        output_folder=tmp_path,
        con=con,
        derive_distinguishing_wrt_adjacent_records=False,
        overwrite=True,
        show_progress=False,
    )

    loaded = load_prepared_canonical_data(tmp_path, con=con).addresses

    assert "distinguishing_adj_start_tokens" not in loaded.columns
    assert "common_adj_start_tokens" not in loaded.columns


def test_chunked_canonical_manifest_row_audit(con, canonical_data, tmp_path):
    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=3,
        overwrite=True,
    )

    manifest = json.loads((tmp_path / "ukam_manifest.json").read_text())
    chunk_files = sorted((tmp_path / "ukam_canonical_addresses_chunks").glob("*.parquet"))

    total_rows_across_chunks = sum(
        con.read_parquet(str(chunk_path)).count("*").fetchone()[0]
        for chunk_path in chunk_files
    )

    assert manifest["row_counts"]["canonical_addresses"] == 3
    assert manifest["row_counts"]["canonical_output_chunks"] == 3
    assert total_rows_across_chunks == manifest["row_counts"]["canonical_addresses"]


def test_load_reads_chunked_canonical_layout(con, canonical_data, tmp_path):
    prepare_canonical_folder(
        canonical_data,
        output_folder=tmp_path,
        con=con,
        output_chunk_count=4,
        overwrite=True,
    )

    result = load_prepared_canonical_data(tmp_path, con=con)

    assert isinstance(result, _PreparedCanonical)
    assert result.addresses.count("*").fetchone()[0] == 3


def test_load_accepts_string_path(con, prepared_folder):
    result = load_prepared_canonical_data(str(prepared_folder), con=con)
    assert result.addresses.count("*").fetchone()[0] == 3


def test_invalid_folder_raises(con):
    with pytest.raises(FileNotFoundError):
        load_prepared_canonical_data("/tmp/nonexistent_folder_xyz_123", con=con)


def test_corrupt_parquet_raises(con, prepared_folder):
    """A corrupt Parquet file should be caught during validation."""
    corrupt_path = prepared_folder / "ukam_canonical_addresses.parquet"
    corrupt_path.write_bytes(b"not a parquet file")

    with pytest.raises(FileNotFoundError, match="not a valid Parquet file"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_load_remote_folder_reads_chunked_layout_and_applies_filter():
    folder_uri = "s3://test-bucket/prepared"

    addresses_rel = MagicMock()
    filtered_addresses_rel = MagicMock()
    addresses_rel.limit.return_value = addresses_rel
    addresses_rel.fetchone.return_value = (1,)
    addresses_rel.filter.return_value = filtered_addresses_rel

    tf_rel = MagicMock()
    tf_rel.limit.return_value = tf_rel
    tf_rel.fetchone.return_value = (1,)

    idx_rel = MagicMock()
    idx_rel.limit.return_value = idx_rel
    idx_rel.fetchone.return_value = (1,)

    con = MagicMock()

    def _read_parquet_side_effect(path):
        if path == f"{folder_uri}/ukam_term_frequencies.parquet":
            return tf_rel
        if path == f"{folder_uri}/ukam_inverted_index.parquet":
            return idx_rel
        if path == f"{folder_uri}/ukam_canonical_addresses_chunks/*.parquet":
            return addresses_rel
        raise AssertionError(f"Unexpected path: {path}")

    con.read_parquet.side_effect = _read_parquet_side_effect

    result = load_prepared_canonical_data(
        folder_uri,
        con=con,
        canonical_address_filter="postcode = 'SW1A2AA'",
    )

    assert isinstance(result, _PreparedCanonical)
    assert result.addresses is filtered_addresses_rel
    assert result.term_frequencies is tf_rel
    assert result.inverted_index is idx_rel
    addresses_rel.filter.assert_called_once_with("postcode = 'SW1A2AA'")


def test_load_remote_folder_reads_single_dataset_directory_layout():
    folder_uri = "s3://test-bucket/prepared"

    addresses_rel = MagicMock()
    addresses_rel.limit.return_value = addresses_rel
    addresses_rel.fetchone.return_value = (1,)

    tf_rel = MagicMock()
    tf_rel.limit.return_value = tf_rel
    tf_rel.fetchone.return_value = (1,)

    idx_rel = MagicMock()
    idx_rel.limit.return_value = idx_rel
    idx_rel.fetchone.return_value = (1,)

    con = MagicMock()

    def _read_parquet_side_effect(path):
        if path == f"{folder_uri}/ukam_term_frequencies.parquet":
            return tf_rel
        if path == f"{folder_uri}/ukam_inverted_index.parquet":
            return idx_rel
        if path == f"{folder_uri}/ukam_canonical_addresses_chunks/*.parquet":
            raise FileNotFoundError("No files found for chunk glob")
        if path == f"{folder_uri}/ukam_canonical_addresses.parquet":
            raise FileNotFoundError("Path is a directory-like prefix")
        if path == f"{folder_uri}/ukam_canonical_addresses.parquet/*.parquet":
            return addresses_rel
        raise AssertionError(f"Unexpected path: {path}")

    con.read_parquet.side_effect = _read_parquet_side_effect

    result = load_prepared_canonical_data(folder_uri, con=con)

    assert isinstance(result, _PreparedCanonical)
    assert result.addresses is addresses_rel


def test_load_remote_folder_rolls_back_and_falls_back_to_single_file():
    folder_uri = "s3://test-bucket/prepared"

    addresses_rel = MagicMock()
    addresses_rel.limit.return_value = addresses_rel
    addresses_rel.fetchone.return_value = (1,)

    tf_rel = MagicMock()
    tf_rel.limit.return_value = tf_rel
    tf_rel.fetchone.return_value = (1,)

    idx_rel = MagicMock()
    idx_rel.limit.return_value = idx_rel
    idx_rel.fetchone.return_value = (1,)

    con = MagicMock()

    def _read_parquet_side_effect(path):
        if path == f"{folder_uri}/ukam_term_frequencies.parquet":
            return tf_rel
        if path == f"{folder_uri}/ukam_inverted_index.parquet":
            return idx_rel
        if path == f"{folder_uri}/ukam_canonical_addresses_chunks/*.parquet":
            raise FileNotFoundError("No files found for chunk glob")
        if path == f"{folder_uri}/ukam_canonical_addresses.parquet":
            return addresses_rel
        raise AssertionError(f"Unexpected path: {path}")

    con.read_parquet.side_effect = _read_parquet_side_effect

    result = load_prepared_canonical_data(folder_uri, con=con)

    assert isinstance(result, _PreparedCanonical)
    assert result.addresses is addresses_rel
    con.execute.assert_any_call("ROLLBACK")


def test_load_remote_folder_missing_required_files_raises_filenotfound():
    folder_uri = "s3://test-bucket/prepared"

    con = MagicMock()
    con.read_parquet.side_effect = FileNotFoundError("No files found")

    with pytest.raises(FileNotFoundError, match="missing required files"):
        load_prepared_canonical_data(folder_uri, con=con)


def test_load_remote_folder_permission_error_raises_permissionerror():
    folder_uri = "s3://test-bucket/prepared"

    con = MagicMock()
    con.read_parquet.side_effect = RuntimeError("HTTP 403 Forbidden")

    with pytest.raises(PermissionError, match="Cannot access prepared canonical data"):
        load_prepared_canonical_data(folder_uri, con=con)
