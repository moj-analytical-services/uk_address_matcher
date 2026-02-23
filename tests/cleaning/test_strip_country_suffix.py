import json
from pathlib import Path

import pytest

from uk_address_matcher.cleaning.steps.normalisation import _strip_country_suffix
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


@pytest.fixture
def strip_input_data(duck_con):
    return duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('10 DOWNING STREET LONDON UNITED KINGDOM'),
            ('10 DOWNING STREET LONDON UK'),
            ('10 DOWNING STREET LONDON WALES'),
            ('10 DOWNING STREET LONDON WALES UK'),
            ('10 DOWNING STREET LONDON GREAT BRITAIN'),
            ('10 DOWNING STREET LONDON NORTHERN IRELAND UK'),
            ('SCOTLAND STREET GLASGOW'),
            (''),
            ('   '),
            ('A B C UK  ')
        ) AS t(clean_full_address)
        """
    )


def test_strip_country_suffix_stage_sql(duck_con, strip_input_data):
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=strip_input_data,
        stage_specs=[_strip_country_suffix],
    )
    result = pipeline.run()

    expected = [
        "10 DOWNING STREET LONDON",
        "10 DOWNING STREET LONDON",
        "10 DOWNING STREET LONDON",
        "10 DOWNING STREET LONDON",
        "10 DOWNING STREET LONDON",
        "10 DOWNING STREET LONDON",
        "SCOTLAND STREET GLASGOW",
        "",
        "",
        "A B C",
    ]

    rows = result.fetchall()
    clean_idx = result.columns.index("clean_full_address")
    assert [row[clean_idx] for row in rows] == expected


def test_common_end_tokens_excludes_country_suffixes(pytestconfig):
    json_path = (
        Path(pytestconfig.rootpath)
        / "uk_address_matcher"
        / "data"
        / "common_uk_end_tokens.json"
    )

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    banned_multi_tokens = {
        "UNITED KINGDOM",
        "GREAT BRITAIN",
        "NORTHERN IRELAND",
    }
    banned_single_tokens = {
        "UK",
        "BRITAIN",
        "ENGLAND",
        "SCOTLAND",
        "WALES",
    }

    aliases = data.get("aliases", {})
    multi_tokens = set(data.get("multi_tokens", []))
    single_tokens = set(data.get("single_tokens", []))

    for token in banned_multi_tokens:
        assert token not in multi_tokens
    for token in banned_single_tokens:
        assert token not in single_tokens

    assert "KINGDOM" not in aliases
    assert "UNITED KINGDOM" not in set(aliases.values())
