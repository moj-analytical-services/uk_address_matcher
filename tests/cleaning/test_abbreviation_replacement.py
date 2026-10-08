import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

from uk_address_matcher.cleaning.pipelines import QUEUE_CLEAN_FULL_ADDRESS
from uk_address_matcher.cleaning.steps import (
    _clean_address_string_first_pass,
    _normalise_abbreviations_and_units,
    _parse_out_address_structure_premise,
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _split_letter_dash_letter,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


@pytest.fixture
def test_abbr_data(duck_con):
    """Set up test data as DuckDB PyRelations for exact matching tests."""
    return duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('42 HIGH ST LONDON'),
            ('FLAT 3 APT 5 MANOR RD GLASGOW'),
            ('FLAT 10 OFFICE SUITE 200 BLVD DRIVE BIRMINGHAM'),
            ('123 TOWNHALL STREET CENTRE MANCHESTER'),
            ('THE COTTAGE WOOD LANE BRISTOL'),
            ('FACTORY WORKS WKS INDUSTRIAL ESTATE RD'),
            ('MUSEUM GALLERY THEATRE THEA CIVIC CENTRE'),
            ('FLAT 3D 12B BAKER AVE LONDON'),
            ('PENTHOUSE 1A OXFORD CLS LONDON'),
            ('10 LHS MEWS LONDON'),
            ('12 RHS COURT ROAD LONDON'),
            ('FLAT 1ST FLR FT 176 LOWER CLAPTON ROAD LONDON'),
            ('FLAT UPPR 20 HIGH STREET LONDON'),
            ('FLAT 1ST FLR LT 4D UFTON ROAD LONDON'),
            ('FLAT 1ST FLR RT 10 RECTORY ROAD LONDON'),
        ) AS t(clean_full_address)
    """
    )


def test_abbreviation_normalisation_sql(duck_con, test_abbr_data):
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=test_abbr_data,
        stage_specs=[_normalise_abbreviations_and_units],
    )
    result_rel = pipeline.run()
    rows = result_rel.fetchall()
    columns = result_rel.columns
    clean_idx = columns.index("clean_full_address")

    expected_addresses = [
        "42 HIGH ST LONDON",
        "FLAT 3 APARTMENT 5 MANOR ROAD GLASGOW",
        "FLAT 10 OFFICE SUITE 200 BOULEVARD DRIVE BIRMINGHAM",
        "123 TOWN HALL STREET CENTRE MANCHESTER",
        "THE COTTAGE WOOD LANE BRISTOL",
        "FACTORY WORKS WORKS INDUSTRIAL ESTATE ROAD",
        "MUSEUM GALLERY THEATRE THEATRE CIVIC CENTRE",
        "FLAT 3D 12B BAKER AVENUE LONDON",
        "PENTHOUSE 1A OXFORD CLOSE LONDON",
        "10 LEFT HAND SIDE MEWS LONDON",
        "12 RIGHT HAND SIDE COURT ROAD LONDON",
        "FLAT FIRST FLOOR FRONT 176 LOWER CLAPTON ROAD LONDON",
        "FLAT UPPER 20 HIGH STREET LONDON",
        "FLAT FIRST FLOOR LEFT 4D UFTON ROAD LONDON",
        "FLAT FIRST FLOOR RIGHT 10 RECTORY ROAD LONDON",
    ]
    for row, expected in zip(rows, expected_addresses):
        assert row[clean_idx] == expected


def test_abbreviations_expand_to_multi_word_business_shells(duck_con):
    """Abbreviations can expand inside business/unit shells.

    The replacement may add multiple words, but surrounding tokens such as CHURCH,
    PRESBYTERY, ARMLEY, SHOP, and street names must be preserved. The expansion
    should also fire wherever the token appears, not only at the start of the string.
    """
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('RC CHURCH 5 EXAMPLE ROAD LONDON'),
            ('ST MARYS RC PRIMARY SCHOOL CHURCH LANE'),
            ('FLAT 2 RC PRESBYTERY 9 CHAPEL STREET'),
            ('SHOP FF 10 HIGH STREET')
        ) AS t(clean_full_address)
    """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_normalise_abbreviations_and_units],
    )
    result_rel = pipeline.run()
    clean_idx = result_rel.columns.index("clean_full_address")
    actual = [row[clean_idx] for row in result_rel.fetchall()]

    assert actual == [
        "ROMAN CATHOLIC CHURCH 5 EXAMPLE ROAD LONDON",
        "ST MARYS ROMAN CATHOLIC PRIMARY SCHOOL CHURCH LANE",
        "FLAT 2 ROMAN CATHOLIC PRESBYTERY 9 CHAPEL STREET",
        "SHOP FIRST FLOOR 10 HIGH STREET",
    ]


def test_confirmed_address_abbreviations_expand(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('BST FNT 23 EXAMPLE STREET'),
            ('LWR GND FLR 102 SAMPLE ROAD'),
            ('HILLSIDE CFT SAMPLE PLACE'),
            ('ALPHA LDGE SAMPLE PLACE'),
            ('ALPHA LDG SAMPLE PLACE'),
            ('ALPHA LGE SAMPLE PLACE'),
            ('ALPHA NEW FARMHSE'),
            ('SAMPLE FM'),
            ('GAMEKPRS COTTAGE SAMPLE PLACE'),
            ('UPPR SAMPLE PLACE'),
            ('UPR SAMPLE PLACE'),
            ('DR SAMPLE HOUSE')
        ) AS t(clean_full_address)
    """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_normalise_abbreviations_and_units],
    )
    result_rel = pipeline.run()
    rows = [row[0] for row in result_rel.fetchall()]

    assert rows == [
        "BASEMENT FRONT 23 EXAMPLE STREET",
        "LOWER GROUND FLOOR 102 SAMPLE ROAD",
        "HILLSIDE CROFT SAMPLE PLACE",
        "ALPHA LODGE SAMPLE PLACE",
        "ALPHA LODGE SAMPLE PLACE",
        "ALPHA LODGE SAMPLE PLACE",
        "ALPHA NEW FARMHOUSE",
        "SAMPLE FARM",
        "GAMEKEEPERS COTTAGE SAMPLE PLACE",
        "UPPER SAMPLE PLACE",
        "UPPER SAMPLE PLACE",
        "DR SAMPLE HOUSE",
    ]


def test_excluding_phrase_keeps_word_boundaries(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('EXC BST 238 TEST ROAD'),
            ('HSE EXC BST 47 TEST ROAD'),
            ('SHOP EXCLUDING BASEMENT 1 TEST ROAD'),
            ('SHOP (EXCLUDING BASEMENT) 2 TEST ROAD'),
            ('HSE EXCL STUDIO 17 TEST ROAD'),
            ('SHOP EXCLUDING GARAGE 1 TEST ROAD')
        ) AS t(clean_full_address)
        """
    )
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_normalise_abbreviations_and_units],
    )

    assert [row[0] for row in pipeline.run().fetchall()] == [
        "EXCLUDING BASEMENT 238 TEST ROAD",
        "HOUSE EXCLUDING BASEMENT 47 TEST ROAD",
        "SHOP EXCLUDING BASEMENT 1 TEST ROAD",
        "SHOP (EXCLUDING BASEMENT) 2 TEST ROAD",
        "HOUSE EXCLUDING STUDIO 17 TEST ROAD",
        "SHOP EXCLUDING GARAGE 1 TEST ROAD",
    ]


def test_first_pass_splits_non_numeric_underscores_only(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('EXCL_PT_BSMT 41 LINTHORPE ROAD LONDON'),
            ('THE DEPOT_ 18 WENLOCK ROAD LONDON'),
            ('0401_0120 SOME BUILDING LONDON'),
            ('NO_1 HIGH STREET LONDON')
        ) AS t(clean_full_address)
    """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_clean_address_string_first_pass],
    )
    result_rel = pipeline.run()
    rows = [row[0] for row in result_rel.fetchall()]

    assert rows == [
        "EXCL PT BSMT 41 LINTHORPE ROAD LONDON",
        "THE DEPOT 18 WENLOCK ROAD LONDON",
        "0401_0120 SOME BUILDING LONDON",
        "NO_1 HIGH STREET LONDON",
    ]


def test_post_abbreviation_split_preserves_names_and_numeric_ranges(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('PEN-Y-GRAIG'),
            ('WELLS-NEXT-THE-SEA'),
            ('TIGH102-NA'),
            ('1-2 GETHIN ROAD'),
            ('UNIT 5/6')
        ) AS t(clean_full_address)
        """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[
            _clean_address_string_first_pass,
            _normalise_abbreviations_and_units,
            _split_letter_dash_letter,
        ],
    )

    assert [row[0] for row in pipeline.run().fetchall()] == [
        "PEN Y GRAIG",
        "WELLS NEXT THE SEA",
        "TIGH102-NA",
        "1-2 GETHIN ROAD",
        "UNIT 5-6",
    ]


def test_specific_phrase_aliases_expand_before_letter_dash_splitting(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('F/F 23 EXAMPLE STREET'),
            ('SAMPLE LDGE H/HEAD SAMPLE ROAD'),
            ('BLCK/SMS SAMPLE CROFT'),
            ('TRNRHALL SAMPLE CROFT'),
            ('H HEAD SAMPLE HOUSE')
        ) AS t(clean_full_address)
        """
    )
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[
            _clean_address_string_first_pass,
            _normalise_abbreviations_and_units,
            _split_letter_dash_letter,
        ],
    )

    assert [row[0] for row in pipeline.run().fetchall()] == [
        "FIRST FLOOR 23 EXAMPLE STREET",
        "SAMPLE LODGE HILLHEAD SAMPLE ROAD",
        "BLACKSMITHS SAMPLE CROFT",
        "TURNERHALL SAMPLE CROFT",
        "H HEAD SAMPLE HOUSE",
    ]


def test_exclusion_phrases_do_not_create_unit_or_premise_features(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('EXCLUDING BASEMENT 41 TEST ROAD', 'EXCLUDING BASEMENT 41 TEST ROAD'),
            (
                'HOUSE EXCLUDING STUDIO 17 TEST ROAD',
                'HOUSE EXCLUDING STUDIO 17 TEST ROAD'
            ),
            ('SHOP EXCLUDING GARAGE 12 TEST ROAD', 'SHOP EXCLUDING GARAGE 12 TEST ROAD'),
            ('BASEMENT FLAT A 11 TEST COURT', 'BASEMENT FLAT A 11 TEST COURT'),
            ('STUDIO 4 TEST PLACE', 'STUDIO 4 TEST PLACE'),
            ('GARAGE 4 TEST STREET', 'GARAGE 4 TEST STREET')
        ) AS t(clean_full_address, original_address_concat)
        """
    )
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[
            _parse_out_flat_position_and_letter,
            _parse_out_business_unit,
            _parse_out_address_structure_premise,
        ],
    )

    result_rel = pipeline.run()
    rows = result_rel.fetchall()
    columns = result_rel.columns
    positional_idx = columns.index("flat_positional")
    business_unit_idx = columns.index("business_unit_type")
    premise_idx = columns.index("address_structure_premise_type")
    assert [
        (row[positional_idx], row[business_unit_idx], row[premise_idx]) for row in rows
    ] == [
        (None, None, None),
        (None, None, None),
        (None, None, "SHOP"),
        ("BASEMENT", None, None),
        (None, "STUDIO", None),
        (None, None, "GARAGE"),
    ]


def test_letter_dash_split_is_enabled_by_default(duck_con):
    input_rel = duck_con.sql("SELECT 'PEN-Y-GRAIG' AS clean_full_address")
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_split_letter_dash_letter],
    )

    assert pipeline.run().fetchone()[0] == "PEN Y GRAIG"


def test_full_cleaning_queue_preserves_underscore_split_before_expansion(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('test-1', 1, 'EXCL_BSMT 41 TEST ROAD LONDON', NULL)
        ) AS t(unique_id, ukam_address_id, address_concat, postcode)
    """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=QUEUE_CLEAN_FULL_ADDRESS,
    )
    result_rel = pipeline.run()
    rows = result_rel.project("clean_full_address").fetchall()

    assert rows == [("EXCLUDING BASEMENT 41 TEST ROAD LONDON",)]
    assert pipeline.run().project("clean_full_address_tokens").fetchall() == [
        (["EXCLUDING", "BASEMENT", "41", "TEST", "ROAD", "LONDON"],)
    ]


## Checks to confirm our abbreviations file doesn't break the following properties:
# - No exact duplicate rows
# - No token collisions after normalisation
# - No two-way circular pairs
# - No cycles in abbreviation chains
# - All mappings converge
def norm(s: str) -> str:
    return s.strip().upper()


@pytest.fixture(scope="module")
def abbr_rows(pytestconfig) -> List[dict]:
    abbr_data_path = (
        Path(pytestconfig.rootpath)
        / "uk_address_matcher"
        / "data"
        / "address_abbreviations.json"
    )

    with abbr_data_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    for i, row in enumerate(data):
        assert "token" in row and "replacement" in row
        assert row["token"] is not None and row["replacement"] is not None
        assert isinstance(row["token"], str) and isinstance(row["replacement"], str)
    return data


@pytest.fixture(scope="module")
def mapping_normalised(abbr_rows: List[dict]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for row in abbr_rows:
        t = norm(row["token"])
        r = row["replacement"].strip()
        if t in m and m[t] != r:
            pass
        m.setdefault(t, r)
    return m


def test_no_exact_duplicate_rows_in_json(abbr_rows: List[dict]) -> None:
    seen: Set[Tuple[str, str]] = set()
    dups: List[Tuple[str, str]] = []
    for row in abbr_rows:
        pair = (row["token"], row["replacement"])
        if pair in seen:
            dups.append(pair)
        seen.add(pair)
    assert not dups, f"Duplicate rows found: {dups}"


def test_no_token_collisions_after_normalisation(abbr_rows: List[dict]) -> None:
    first_seen: Dict[str, Tuple[str, str]] = {}
    collisions: List[str] = []
    for row in abbr_rows:
        t_norm = norm(row["token"])
        if t_norm not in first_seen:
            first_seen[t_norm] = (row["token"], row["replacement"])
        else:
            prev_raw, prev_rep = first_seen[t_norm]
            collisions.append(
                f"{t_norm!r}: ({prev_raw!r}->{prev_rep!r}) "
                f"vs ({row['token']!r}->{row['replacement']!r})"
            )
    assert not collisions, "Duplicate tokens after normalisation:\n" + "\n".join(
        collisions
    )


def test_no_two_way_circular_pairs(mapping_normalised: Dict[str, str]) -> None:
    issues: List[str] = []
    tokens = set(mapping_normalised.keys())
    for a, r_raw in mapping_normalised.items():
        if norm(a) == norm(r_raw):
            continue
        b = norm(r_raw)
        if b in tokens:
            r2 = mapping_normalised[b]
            if norm(r2) == norm(a):
                issues.append(f"{a}->{b} and {b}->{a}")
    assert not issues, "Two-way circular mappings:\n" + "\n".join(issues)


def test_no_cycles_in_abbreviation_chains(mapping_normalised: Dict[str, str]) -> None:
    adj: Dict[str, List[str]] = {}
    tokens_upper = {norm(t) for t in mapping_normalised}
    for a_raw, r_raw in mapping_normalised.items():
        a = norm(a_raw)
        b = norm(r_raw)
        if a != b and b in tokens_upper:
            adj.setdefault(a, []).append(b)

    visiting: Set[str] = set()
    visited: Set[str] = set()
    cycle_paths: List[List[str]] = []

    def dfs(node: str, path: List[str]) -> None:
        if node in visited:
            return
        if node in visiting:
            idx = path.index(node)
            cycle_paths.append(path[idx:] + [node])
            return
        visiting.add(node)
        path.append(node)
        for nxt in adj.get(node, []):
            dfs(nxt, path)
        path.pop()
        visiting.remove(node)
        visited.add(node)

    for start in list(adj.keys()):
        if start not in visited:
            dfs(start, [])
    assert not cycle_paths, "Cycles detected:\n" + "\n".join(
        " -> ".join(c) for c in cycle_paths
    )


def test_mapping_converges(mapping_normalised: Dict[str, str]) -> None:
    tokens = list(mapping_normalised.keys())
    max_steps = len(tokens)
    offenders: List[str] = []

    def apply_once(tok: str) -> str:
        t = norm(tok)
        r = mapping_normalised.get(t)
        return norm(r) if r is not None else t

    for t in tokens:
        seen: Set[str] = set()
        cur = norm(t)
        steps = 0
        while steps <= max_steps:
            if cur in seen:
                offenders.append(t)
                break
            seen.add(cur)
            nxt = apply_once(cur)
            if nxt == cur:
                break
            cur = nxt
            steps += 1
    assert not offenders, "Non-converging tokens:\n" + "\n".join(offenders)
