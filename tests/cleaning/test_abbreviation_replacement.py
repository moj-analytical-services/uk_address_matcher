import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

from uk_address_matcher.cleaning.pipelines import QUEUE_CLEAN_FULL_ADDRESS
from uk_address_matcher.cleaning.steps import (
    _clean_address_string_first_pass,
    _join_excluding_with_next_token,
    _normalise_abbreviations_and_units,
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


def test_excluding_token_is_joined_with_following_token(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('EXC BST 238 ALBION ROAD LONDON'),
            ('HSE EXC BST 47 ALKHAM ROAD LONDON'),
            ('SHOP EXCLUDING BASEMENT 1 TEST ROAD LONDON'),
            ('SHOP (EXCLUDING BASEMENT) 2 TEST ROAD LONDON'),
            ('HSE EXCL STUDIO 17 ASHTEAD ROAD LONDON'),
            ('SHOP EXCLUDING GARAGE 1 TEST ROAD LONDON')
        ) AS t(clean_full_address)
    """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[
            _normalise_abbreviations_and_units,
            _join_excluding_with_next_token,
        ],
    )
    result_rel = pipeline.run()
    rows = [row[0] for row in result_rel.fetchall()]

    assert rows == [
        "EXCLUDINGBASEMENT 238 ALBION ROAD LONDON",
        "HOUSE EXCLUDINGBASEMENT 47 ALKHAM ROAD LONDON",
        "SHOP EXCLUDINGBASEMENT 1 TEST ROAD LONDON",
        "SHOP (EXCLUDINGBASEMENT) 2 TEST ROAD LONDON",
        "HOUSE EXCLUDINGSTUDIO 17 ASHTEAD ROAD LONDON",
        "SHOP EXCLUDINGGARAGE 1 TEST ROAD LONDON",
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


def test_first_pass_normalises_numeric_to_ranges(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('55 TO 57 OLD STREET ROAD'),
            ('FLAT 2 55 TO 57 OLD STREET ROAD'),
            ('NOT A RANGE TO 57 OLD STREET ROAD'),
            ('55 TO 57-59 OLD STREET ROAD'),
            ('1-55 TO 57 OLD STREET ROAD')
        ) AS t(clean_full_address)
        """
    )

    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=input_rel,
        stage_specs=[_clean_address_string_first_pass],
    )
    rows = [row[0] for row in pipeline.run().fetchall()]

    assert rows == [
        "55-57 OLD STREET ROAD",
        "FLAT 2 55-57 OLD STREET ROAD",
        "NOT A RANGE TO 57 OLD STREET ROAD",
        "55 TO 57-59 OLD STREET ROAD",
        "1-55 TO 57 OLD STREET ROAD",
    ]


def test_full_cleaning_queue_preserves_underscore_split_before_expansion(duck_con):
    input_rel = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('test-1', 1, 'EXCL_BSMT 41 LINTHORPE ROAD LONDON', NULL)
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

    assert rows == [("EXCLUDINGBASEMENT 41 LINTHORPE ROAD LONDON",)]


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
