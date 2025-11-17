import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

from uk_address_matcher.cleaning.steps import _normalise_abbreviations_and_units
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


@pytest.fixture
def test_abbr_data(duck_con):
    """Set up test data as DuckDB PyRelations for exact matching tests."""
    return duck_con.sql("""
        SELECT * FROM (VALUES
            ('42 HIGH ST LONDON'),
            ('FLAT 3 APT 5 MANOR RD GLASGOW'),
            ('UNIT 10 OFFICE SUITE 200 BLVD DRIVE BIRMINGHAM'),
            ('123 TOWNHALL STREET CENTRE MANCHESTER'),
            ('THE COTTAGE WOOD LANE BRISTOL'),
            ('FACTORY WORKS WKS INDUSTRIAL ESTATE RD'),
            ('MUSEUM GALLERY THEATRE THEA CIVIC CENTRE'),
            ('FLAT 3D 12B BAKER AVE LONDON'),
            ('PENTHOUSE 1A OXFORD CLS LONDON'),
        ) AS t(clean_full_address)
    """)


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
        "42 HIGH STREET LONDON",
        "FLAT 3 FLAT 5 MANOR ROAD GLASGOW",
        "FLAT 10 OFFICE SUITE 200 BOULEVARD DRIVE BIRMINGHAM",
        "123 TOWN HALL STREET CENTRE MANCHESTER",
        "THE COTTAGE WOOD LANE BRISTOL",
        "FACTORY WORKS WORKS INDUSTRIAL ESTATE ROAD",
        "MUSEUM GALLERY THEATRE THEATRE CIVIC CENTRE",
        "FLAT 3D 12B BAKER AVENUE LONDON",
        "FLAT 1A OXFORD CLOSE LONDON",
    ]
    for row, expected in zip(rows, expected_addresses):
        assert row[clean_idx] == expected


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
                f"{t_norm!r}: ({prev_raw!r}->{prev_rep!r}) vs ({row['token']!r}->{row['replacement']!r})"
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
