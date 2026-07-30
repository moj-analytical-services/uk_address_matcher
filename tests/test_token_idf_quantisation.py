import duckdb
import pytest

from benchmarking.experiments.build_token_information_models import build_model_variants
from benchmarking.experiments.token_idf_quantisation import (
    TOKEN_IDF_QUANTISATION_SCALE,
    formula_equivalence_comparison,
    quantisation_equivalence_comparison,
    quantised_token_comparison,
    replace_token_comparison,
    settings_for_token_idf_variant,
)
from uk_address_matcher.linking_model.comparisons.shared_token_information import (
    quantised_shared_token_comparison,
    shared_token_information_q_sql,
)
from uk_address_matcher.linking_model.splink_model import _get_model_settings_dict


def test_formula_control_preserves_reachable_baseline_weights():
    comparison = formula_equivalence_comparison()

    levels = comparison["comparison_levels"]

    assert len(levels) == 31
    assert levels[0]["sql_condition"].endswith("> 29")
    assert levels[6]["m_probability"] == 6888.623433758429
    assert levels[-1]["sql_condition"] == "ELSE"
    assert levels[-1]["u_probability"] == 256


def test_quantisation_control_uses_integer_thresholds():
    comparison = quantisation_equivalence_comparison()

    levels = comparison["comparison_levels"]

    assert levels[0]["sql_condition"].endswith(f"> {29 * TOKEN_IDF_QUANTISATION_SCALE}")
    assert (
        levels[0]["m_probability"]
        == formula_equivalence_comparison()["comparison_levels"][0]["m_probability"]
    )
    assert "token_idf_q_hist_l" in levels[0]["sql_condition"]


def test_candidate_has_null_eight_non_null_levels_and_fixed_weights():
    comparison = quantised_token_comparison()

    levels = comparison["comparison_levels"]

    assert comparison["output_column_name"] == "shared_token_information"
    assert levels[0]["is_null_level"] is True
    assert len(levels) == 9
    assert [level["m_probability"] for level in levels[1:]] == [
        32768,
        8192,
        4096,
        256,
        16,
        2,
        1,
        0.5,
    ]
    assert all(level["u_probability"] == 1 for level in levels[1:])


def test_replace_token_comparison_is_immutable_and_supports_ablation():
    settings = _get_model_settings_dict()
    candidate = quantised_token_comparison()

    replaced = replace_token_comparison(settings, candidate)
    ablated = replace_token_comparison(settings, None)

    assert any(
        comparison["output_column_name"] == "token_rel_freq_arr_hist"
        for comparison in settings["comparisons"]
    )
    assert candidate in replaced["comparisons"]
    assert all(
        comparison["output_column_name"] != "token_rel_freq_arr_hist"
        for comparison in ablated["comparisons"]
    )


def test_settings_factory_leaves_baseline_default_and_builds_variants():
    assert settings_for_token_idf_variant("baseline") is None
    assert settings_for_token_idf_variant("quantised_candidate") is not None
    assert settings_for_token_idf_variant("ablation") is not None


def _quantised_map(entries: dict[str, tuple[int, int]]) -> str:
    keys = ", ".join(repr(key) for key in entries)
    values = ", ".join(
        "struct_pack("
        f"idf_q := {idf_q}::USMALLINT, "
        f"token_count := {token_count}::UTINYINT"
        ")"
        for idf_q, token_count in entries.values()
    )
    return f"MAP([{keys}], [{values}])"


def _score(
    left: dict[str, tuple[int, int]] | None, right: dict[str, tuple[int, int]] | None
):
    null_map = "NULL::MAP(VARCHAR, STRUCT(idf_q USMALLINT, token_count UTINYINT))"
    left_sql = null_map if left is None else _quantised_map(left)
    right_sql = null_map if right is None else _quantised_map(right)
    con = duckdb.connect()
    try:
        return con.sql(
            "SELECT "
            f"{shared_token_information_q_sql('left_map', 'right_map')} AS score_q "
            f"FROM (SELECT {left_sql} AS left_map, {right_sql} AS right_map)"
        ).fetchone()[0]
    finally:
        con.close()


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ({"ALPHA": (500, 1)}, {"BETA": (600, 1)}, 0),
        ({"ALPHA": (500, 1)}, {"ALPHA": (500, 1)}, 500),
        ({"ALPHA": (500, 3)}, {"ALPHA": (500, 2)}, 1000),
        (
            {"ALPHA": (500, 1), "BETA": (700, 1), "GAMMA": (900, 1)},
            {"ALPHA": (500, 1), "BETA": (700, 1), "GAMMA": (900, 1)},
            2100,
        ),
        ({"ALPHA": (600, 1)}, {"ALPHA": (500, 1)}, 500),
        ({}, {}, 0),
    ],
)
def test_shared_token_information_q_sql(left, right, expected):
    assert _score(left, right) == expected
    assert _score(left, right) == _score(right, left)


@pytest.mark.parametrize(
    ("score", "expected_bf"),
    [
        (0, 0.5),
        (1, 1.0),
        (256, 1.0),
        (257, 2.0),
        (1024, 2.0),
        (1025, 16.0),
        (2048, 16.0),
        (2049, 256.0),
        (3072, 256.0),
        (3073, 4096.0),
        (4096, 4096.0),
        (4097, 8192.0),
        (6144, 8192.0),
        (6145, 32768.0),
    ],
)
def test_quantised_shared_token_threshold_boundaries(score, expected_bf):
    comparison = quantised_shared_token_comparison()
    score_map = {"ALPHA": (score, 1)} if score else {}
    con = duckdb.connect()
    try:
        relation = con.sql(
            "SELECT "
            f"{_quantised_map(score_map)} AS token_idf_q_hist_l, "
            f"{_quantised_map(score_map)} AS token_idf_q_hist_r"
        )
        conditions = comparison["comparison_levels"]
        case_sql = (
            "CASE "
            + " ".join(
                f"WHEN {level['sql_condition']} THEN {level.get('m_probability', 'NULL')}"
                for level in conditions[:-1]
            )
            + f" ELSE {conditions[-1]['m_probability']} END"
        )
        assert relation.project(f"{case_sql} AS bf").fetchone()[0] == expected_bf
    finally:
        con.close()


def test_null_token_map_uses_the_null_gamma_level():
    comparison = quantised_shared_token_comparison()
    null_condition = comparison["comparison_levels"][0]["sql_condition"]
    con = duckdb.connect()
    try:
        result = con.sql(
            "SELECT CASE "
            f"WHEN {null_condition} THEN -1 "
            "ELSE 0 END AS gamma "
            "FROM (SELECT "
            "NULL::MAP(VARCHAR, STRUCT(idf_q USMALLINT, token_count UTINYINT)) "
            "AS token_idf_q_hist_l, "
            "MAP(['ALPHA'], [struct_pack(idf_q := 500::USMALLINT, "
            "token_count := 1::UTINYINT)]) AS token_idf_q_hist_r)"
        ).fetchone()[0]
    finally:
        con.close()

    assert result == -1


def test_quantised_scores_are_within_one_percent_of_precise_information():
    frequencies = [(1e-1, 1), (5e-5, 2), (1e-4, 1), (1e-7, 3)]
    precise = sum(
        -__import__("math").log10(frequency) * count for frequency, count in frequencies
    )
    quantised = (
        sum(
            round(-__import__("math").log10(frequency) * TOKEN_IDF_QUANTISATION_SCALE)
            * count
            for frequency, count in frequencies
        )
        / TOKEN_IDF_QUANTISATION_SCALE
    )

    assert abs(quantised - precise) / precise < 0.01


def test_generated_model_variants_are_valid_and_scoped(tmp_path, monkeypatch):
    from benchmarking.experiments import build_token_information_models

    monkeypatch.setattr(build_token_information_models, "VARIANTS_PATH", tmp_path)
    variants = build_model_variants()
    banded = variants["splink_model_quantised_banded.json"]
    ablated = variants["splink_model_no_shared_token_information.json"]

    banded_comparisons = [
        comparison
        for comparison in banded["comparisons"]
        if comparison.get("output_column_name") == "shared_token_information"
    ]
    assert len(banded_comparisons) == 1
    assert "token_rel_freq_arr_hist" not in str(banded)
    assert len(banded_comparisons[0]["comparison_levels"]) == 9
    assert all(
        level.get("fix_m_probability") is True and level.get("fix_u_probability") is True
        for level in banded_comparisons[0]["comparison_levels"][1:]
    )
    assert all(
        comparison.get("output_column_name") != "shared_token_information"
        for comparison in ablated["comparisons"]
    )
    paths = build_token_information_models.write_model_variants()
    assert {path.name for path in paths} == set(variants)
