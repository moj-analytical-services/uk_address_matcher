from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

from uk_address_matcher.post_linkage.match_result.result import (
    MatchResult,
    _build_threshold_metrics_sql,
)

# Sentinel values used by threshold metrics SQL for ±infinity
_NEG_INF = -999.0
_POS_INF = 999.0


def _run_threshold_metrics(
    matches: list[dict], canonical_ids: list[str]
) -> dict[float, dict]:
    """Register tables, run threshold-metrics SQL, return rows keyed by threshold."""
    con = duckdb.connect()
    con.register("__ukam_threshold_matches__", pa.Table.from_pylist(matches))
    con.register(
        "__ukam_threshold_canonical__",
        pa.Table.from_pylist([{"unique_id": uid} for uid in canonical_ids]),
    )
    rel = con.sql(_build_threshold_metrics_sql("m.match_weight"))
    cols = rel.columns
    rows = rel.fetchall()
    return {row[cols.index("truth_threshold")]: dict(zip(cols, row)) for row in rows}


def _make_result_without_ukam_label() -> MatchResult:
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                }
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))

    return MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_null_match_reason_maps_to_neg_inf():
    """NULL match_reason → truth_threshold of -999 (treated as −∞)."""
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": None,
                "ukam_label": "1",
                "match_weight": None,
                "match_reason": None,
            },
        ],
        canonical_ids=["1"],
    )
    assert _NEG_INF in rows


def test_deterministic_reasons_map_to_pos_inf():
    """All non-splink, non-NULL reasons → truth_threshold of +999 (treated as +∞)."""
    for reason in [
        "exact: full match",
        "peeled_address: match after removing common uk end tokens",
        "unique_trigram: unique trigram match",
    ]:
        rows = _run_threshold_metrics(
            matches=[
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": reason,
                },
            ],
            canonical_ids=["1"],
        )
        assert _POS_INF in rows, f"reason '{reason}' should map to +inf threshold"


def test_splink_match_uses_actual_weight():
    """Splink match_reason uses the real match_weight as the threshold."""
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": "1",
                "ukam_label": "1",
                "match_weight": 5.0,
                "match_reason": "splink: probabilistic match",
            },
        ],
        canonical_ids=["1"],
    )
    assert 5.0 in rows


def test_tp_fp_fn_tn_basic():
    """One exact TP, one splink TP, one true negative (no ukam_label).

    At threshold −999: all three rows are 'predicted positive'.
      Both labelled records match → TP=2; the unlabelled record → FP=1.
    At threshold 5.0 (splink weight): exact still above, splink at boundary.
      TP=2, FP=0, FN=0, TN=1.
    At threshold +999: only exact survives.
      TP=1, FP=0, FN=1 (splink match now below), TN=1.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": "1",
                "ukam_label": "1",
                "match_weight": None,
                "match_reason": "exact: full match",
            },
            {
                "unique_id": "b",
                "resolved_canonical_id": "2",
                "ukam_label": "2",
                "match_weight": 5.0,
                "match_reason": "splink: probabilistic match",
            },
            {
                "unique_id": "c",
                "resolved_canonical_id": None,
                "ukam_label": None,
                "match_weight": None,
                "match_reason": None,
            },
        ],
        canonical_ids=["1", "2"],
    )

    r = rows[_NEG_INF]
    assert (r["tp"], r["fp"], r["fn"], r["tn"]) == (2.0, 1.0, 0.0, 0.0)

    r = rows[5.0]
    assert (r["tp"], r["fp"], r["fn"], r["tn"]) == (2.0, 0.0, 0.0, 1.0)

    r = rows[_POS_INF]
    assert (r["tp"], r["fp"], r["fn"], r["tn"]) == (1.0, 0.0, 1.0, 1.0)


def test_wrong_canonical_match_keeps_emitted_weight_and_counts_fp():
    """Wrong canonical IDs keep the emitted splink score and count as FP.

    With top-1 semantics, wrong-ID rows are false positives at their emitted
    score (not floored to -999), while recall is still reduced because TP/P
    stays below 1.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "e",
                "resolved_canonical_id": "1",
                "ukam_label": "1",
                "match_weight": None,
                "match_reason": "exact: full match",
            },
            {
                "unique_id": "a",
                "resolved_canonical_id": "2",
                "ukam_label": "1",
                "match_weight": 8.0,
                "match_reason": "splink: probabilistic match",
            },
        ],
        canonical_ids=["1", "2"],
    )

    assert 8.0 in rows
    r = rows[8.0]
    assert r["tp"] == 1.0
    assert r["fp"] == 1.0
    assert r["fn"] == 1.0

    r = rows[_POS_INF]
    assert r["tp"] == 1.0
    assert r["fp"] == 0.0


def test_label_without_canonical_is_not_a_false_negative():
    """A ukam_label with no corresponding canonical record is not matchable,
    so an unmatched result is a TN, not a FN.

    A second record with a real splink score anchors a threshold above -999,
    at which the unmatchable record falls into TN.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": None,
                "ukam_label": "99",
                "match_weight": None,
                "match_reason": None,
            },  # "99" not in canonical
            {
                "unique_id": "b",
                "resolved_canonical_id": "1",
                "ukam_label": "1",
                "match_weight": 5.0,
                "match_reason": "splink: probabilistic match",
            },
        ],
        canonical_ids=["1"],
    )

    # At threshold 5.0, record 'a' (score=-999) is predicted negative; its label
    # has no canonical entry so it is clerical_negative → TN, not FN.
    r = rows[5.0]
    assert r["fn"] == 0.0
    assert r["tn"] == 1.0


def test_wrong_match_threshold_transition_fp_then_fn_only():
    """A wrong-ID splink decision transitions from FP+FN to FN-only as threshold rises.

    Why this is correct under top-1 record-level evaluation:

    - At thresholds that ACCEPT the wrong decision score, we emit a wrong match,
      so the record contributes a false positive; and because TP is still not
      achieved for that labelled positive, it also contributes to false negatives
      via FN = P - TP.
    - At thresholds ABOVE the wrong score, the wrong decision is rejected,
      removing the false positive, but the true link is still unrecovered,
      so it remains a false negative.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "good",
                "resolved_canonical_id": "1",
                "ukam_label": "1",
                "match_weight": None,
                "match_reason": "exact: full match",
            },
            {
                "unique_id": "wrong",
                "resolved_canonical_id": "2",
                "ukam_label": "1",
                "match_weight": 15.0,
                "match_reason": "splink: probabilistic match",
            },
        ],
        canonical_ids=["1", "2"],
    )

    # Equivalent to using threshold > 14 (decision score 15 is accepted)
    r = rows[15.0]
    assert r["tp"] == 1.0
    assert r["fp"] == 1.0
    assert r["fn"] == 1.0

    # Equivalent to using threshold > 16 (score 15 is rejected)
    # represented by the next higher threshold bucket (+999)
    r = rows[_POS_INF]
    assert r["tp"] == 1.0
    assert r["fp"] == 0.0
    assert r["fn"] == 1.0


def test_fp_all_vs_fp_neg_differ_for_wrong_id():
    """fp (fp_all) and fp_neg diverge when a matchable row gets a wrong-ID decision.

    The wrong-ID row is a matchable record (clerical_positive=1) matched to a
    different canonical ID.  It counts in ``fp`` (accepted non-TP) but NOT in
    ``fp_neg`` (only genuine negatives wrongly accepted), so:

        fp > fp_neg  at the score where the wrong decision is accepted.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": "2",  # wrong canonical
                "ukam_label": "1",  # should be "1"
                "match_weight": 10.0,
                "match_reason": "splink: probabilistic match",
            },
        ],
        canonical_ids=["1", "2"],
    )

    r = rows[10.0]
    # fp_all = 1 (wrong-ID accepted); fp_neg = 0 (no genuine negatives accepted)
    assert r["fp"] == 1.0
    assert r["fp_neg"] == 0.0
    assert r["tn"] == 0.0  # n=0, so tn=GREATEST(0-0,0)=0, never negative


def test_tn_never_negative():
    """TN must be >= 0 at every threshold, including when all rows are matchable.

    When the entire dataset is matchable (no genuine negatives, n=0), the old
    formula tn = n - fp would yield 0 - fp < 0.  The fix clamps with GREATEST.
    """
    rows = _run_threshold_metrics(
        matches=[
            {
                "unique_id": "a",
                "resolved_canonical_id": "2",  # wrong canonical
                "ukam_label": "1",
                "match_weight": 5.0,
                "match_reason": "splink: probabilistic match",
            },
            {
                "unique_id": "b",
                "resolved_canonical_id": "2",
                "ukam_label": "2",
                "match_weight": None,
                "match_reason": "exact: full match",
            },
        ],
        canonical_ids=["1", "2"],  # all rows are matchable → n=0
    )

    for threshold, row in rows.items():
        assert row["tn"] >= 0.0, f"tn < 0 at threshold {threshold}: {row['tn']}"


def test_accuracy_analysis_rejects_roc_output_type():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                }
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    with pytest.raises(ValueError, match="Invalid output_type"):
        result.accuracy_analysis(output_type="roc")


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        ("_accuracy_table", {}),
        (
            "_compare_splink_model_results",
            {
                "baseline_match_weight": 10.0,
                "precision_at_metrics": None,
            },
        ),
    ],
)
def test_match_result_analysis_methods_require_ukam_label(method_name, kwargs):
    result = _make_result_without_ukam_label()
    method = getattr(result, method_name)

    with pytest.raises(
        ValueError,
        match=f"{method_name} requires a 'ukam_label' column",
    ):
        method(**kwargs)


def test_compare_splink_model_results_requires_ukam_label_without_top_k_metrics():
    result = _make_result_without_ukam_label()

    with pytest.raises(
        ValueError,
        match="_compare_splink_model_results requires a 'ukam_label' column",
    ):
        result._compare_splink_model_results(
            baseline_match_weight=10.0,
            precision_at_metrics=None,
        )


def test_accuracy_table_supports_splink_threshold_override():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": "9",
                    "ukam_label": "3",
                    "match_weight": 8.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register(
        "c",
        pa.Table.from_pylist(
            [{"unique_id": "1"}, {"unique_id": "2"}, {"unique_id": "3"}],
        ),
    )

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    default_df = result._accuracy_table().df()
    override_df = result._accuracy_table(splink_match_weight_threshold=10.0).df()

    default_overall = default_df.loc[default_df["stage"] == "overall"].iloc[0]
    override_overall = override_df.loc[override_df["stage"] == "overall"].iloc[0]

    assert int(default_overall["rows_matched_in_stage"]) == 3
    assert int(default_overall["correct_matches"]) == 2
    assert pytest.approx(float(default_overall["precision"])) == 2.0 / 3.0
    assert pytest.approx(float(default_overall["recall"])) == 2.0 / 3.0

    assert int(override_overall["rows_matched_in_stage"]) == 2
    assert int(override_overall["correct_matches"]) == 2
    assert pytest.approx(float(override_overall["precision"])) == 1.0
    assert pytest.approx(float(override_overall["recall"])) == 2.0 / 3.0


def test_accuracy_table_probability_threshold_matches_weight_threshold():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "1",
                    "match_weight": 8.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}, {"unique_id": "2"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    by_weight = result._accuracy_table(splink_match_weight_threshold=10.0).df()
    by_probability = result._accuracy_table(
        splink_match_probability_threshold=0.999,
    ).df()

    weight_overall = by_weight.loc[by_weight["stage"] == "overall"].iloc[0]
    probability_overall = by_probability.loc[by_probability["stage"] == "overall"].iloc[0]

    assert int(weight_overall["rows_matched_in_stage"]) == int(
        probability_overall["rows_matched_in_stage"]
    )
    assert int(weight_overall["correct_matches"]) == int(
        probability_overall["correct_matches"]
    )


def test_accuracy_table_probability_threshold_rejects_out_of_range_values():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                }
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    with pytest.raises(
        ValueError,
        match="must be between 0.0 and 1.0 inclusive",
    ):
        result._accuracy_table(splink_match_probability_threshold=-0.01).df()

    with pytest.raises(
        ValueError,
        match="must be between 0.0 and 1.0 inclusive",
    ):
        result._accuracy_table(splink_match_probability_threshold=1.01).df()


def test_stage_diagnostics_includes_flow_and_timing_columns_only():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": 11.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}, {"unique_id": "2"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _stage_diagnostics=[
            {
                "stage": "exact_matches",
                "unmatched_before": 2,
                "matched_this_stage": 1,
                "remaining_after": 1,
                "matched_pct_of_unmatched": 0.5,
                "matched_pct_of_input": 0.5,
                "elapsed_seconds": 0.1,
            },
            {
                "stage": "splink",
                "unmatched_before": 1,
                "matched_this_stage": 1,
                "remaining_after": 0,
                "matched_pct_of_unmatched": 1.0,
                "matched_pct_of_input": 0.5,
                "elapsed_seconds": 0.2,
            },
        ],
    )

    diagnostics_df = result._stage_diagnostics_table().df()
    assert diagnostics_df.columns[0] == "stage"
    assert diagnostics_df.columns[1] == "stage_order"
    assert list(diagnostics_df["stage"]) == ["exact_matches", "splink"]
    assert list(diagnostics_df["stage_order"]) == [0, 1]
    assert "rows_entering_stage" in diagnostics_df.columns
    assert "rows_matched_in_stage" in diagnostics_df.columns
    assert "rows_remaining_after_stage" not in diagnostics_df.columns
    assert "stage_match_rate" in diagnostics_df.columns
    assert "share_of_total_input_matched" in diagnostics_df.columns
    assert "elapsed_seconds" in diagnostics_df.columns
    assert "precision" not in diagnostics_df.columns
    assert "recall" not in diagnostics_df.columns
    assert "f1" not in diagnostics_df.columns


def test_stage_diagnostics_without_labels_still_returns_timing_rows():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                }
            ]
        ),
    )

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _stage_diagnostics=[
            {
                "stage": "exact_matches",
                "unmatched_before": 1,
                "matched_this_stage": 1,
                "remaining_after": 0,
                "matched_pct_of_unmatched": 1.0,
                "matched_pct_of_input": 1.0,
                "elapsed_seconds": 0.03,
            }
        ],
    )

    diagnostics_df = result._stage_diagnostics_table().df()
    assert list(diagnostics_df["stage"]) == ["exact_matches"]
    assert list(diagnostics_df["stage_order"]) == [0]
    assert float(diagnostics_df.iloc[0]["elapsed_seconds"]) == pytest.approx(0.03)


def test_accuracy_table_keeps_overall_without_total_row():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": None,
                    "ukam_label": "2",
                    "match_weight": None,
                    "match_reason": None,
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}, {"unique_id": "2"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    accuracy_df = result._accuracy_table().df()
    overall = accuracy_df.loc[accuracy_df["stage"] == "overall"].iloc[0]
    assert int(overall["rows_matched_in_stage"]) == 1
    assert "total" not in set(accuracy_df["stage"])
    assert "rows_entering_stage" not in accuracy_df.columns
    assert "total_input_rows" not in accuracy_df.columns


def test_compare_splink_model_results_returns_headline_and_delta_tables():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": "9",
                    "ukam_label": "3",
                    "match_weight": 8.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register(
        "c",
        pa.Table.from_pylist(
            [{"unique_id": "1"}, {"unique_id": "2"}, {"unique_id": "3"}],
        ),
    )

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    comparison_output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=[8.0, 10.0],
    )

    headline_df = comparison_output.headline_table.df()
    delta_df = comparison_output.delta_table.df()

    assert comparison_output.total_input_rows == 3

    assert "scenario" in headline_df.columns
    assert "threshold" not in headline_df.columns
    assert "match_outcome" in headline_df.columns
    assert "match_rate" not in headline_df.columns
    assert "correct_matches" not in headline_df.columns
    assert "mismatched_matches" not in headline_df.columns
    assert "precision" in headline_df.columns
    assert "recall" in headline_df.columns
    assert "f1" in headline_df.columns
    assert "interpretation" not in headline_df.columns
    assert "total_input_rows" not in headline_df.columns
    assert "accuracy" not in headline_df.columns

    assert "scenario" in delta_df.columns
    assert "delta_matched_rows" not in delta_df.columns
    assert "delta_correct_matches" not in delta_df.columns
    assert "delta_mismatched_matches" not in delta_df.columns
    assert "delta_match_outcome" in delta_df.columns
    assert "precision_delta" in delta_df.columns
    assert "recall_delta" in delta_df.columns
    assert "f1_delta" in delta_df.columns

    baseline_rows = delta_df[delta_df["scenario"] == "weight_10.0 (baseline)"]
    assert not baseline_rows.empty
    assert (
        baseline_rows["delta_match_outcome"]
        .str.contains(r"matched 1 \| correct 1 \| wrong 0", regex=True)
        .all()
    )
    assert baseline_rows["precision_delta"].eq("100.0%").all()
    assert baseline_rows["recall_delta"].eq("33.33%").all()
    assert baseline_rows["f1_delta"].eq("50.0%").all()


def test_compare_splink_top_k_results_returns_headline_and_delta_tables():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": None,
                    "ukam_label": "3",
                    "match_weight": None,
                    "match_reason": None,
                },
            ]
        ),
    )
    con.register(
        "c",
        pa.Table.from_pylist(
            [{"unique_id": "1"}, {"unique_id": "2"}, {"unique_id": "3"}],
        ),
    )
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 12.0,
                    "match_probability": 0.999,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "9",
                    "match_weight": 11.0,
                    "match_probability": 0.98,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "2",
                    "match_weight": 9.0,
                    "match_probability": 0.9,
                },
                {
                    "unique_id_r": "c",
                    "unique_id_l": "8",
                    "match_weight": 7.0,
                    "match_probability": 0.8,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    comparison_output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=[8.0, 10.0],
        precision_at_metrics=[5, 1, 3],
    )

    headline_df = comparison_output.headline_table.df()
    delta_df = comparison_output.delta_table.df()

    assert comparison_output.total_input_rows == 3

    assert "scenario" in headline_df.columns
    assert "threshold" not in headline_df.columns
    assert "match_outcome" in headline_df.columns
    assert "rows_with_true_match" in headline_df.columns
    assert "rows_with_predictions" in headline_df.columns
    assert "precision_at_k" in headline_df.columns
    assert "recall_at_k" in headline_df.columns
    assert "mean_reciprocal_rank" in headline_df.columns
    assert "average_true_rank" in headline_df.columns

    assert "scenario" in delta_df.columns
    assert "rows_with_predictions_delta" in delta_df.columns
    assert "precision_at_k_delta" in delta_df.columns
    assert "recall_at_k_delta" in delta_df.columns
    assert "mrr_delta" in delta_df.columns
    assert "average_true_rank_delta" in delta_df.columns

    baseline_rows = headline_df[headline_df["scenario"] == "weight_10.0 (baseline)"]
    assert not baseline_rows.empty
    assert baseline_rows["rows_with_true_match"].eq(3).all()
    assert baseline_rows["rows_with_predictions"].eq(2).all()
    assert baseline_rows["match_outcome"].eq("matched 1 | correct 1 | wrong 0").all()
    assert baseline_rows["precision_at_k"].str.contains("@1 50.0%", regex=False).all()
    assert baseline_rows["precision_at_k"].str.contains("@3 50.0%", regex=False).all()
    assert baseline_rows["precision_at_k"].str.contains("@5 50.0%", regex=False).all()
    assert baseline_rows["recall_at_k"].str.contains("@1 33.33%", regex=False).all()
    assert baseline_rows["recall_at_k"].str.contains("@3 33.33%", regex=False).all()
    assert baseline_rows["recall_at_k"].str.contains("@5 33.33%", regex=False).all()
    assert baseline_rows["mean_reciprocal_rank"].eq(0.333333).all()
    assert baseline_rows["average_true_rank"].eq(1.0).all()

    comparison_rows = delta_df[delta_df["scenario"] == "weight_8.0"]
    assert not comparison_rows.empty
    assert comparison_rows["rows_with_predictions_delta"].eq("0").all()
    assert comparison_rows["precision_at_k_delta"].str.contains("@1 0.00 pp").all()
    assert (
        comparison_rows["precision_at_k_delta"]
        .str.contains(
            "@3 +50.00 pp",
            regex=False,
        )
        .all()
    )
    assert (
        comparison_rows["precision_at_k_delta"]
        .str.contains(
            "@5 +50.00 pp",
            regex=False,
        )
        .all()
    )
    assert comparison_rows["recall_at_k_delta"].str.contains("@1 0.00 pp").all()
    assert (
        comparison_rows["recall_at_k_delta"]
        .str.contains(
            "@3 +33.33 pp",
            regex=False,
        )
        .all()
    )
    assert (
        comparison_rows["recall_at_k_delta"]
        .str.contains(
            "@5 +33.33 pp",
            regex=False,
        )
        .all()
    )
    assert comparison_rows["mrr_delta"].eq("+0.167").all()
    assert comparison_rows["average_true_rank_delta"].eq("+0.500").all()

    baseline_delta_rows = delta_df[delta_df["scenario"] == "weight_10.0 (baseline)"]
    assert not baseline_delta_rows.empty
    assert baseline_delta_rows["rows_with_predictions_delta"].eq("2").all()
    assert (
        baseline_delta_rows["precision_at_k_delta"]
        .str.contains("@1 50.0%", regex=False)
        .all()
    )
    assert (
        baseline_delta_rows["recall_at_k_delta"]
        .str.contains("@1 33.33%", regex=False)
        .all()
    )
    assert baseline_delta_rows["mrr_delta"].eq("0.333").all()
    assert baseline_delta_rows["average_true_rank_delta"].eq("1.000").all()


def test_compare_splink_top_k_results_supports_custom_precision_metrics():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 12.0,
                    "match_probability": 0.999,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )
    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    comparison_output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=None,
        precision_at_metrics=[5, 1],
    )

    headline_df = comparison_output.headline_table.df()
    assert "precision_at_k" in headline_df.columns
    assert "@1" in headline_df.iloc[0]["precision_at_k"]
    assert "@5" in headline_df.iloc[0]["precision_at_k"]
    assert "average_true_rank" in headline_df.columns


def test_compare_splink_top_k_results_machine_readable_delta_columns():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": None,
                    "ukam_label": "3",
                    "match_weight": None,
                    "match_reason": None,
                },
            ]
        ),
    )
    con.register(
        "c",
        pa.Table.from_pylist(
            [{"unique_id": "1"}, {"unique_id": "2"}, {"unique_id": "3"}],
        ),
    )
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 12.0,
                    "match_probability": 0.999,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "9",
                    "match_weight": 11.0,
                    "match_probability": 0.98,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "2",
                    "match_weight": 9.0,
                    "match_probability": 0.9,
                },
                {
                    "unique_id_r": "c",
                    "unique_id_l": "8",
                    "match_weight": 7.0,
                    "match_probability": 0.8,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )
    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=[8.0, 10.0],
        precision_at_metrics=[1, 3, 5],
        human_readable=False,
    )

    headline_df = output.headline_table.df()
    delta_df = output.delta_table.df()

    assert "precision_at_k" not in headline_df.columns
    assert "recall_at_k" not in headline_df.columns
    assert "precision_at_1" in headline_df.columns
    assert "precision_at_3" in headline_df.columns
    assert "precision_at_5" in headline_df.columns
    assert "recall_at_1" in headline_df.columns
    assert "recall_at_3" in headline_df.columns
    assert "recall_at_5" in headline_df.columns

    assert "delta_match_outcome" not in delta_df.columns
    assert "matched_rows_delta" in delta_df.columns
    assert "correct_matches_delta" in delta_df.columns
    assert "mismatched_matches_delta" in delta_df.columns
    assert "precision_delta" in delta_df.columns
    assert "recall_delta" in delta_df.columns
    assert "f1_delta" in delta_df.columns
    assert "rows_with_predictions_delta" in delta_df.columns
    assert "precision_at_1_delta" in delta_df.columns
    assert "precision_at_3_delta" in delta_df.columns
    assert "precision_at_5_delta" in delta_df.columns
    assert "recall_at_1_delta" in delta_df.columns
    assert "recall_at_3_delta" in delta_df.columns
    assert "recall_at_5_delta" in delta_df.columns
    assert "mrr_delta" in delta_df.columns
    assert "average_true_rank_delta" in delta_df.columns

    assert (
        delta_df.loc[
            delta_df["scenario"] == "weight_10.0 (baseline)", "matched_rows_delta"
        ]
        .isna()
        .all()
    )
    assert (
        delta_df.loc[delta_df["scenario"] == "weight_10.0 (baseline)", "precision_delta"]
        .isna()
        .all()
    )


def test_top_k_perfect_ranking_yields_unit_metrics():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": "3",
                    "ukam_label": "3",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 12.0,
                    "match_probability": 0.99,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "2",
                    "match_weight": 11.0,
                    "match_probability": 0.98,
                },
                {
                    "unique_id_r": "c",
                    "unique_id_l": "3",
                    "match_weight": 10.5,
                    "match_probability": 0.97,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )
    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=None,
        precision_at_metrics=[1, 3, 5],
    )
    df = output.headline_table.df()
    row = df.loc[df["scenario"] == "weight_10.0 (baseline)"].iloc[0]

    assert "@1 100.0%" in row["precision_at_k"]
    assert "@3 100.0%" in row["precision_at_k"]
    assert "@5 100.0%" in row["precision_at_k"]
    assert "@1 100.0%" in row["recall_at_k"]
    assert "@3 100.0%" in row["recall_at_k"]
    assert "@5 100.0%" in row["recall_at_k"]
    assert row["mean_reciprocal_rank"] == 1.0
    assert row["average_true_rank"] == 1.0


def test_top_k_worse_ranking_has_higher_average_rank_and_lower_mrr():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "2",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "9",
                    "match_weight": 12.0,
                    "match_probability": 0.99,
                },
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 11.0,
                    "match_probability": 0.98,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "8",
                    "match_weight": 12.0,
                    "match_probability": 0.99,
                },
                {
                    "unique_id_r": "b",
                    "unique_id_l": "2",
                    "match_weight": 11.0,
                    "match_probability": 0.98,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )
    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=None,
        precision_at_metrics=[1, 3, 5],
    )
    df = output.headline_table.df()
    row = df.loc[df["scenario"] == "weight_10.0 (baseline)"].iloc[0]

    assert "@1 0.0%" in row["precision_at_k"]
    assert "@3 100.0%" in row["precision_at_k"]
    assert "@5 100.0%" in row["precision_at_k"]
    assert "@1 0.0%" in row["recall_at_k"]
    assert "@3 100.0%" in row["recall_at_k"]
    assert "@5 100.0%" in row["recall_at_k"]
    assert row["mean_reciprocal_rank"] == 0.5
    assert row["average_true_rank"] == 2.0


def test_compare_splink_model_results_omits_top_k_when_no_precision_metrics():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    comparison_output = result._compare_splink_model_results(
        baseline_match_weight=10.0,
        splink_comparison_weights=None,
        precision_at_metrics=None,
    )
    headline_df = comparison_output.headline_table.df()
    delta_df = comparison_output.delta_table.df()

    assert "precision@1" not in headline_df.columns
    assert "precision_at_k" not in headline_df.columns
    assert "recall_at_k" not in headline_df.columns
    assert "mean_reciprocal_rank" not in headline_df.columns
    assert "average_true_rank" not in headline_df.columns
    assert "precision_at_k_delta" not in delta_df.columns
    assert "recall_at_k_delta" not in delta_df.columns
    assert "mrr_delta" not in delta_df.columns
    assert "average_true_rank_delta" not in delta_df.columns


@pytest.mark.parametrize("precision_at_metrics", [[0], [-1], [11], [1, 12]])
def test_compare_splink_model_results_enforces_top_k_bounds(precision_at_metrics):
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": 12.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register("c", pa.Table.from_pylist([{"unique_id": "1"}]))
    con.register(
        "splink_predictions",
        pa.Table.from_pylist(
            [
                {
                    "unique_id_r": "a",
                    "unique_id_l": "1",
                    "match_weight": 12.0,
                    "match_probability": 0.99,
                },
            ]
        ),
    )

    fake_splink_stage = SimpleNamespace(
        predictions_table="splink_predictions",
        linker=object(),
    )
    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
        _splink_stage=fake_splink_stage,
    )

    with pytest.raises(ValueError, match="between 1 and 10 inclusive"):
        result._compare_splink_model_results(
            baseline_match_weight=10.0,
            splink_comparison_weights=None,
            precision_at_metrics=precision_at_metrics,
        )


def test_accuracy_table_is_quality_focused_without_flow_columns():
    con = duckdb.connect()
    con.register(
        "m",
        pa.Table.from_pylist(
            [
                {
                    "unique_id": "a",
                    "resolved_canonical_id": "1",
                    "ukam_label": "1",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "b",
                    "resolved_canonical_id": "2",
                    "ukam_label": "9",
                    "match_weight": None,
                    "match_reason": "exact: full match",
                },
                {
                    "unique_id": "c",
                    "resolved_canonical_id": "3",
                    "ukam_label": "3",
                    "match_weight": 11.0,
                    "match_reason": "splink: probabilistic match",
                },
                {
                    "unique_id": "d",
                    "resolved_canonical_id": "8",
                    "ukam_label": "4",
                    "match_weight": 9.0,
                    "match_reason": "splink: probabilistic match",
                },
            ]
        ),
    )
    con.register(
        "c",
        pa.Table.from_pylist(
            [
                {"unique_id": "1"},
                {"unique_id": "2"},
                {"unique_id": "3"},
                {"unique_id": "4"},
            ],
        ),
    )

    result = MatchResult(
        _relation=con.table("m"),
        con=con,
        _canonical_relation=con.table("c"),
    )

    table_df = result._accuracy_table(splink_match_weight_threshold=10.0).df()

    exact = table_df.loc[table_df["stage"] == "exact_matches"].iloc[0]
    splink = table_df.loc[table_df["stage"] == "splink"].iloc[0]

    assert int(exact["rows_matched_in_stage"]) == 2
    assert int(splink["rows_matched_in_stage"]) == 1
    assert int(exact["wrong_matches"]) == 1
    assert int(splink["wrong_matches"]) == 0

    # Flow metrics are owned by stage diagnostics, not stage accuracy.
    assert "rows_entering_stage" not in table_df.columns
    assert "stage_match_rate" not in table_df.columns
    assert "share_of_total_input_matched" not in table_df.columns
