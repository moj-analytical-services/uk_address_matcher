from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from uk_address_matcher.analysis.accuracy_sql import (
    average_rank_within_k_sql,
    hit_rate_at_k_sql,
    precision_at_k_sql,
    ratio_sql,
    reciprocal_rank_mean_sql,
)
from uk_address_matcher.analysis.accuracy_table import build_accuracy_table
from uk_address_matcher.analysis.sql_helpers import (
    baseline_or_delta_sql,
    percent_string_sql,
    signed_count_delta_sql,
    signed_pp_delta_sql,
    sql_literal,
)
from uk_address_matcher.analysis.validation import requires_ukam_label

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class SplinkModelComparisonOutput:
    """Display-focused Splink threshold comparison output.

    This output combines top-1 emitted-decision quality with optional Splink
    top-k retrieval/ranking metrics into two tables:

    - headline_table: per-threshold absolute metrics
    - delta_table: baseline values for the baseline row and signed deltas for
      non-baseline rows (all rendered as VARCHAR)

    Args accepted by the builder:
        baseline_match_weight: The reference Splink match-weight threshold.
        splink_comparison_weights: Optional additional thresholds to compare.
        predictions_relation: Optional Splink predictions relation used when
            top-k metrics are requested.
        precision_at_metrics: Optional top-k cutoffs. When omitted, no top-k
            columns are included.

    Returned fields:
        headline_table: DuckDB relation with baseline/comparison metric values.
        delta_table: DuckDB relation with baseline values or deltas per metric.
        total_input_rows: Number of rows in the labelled input relation.
    """

    headline_table: duckdb.DuckDBPyRelation
    delta_table: duckdb.DuckDBPyRelation
    total_input_rows: int | None


def _resolve_precision_at_metrics(
    precision_at_metrics: list[int] | None,
) -> list[int] | None:
    if precision_at_metrics is None:
        return None

    requested_metrics = precision_at_metrics
    for metric in requested_metrics:
        if not isinstance(metric, int) or isinstance(metric, bool):
            raise ValueError(
                f"precision_at_metrics entries must be integers, got '{metric}'."
            )
        if metric < 1 or metric > 10:
            raise ValueError(
                "precision_at_metrics entries must be between 1 and 10 "
                f"inclusive, got '{metric}'."
            )

    unique_sorted = sorted(set(requested_metrics))
    if not unique_sorted:
        raise ValueError("precision_at_metrics must contain at least one cutoff.")
    return unique_sorted


def _resolve_first_present_column(
    *,
    columns: list[str],
    candidates: list[str],
    missing_message: str,
) -> str:
    for column in candidates:
        if column in columns:
            return column
    raise ValueError(missing_message)


def _resolve_prediction_input_id_column(predictions: duckdb.DuckDBPyRelation) -> str:
    return _resolve_first_present_column(
        columns=predictions.columns,
        candidates=["unique_id_r", "unique_id", "ukam_address_id_r", "ukam_address_id"],
        missing_message=(
            "Splink predictions table is missing an input ID column for top-k analysis."
        ),
    )


def _resolve_prediction_candidate_id_column(
    predictions: duckdb.DuckDBPyRelation,
) -> str:
    return _resolve_first_present_column(
        columns=predictions.columns,
        candidates=["unique_id_l", "unique_id", "ukam_address_id_l"],
        missing_message=(
            "Splink predictions table is missing a candidate ID column "
            "for top-k analysis."
        ),
    )


def _resolve_label_input_id_expression(
    *,
    labelled_columns: set[str],
    prediction_input_id_column: str,
) -> str:
    if prediction_input_id_column in {"unique_id_r", "unique_id"}:
        if "unique_id" in labelled_columns:
            return "CAST(unique_id AS VARCHAR)"
        if "ukam_address_id" in labelled_columns:
            return "CAST(ukam_address_id AS VARCHAR)"
    if prediction_input_id_column in {"ukam_address_id_r", "ukam_address_id"}:
        if "ukam_address_id" in labelled_columns:
            return "CAST(ukam_address_id AS VARCHAR)"
        if "unique_id" in labelled_columns:
            return "CAST(unique_id AS VARCHAR)"
    raise ValueError(
        "Top-k analysis could not find a compatible input ID column in match results."
    )


def _build_scenario_definitions(
    *,
    baseline_match_weight: float,
    splink_comparison_weights: list[float] | None,
) -> list[tuple[int, float, str]]:
    baseline_value = float(baseline_match_weight)
    scenarios: list[tuple[int, float, str]] = [
        (0, baseline_value, f"weight_{baseline_value}")
    ]

    unique_comparison_weights: list[float] = []
    if splink_comparison_weights is not None:
        seen_weights: set[float] = set()
        for weight in splink_comparison_weights:
            weight_value = float(weight)
            if weight_value == baseline_value or weight_value in seen_weights:
                continue
            seen_weights.add(weight_value)
            unique_comparison_weights.append(weight_value)

    for index, threshold_value in enumerate(sorted(unique_comparison_weights), start=1):
        scenarios.append((index, threshold_value, f"weight_{threshold_value}"))
    return scenarios


def _build_top_k_scenario_metrics_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    labelled_relation: duckdb.DuckDBPyRelation,
    predictions_relation: duckdb.DuckDBPyRelation,
    threshold_match_weight: float,
    scenario_order: int,
    scenario_label: str,
    precision_at_cutoffs: list[int],
) -> duckdb.DuckDBPyRelation:
    if "match_weight" not in predictions_relation.columns:
        raise ValueError(
            "Splink predictions table is missing required column 'match_weight'."
        )

    labelled_columns = set(labelled_relation.columns)
    prediction_input_id_column = _resolve_prediction_input_id_column(predictions_relation)
    prediction_candidate_id_column = _resolve_prediction_candidate_id_column(
        predictions_relation
    )
    label_input_id_expr = _resolve_label_input_id_expression(
        labelled_columns=labelled_columns,
        prediction_input_id_column=prediction_input_id_column,
    )

    precision_select_parts: list[str] = []
    recall_select_parts: list[str] = []
    for cutoff in precision_at_cutoffs:
        precision_ratio_sql = precision_at_k_sql(
            rank_sql="true_rank",
            k=cutoff,
            has_predictions_sql="has_predictions",
        )
        precision_select_parts.append(
            f"            ROUND({precision_ratio_sql}, 6) AS precision_at_{cutoff}"
        )
        recall_select_parts.append(
            (
                "            ROUND("
                + hit_rate_at_k_sql(rank_sql="true_rank", k=cutoff)
                + f", 6) AS recall_at_{cutoff}"
            )
        )
    precision_select_sql = ",\n".join(precision_select_parts)
    recall_select_sql = ",\n".join(recall_select_parts)
    max_cutoff = max(precision_at_cutoffs)

    return con.sql(
        f"""
        WITH labels AS (
            SELECT DISTINCT
                {label_input_id_expr} AS input_id,
                CAST(ukam_label AS VARCHAR) AS true_candidate_id
            FROM ({labelled_relation.sql_query()}) AS m
            WHERE ukam_label IS NOT NULL
        ),
        ranked_predictions AS (
            SELECT
                CAST(pred.{prediction_input_id_column} AS VARCHAR) AS input_id,
                CAST(pred.{prediction_candidate_id_column} AS VARCHAR)
                    AS candidate_id,
                pred.match_weight,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(pred.{prediction_input_id_column} AS VARCHAR)
                    ORDER BY
                        pred.match_weight DESC,
                        CAST(pred.{prediction_candidate_id_column} AS VARCHAR) ASC
                ) AS candidate_rank
            FROM ({predictions_relation.sql_query()}) AS pred
            WHERE pred.match_weight >= {threshold_match_weight}
        ),
        predicted_inputs AS (
            SELECT DISTINCT input_id
            FROM ranked_predictions
        ),
        true_ranks AS (
            SELECT
                l.input_id,
                MIN(r.candidate_rank) AS true_rank,
                CASE WHEN p.input_id IS NULL THEN 0 ELSE 1 END AS has_predictions
            FROM labels AS l
            LEFT JOIN ranked_predictions AS r
                ON r.input_id = l.input_id
                AND r.candidate_id = l.true_candidate_id
            LEFT JOIN predicted_inputs AS p
                ON p.input_id = l.input_id
            GROUP BY l.input_id, p.input_id
        )
        SELECT
            {scenario_order}::INTEGER AS scenario_order,
            '{sql_literal(scenario_label)}' AS scenario,
            {threshold_match_weight}::DOUBLE AS threshold_match_weight,
            COUNT(*)::BIGINT AS rows_with_true_match,
            SUM(has_predictions)::BIGINT AS rows_with_predictions,
            {precision_select_sql},
            {recall_select_sql},
            ROUND({reciprocal_rank_mean_sql(rank_sql="true_rank")}, 6)
                AS mean_reciprocal_rank,
            ROUND(
                {average_rank_within_k_sql(rank_sql="true_rank", k=max_cutoff)},
                6
            ) AS average_true_rank
        FROM true_ranks
        """
    )


def _build_top_k_compared_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    labelled_relation: duckdb.DuckDBPyRelation,
    predictions_relation: duckdb.DuckDBPyRelation,
    scenarios: list[tuple[int, float, str]],
    precision_at_cutoffs: list[int],
) -> duckdb.DuckDBPyRelation:
    scenario_relations = [
        _build_top_k_scenario_metrics_relation(
            con,
            labelled_relation=labelled_relation,
            predictions_relation=predictions_relation,
            threshold_match_weight=threshold_value,
            scenario_order=scenario_order,
            scenario_label=scenario_label,
            precision_at_cutoffs=precision_at_cutoffs,
        )
        for scenario_order, threshold_value, scenario_label in scenarios
    ]

    baseline_label = scenarios[0][2]
    baseline_literal = sql_literal(baseline_label)
    scenario_union_sql = "\nUNION ALL\n".join(
        f"SELECT * FROM ({rel.sql_query().strip()}) AS scenario_metrics"
        for rel in scenario_relations
    )

    precision_compared_select_sql = ",\n".join(
        f"            c.precision_at_{cutoff}" for cutoff in precision_at_cutoffs
    )
    recall_compared_select_sql = ",\n".join(
        f"            c.recall_at_{cutoff}" for cutoff in precision_at_cutoffs
    )
    precision_delta_select_sql = ",\n".join(
        (
            f"            c.precision_at_{cutoff} - "
            f"b.baseline_precision_at_{cutoff} AS precision_at_{cutoff}_delta"
        )
        for cutoff in precision_at_cutoffs
    )
    recall_delta_select_sql = ",\n".join(
        (
            f"            c.recall_at_{cutoff} - "
            f"b.baseline_recall_at_{cutoff} AS recall_at_{cutoff}_delta"
        )
        for cutoff in precision_at_cutoffs
    )
    baseline_precision_select_sql = ",\n".join(
        f"                precision_at_{cutoff} AS baseline_precision_at_{cutoff}"
        for cutoff in precision_at_cutoffs
    )
    baseline_recall_select_sql = ",\n".join(
        f"                recall_at_{cutoff} AS baseline_recall_at_{cutoff}"
        for cutoff in precision_at_cutoffs
    )

    return con.sql(
        f"""
        WITH compared AS (
            {scenario_union_sql}
        ),
        baseline_row AS (
            SELECT
                {baseline_precision_select_sql},
                {baseline_recall_select_sql},
                rows_with_predictions AS baseline_rows_with_predictions,
                mean_reciprocal_rank AS baseline_mrr,
                average_true_rank AS baseline_average_true_rank
            FROM compared
            WHERE scenario = '{baseline_literal}'
        )
        SELECT
            c.scenario_order,
            c.scenario,
            c.threshold_match_weight,
            c.rows_with_true_match,
            c.rows_with_predictions,
            {precision_compared_select_sql},
            {recall_compared_select_sql},
            c.mean_reciprocal_rank,
            c.average_true_rank,
            c.rows_with_predictions - b.baseline_rows_with_predictions
                AS rows_with_predictions_delta,
            {precision_delta_select_sql},
            {recall_delta_select_sql},
            c.mean_reciprocal_rank - b.baseline_mrr AS mrr_delta,
            c.average_true_rank - b.baseline_average_true_rank AS average_true_rank_delta,
            CASE WHEN c.scenario = '{baseline_literal}' THEN TRUE ELSE FALSE END
                AS is_baseline
        FROM compared AS c
        LEFT JOIN baseline_row AS b
            ON TRUE
        ORDER BY c.scenario_order, c.threshold_match_weight
        """
    )


def _signed_fixed_decimal_sql(delta_sql: str, *, decimals: int) -> str:
    format_spec = f"%.{decimals}f"
    positive_sql = f"concat('+', printf('{format_spec}', ({delta_sql})))"
    negative_sql = f"printf('{format_spec}', ({delta_sql}))"
    zero_sql = f"printf('{format_spec}', 0.0)"
    return (
        "CASE "
        f"WHEN ({delta_sql}) > 0 THEN {positive_sql} "
        f"WHEN ({delta_sql}) < 0 THEN {negative_sql} "
        f"ELSE {zero_sql} END"
    )


def _k_metric_compact_sql(*, value_expr_by_k: list[tuple[int, str]]) -> str:
    parts = [
        f"concat('@{k} ', CAST(ROUND(({value_expr}) * 100.0, 2) AS VARCHAR), '%')"
        for k, value_expr in value_expr_by_k
    ]
    return f"concat_ws(' | ', {', '.join(parts)})"


def _k_delta_compact_display_sql(
    *,
    is_baseline_sql: str,
    baseline_expr_by_k: list[tuple[int, str]],
    delta_expr_by_k: list[tuple[int, str]],
) -> str:
    baseline_pairs = [
        f"concat('@{k} ', CAST(ROUND(({value_expr}) * 100.0, 2) AS VARCHAR), '%')"
        for k, value_expr in baseline_expr_by_k
    ]
    delta_pairs = [
        (
            f"concat('@{k} ', "
            + signed_pp_delta_sql(
                delta_sql=delta_expr,
                decimals=2,
                zero_text="0.00 pp",
            )
            + ")"
        )
        for k, delta_expr in delta_expr_by_k
    ]
    return baseline_or_delta_sql(
        is_baseline_sql=is_baseline_sql,
        baseline_value_sql=f"concat_ws(' | ', {', '.join(baseline_pairs)})",
        delta_value_sql=f"concat_ws(' | ', {', '.join(delta_pairs)})",
    )


@dataclass(frozen=True)
class _TopKSqlFragments:
    join_sql: str
    headline_columns_sql: str
    delta_numeric_columns_sql: str


def _scenario_display_sql() -> str:
    return (
        "CASE WHEN c.is_baseline THEN concat(c.scenario, ' (baseline)') "
        "ELSE c.scenario END"
    )


def _build_human_headline_top_k_columns_sql(
    precision_at_cutoffs: list[int] | None,
) -> str:
    if precision_at_cutoffs is None:
        return ""

    return (
        ",\n"
        "            tk.rows_with_true_match AS rows_with_true_match,\n"
        "            tk.rows_with_predictions AS rows_with_predictions,\n"
        "            "
        + _k_metric_compact_sql(
            value_expr_by_k=[
                (cutoff, f"tk.precision_at_{cutoff}") for cutoff in precision_at_cutoffs
            ]
        )
        + " AS precision_at_k,\n"
        "            "
        + _k_metric_compact_sql(
            value_expr_by_k=[
                (cutoff, f"tk.recall_at_{cutoff}") for cutoff in precision_at_cutoffs
            ]
        )
        + " AS recall_at_k,\n"
        "            ROUND(tk.mean_reciprocal_rank, 6) AS mean_reciprocal_rank,\n"
        "            ROUND(tk.average_true_rank, 6) AS average_true_rank"
    )


def _build_headline_table(
    con: duckdb.DuckDBPyConnection,
    *,
    base_compared: duckdb.DuckDBPyRelation,
    top_k_sql_fragments: _TopKSqlFragments,
    precision_at_cutoffs: list[int] | None,
    human_readable: bool,
) -> duckdb.DuckDBPyRelation:
    scenario_sql = _scenario_display_sql()
    if human_readable:
        headline_columns_sql = (
            "concat(\n"
            "                    'matched ', format('{:,}', c.rows_matched_in_stage),\n"
            "                    ' | correct ', format('{:,}', c.correct_matches),\n"
            "                    ' | wrong ', format('{:,}', c.wrong_matches)\n"
            "                ) AS match_outcome,\n"
            "                ROUND(c.precision, 6) AS precision,\n"
            "                ROUND(c.recall, 6) AS recall,\n"
            "                ROUND(c.f1, 6) AS f1"
            + _build_human_headline_top_k_columns_sql(precision_at_cutoffs)
        )
        order_by_sql = "c.scenario_order"
    else:
        headline_columns_sql = (
            "ROUND(c.threshold_match_weight, 2) AS threshold,\n"
            "                c.rows_matched_in_stage AS matched_rows,\n"
            "                "
            + percent_string_sql(
                value_sql=ratio_sql(
                    "c.rows_matched_in_stage",
                    "c.rows_entering_splink",
                    when_zero_sql="NULL::DOUBLE",
                ),
                decimals=1,
            )
            + " AS match_rate,\n"
            "                c.correct_matches AS correct_matches,\n"
            "                c.wrong_matches AS mismatched_matches,\n"
            "                ROUND(c.precision, 6) AS precision,\n"
            "                ROUND(c.recall, 6) AS recall,\n"
            "                ROUND(c.f1, 6) AS f1"
            + top_k_sql_fragments.headline_columns_sql
        )
        order_by_sql = "c.scenario_order, threshold"

    return con.sql(
        f"""
        WITH compared AS (
            SELECT *
            FROM ({base_compared.sql_query()}) AS c
        )
        SELECT
            {scenario_sql} AS scenario,
            {headline_columns_sql}
        FROM compared AS c
        {top_k_sql_fragments.join_sql}
        ORDER BY {order_by_sql}
        """
    )


def _build_human_top_1_delta_columns_sql() -> str:
    metric_columns: list[str] = []
    for metric in ["precision", "recall", "f1"]:
        metric_columns.append(
            "            "
            + baseline_or_delta_sql(
                is_baseline_sql="c.is_baseline",
                baseline_value_sql=percent_string_sql(
                    value_sql=f"c.{metric}",
                    decimals=2,
                ),
                delta_value_sql=signed_pp_delta_sql(
                    delta_sql=f"c.delta_{metric}",
                    decimals=2,
                    zero_text="0.00 pp",
                ),
            )
            + f" AS {metric}_delta"
        )
    return ",\n".join(metric_columns)


def _build_base_compared_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    relation: duckdb.DuckDBPyRelation,
    scenarios: list[tuple[int, float, str]],
    total_input_rows: int,
    rows_entering_splink: int,
) -> duckdb.DuckDBPyRelation:
    scenario_queries: list[str] = []
    baseline_label = scenarios[0][2]
    baseline_literal = sql_literal(baseline_label)

    for scenario_order, threshold_value, scenario_label in scenarios:
        rel = build_accuracy_table(
            con,
            relation,
            splink_match_weight_threshold=threshold_value,
        )
        rel_sql = rel.sql_query()
        scenario_queries.append(
            f"""
            SELECT
                {scenario_order} AS scenario_order,
                '{sql_literal(scenario_label)}' AS scenario,
                {threshold_value}::DOUBLE AS threshold_match_weight,
                {total_input_rows}::BIGINT AS total_input_rows,
                {rows_entering_splink}::BIGINT AS rows_entering_splink,
                *
            FROM ({rel_sql}) AS accuracy
            WHERE stage = 'splink'
            UNION ALL
            SELECT
                {scenario_order} AS scenario_order,
                '{sql_literal(scenario_label)}' AS scenario,
                {threshold_value}::DOUBLE AS threshold_match_weight,
                {total_input_rows}::BIGINT AS total_input_rows,
                {rows_entering_splink}::BIGINT AS rows_entering_splink,
                'splink' AS stage,
                0::BIGINT AS rows_matched_in_stage,
                0::BIGINT AS correct_matches,
                0::BIGINT AS wrong_matches,
                NULL::DOUBLE AS precision,
                NULL::DOUBLE AS wrong_match_rate,
                0.0::DOUBLE AS correct_share_of_total,
                0.0::DOUBLE AS recall,
                0.0::DOUBLE AS f1
            WHERE NOT EXISTS (
                SELECT 1
                FROM ({rel_sql}) AS accuracy
                WHERE stage = 'splink'
            )
            """
        )

    union_sql = "\nUNION ALL\n".join(q.strip() for q in scenario_queries)
    return con.sql(
        f"""
        WITH compared AS (
            {union_sql}
        ),
        baseline_rows AS (
            SELECT
                rows_matched_in_stage AS baseline_matched_rows,
                correct_matches AS baseline_correct_matches,
                wrong_matches AS baseline_wrong_matches,
                precision AS baseline_precision,
                recall AS baseline_recall,
                f1 AS baseline_f1
            FROM compared
            WHERE scenario = '{baseline_literal}'
        )
        SELECT
            c.scenario_order,
            c.scenario,
            c.threshold_match_weight,
            c.total_input_rows,
            c.rows_entering_splink,
            c.rows_matched_in_stage,
            c.correct_matches,
            c.wrong_matches,
            c.precision,
            c.recall,
            c.f1,
            c.rows_matched_in_stage - b.baseline_matched_rows AS delta_matched_rows,
            c.correct_matches - b.baseline_correct_matches AS delta_correct_matches,
            c.wrong_matches - b.baseline_wrong_matches AS delta_wrong_matches,
            c.precision - b.baseline_precision AS delta_precision,
            c.recall - b.baseline_recall AS delta_recall,
            c.f1 - b.baseline_f1 AS delta_f1,
            CASE WHEN c.scenario = '{baseline_literal}' THEN TRUE ELSE FALSE END
                AS is_baseline
        FROM compared AS c
        LEFT JOIN baseline_rows AS b
            ON TRUE
        ORDER BY c.scenario_order, c.threshold_match_weight
        """
    )


def _build_top_k_sql_fragments(
    *,
    top_k_compared: duckdb.DuckDBPyRelation | None,
    precision_at_cutoffs: list[int] | None,
) -> _TopKSqlFragments:
    if top_k_compared is None or precision_at_cutoffs is None:
        return _TopKSqlFragments(
            join_sql="",
            headline_columns_sql="",
            delta_numeric_columns_sql="",
        )

    top_k_join_sql = (
        "LEFT JOIN ("
        f"{top_k_compared.sql_query()}"
        ") AS tk "
        "ON c.scenario_order = tk.scenario_order "
        "AND c.scenario = tk.scenario "
        "AND c.threshold_match_weight = tk.threshold_match_weight"
    )

    precision_headline_columns_sql = ",\n".join(
        f"            ROUND(tk.precision_at_{cutoff}, 6) AS precision_at_{cutoff}"
        for cutoff in precision_at_cutoffs
    )
    recall_headline_columns_sql = ",\n".join(
        f"            ROUND(tk.recall_at_{cutoff}, 6) AS recall_at_{cutoff}"
        for cutoff in precision_at_cutoffs
    )
    headline_top_k_columns_sql = (
        ",\n"
        "            tk.rows_with_true_match AS rows_with_true_match,\n"
        "            tk.rows_with_predictions AS rows_with_predictions,\n"
        + precision_headline_columns_sql
        + ",\n"
        + recall_headline_columns_sql
        + ",\n"
        "            ROUND(tk.mean_reciprocal_rank, 6) AS mean_reciprocal_rank,\n"
        "            ROUND(tk.average_true_rank, 6) AS average_true_rank"
    )

    precision_delta_columns_sql = ",\n".join(
        (
            "            CASE "
            "WHEN c.is_baseline THEN NULL::DOUBLE "
            f"ELSE ROUND(tk.precision_at_{cutoff}_delta, 6) END "
            f"AS precision_at_{cutoff}_delta"
        )
        for cutoff in precision_at_cutoffs
    )
    recall_delta_columns_sql = ",\n".join(
        (
            "            CASE "
            "WHEN c.is_baseline THEN NULL::DOUBLE "
            f"ELSE ROUND(tk.recall_at_{cutoff}_delta, 6) END "
            f"AS recall_at_{cutoff}_delta"
        )
        for cutoff in precision_at_cutoffs
    )

    delta_numeric_columns_sql = (
        ",\n"
        "            CASE "
        "WHEN c.is_baseline THEN NULL::BIGINT "
        "ELSE CAST(tk.rows_with_predictions_delta AS BIGINT) END "
        + " AS rows_with_predictions_delta,\n"
        + precision_delta_columns_sql
        + ",\n"
        + recall_delta_columns_sql
        + ",\n"
        "            CASE "
        "WHEN c.is_baseline THEN NULL::DOUBLE "
        "ELSE ROUND(tk.mrr_delta, 6) END AS mrr_delta,\n"
        "            CASE "
        "WHEN c.is_baseline THEN NULL::DOUBLE "
        "ELSE ROUND(tk.average_true_rank_delta, 6) END AS average_true_rank_delta"
    )

    return _TopKSqlFragments(
        join_sql=top_k_join_sql,
        headline_columns_sql=headline_top_k_columns_sql,
        delta_numeric_columns_sql=delta_numeric_columns_sql,
    )


@requires_ukam_label("relation", function_name="_compare_splink_model_results")
def build_splink_model_comparison(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    *,
    baseline_match_weight: float,
    splink_comparison_weights: list[float] | None = None,
    predictions_relation: duckdb.DuckDBPyRelation | None = None,
    precision_at_metrics: list[int] | None = None,
    human_readable: bool = True,
) -> SplinkModelComparisonOutput:
    """Build unified Splink comparison tables for top-1 and optional top-k metrics.

    The output always includes top-1 emitted-decision metrics for the baseline
    threshold and any additional comparison thresholds.

    If ``precision_at_metrics`` is provided, top-k retrieval/ranking metrics are
    included in the same headline and delta tables. Top-k cutoffs are validated
    to be integers in the inclusive range [1, 10]. The ``average_true_rank``
    metric is computed using the largest requested cutoff.

    ``human_readable`` controls presentation formatting:
    - ``True``: compact headline and delta display strings for reporting.
    - ``False``: machine-friendly headline columns and atomic numeric deltas.

    Baseline/comparison rules:
    - Baseline is always reported.
    - Comparison thresholds are optional.
    - If the baseline threshold appears in comparison weights, it is deduplicated.

    Returns:
        SplinkModelComparisonOutput with ``headline_table``, ``delta_table``, and
        ``total_input_rows``.
    """
    scenarios = _build_scenario_definitions(
        baseline_match_weight=baseline_match_weight,
        splink_comparison_weights=splink_comparison_weights,
    )

    total_input_rows = int(
        con.sql(f"SELECT COUNT(*) FROM ({relation.sql_query()}) AS m").fetchone()[0]
    )
    rows_entering_splink = int(
        con.sql(
            f"""
            SELECT COUNT(*)
            FROM ({relation.sql_query()}) AS m
            WHERE m.match_reason IS NULL
                OR split_part(m.match_reason::VARCHAR, ':', 1) = 'splink'
            """
        ).fetchone()[0]
    )

    base_compared = _build_base_compared_relation(
        con,
        relation=relation,
        scenarios=scenarios,
        total_input_rows=total_input_rows,
        rows_entering_splink=rows_entering_splink,
    )

    precision_at_cutoffs = _resolve_precision_at_metrics(precision_at_metrics)
    top_k_compared: duckdb.DuckDBPyRelation | None = None
    if precision_at_cutoffs is not None:
        if predictions_relation is None:
            raise ValueError(
                "precision_at_metrics was provided but Splink predictions are "
                "unavailable. "
                "Ensure Splink ran and predictions can be retrieved."
            )
        top_k_compared = _build_top_k_compared_relation(
            con,
            labelled_relation=relation,
            predictions_relation=predictions_relation,
            scenarios=scenarios,
            precision_at_cutoffs=precision_at_cutoffs,
        )

    top_k_sql_fragments = _build_top_k_sql_fragments(
        top_k_compared=top_k_compared,
        precision_at_cutoffs=precision_at_cutoffs,
    )

    headline_table = _build_headline_table(
        con,
        base_compared=base_compared,
        top_k_sql_fragments=top_k_sql_fragments,
        precision_at_cutoffs=precision_at_cutoffs,
        human_readable=human_readable,
    )

    delta_numeric_table = con.sql(
        f"""
        WITH compared AS (
            SELECT *
            FROM ({base_compared.sql_query()}) AS c
        )
        SELECT
            {_scenario_display_sql()} AS scenario,
            c.is_baseline,
            CASE
                WHEN c.is_baseline THEN NULL::BIGINT
                ELSE CAST(c.delta_matched_rows AS BIGINT)
            END AS matched_rows_delta,
            CASE
                WHEN c.is_baseline THEN NULL::BIGINT
                ELSE CAST(c.delta_correct_matches AS BIGINT)
            END AS correct_matches_delta,
            CASE
                WHEN c.is_baseline THEN NULL::BIGINT
                ELSE CAST(c.delta_wrong_matches AS BIGINT)
            END AS mismatched_matches_delta,
            CASE
                WHEN c.is_baseline THEN NULL::DOUBLE
                ELSE ROUND(c.delta_precision, 6)
            END AS precision_delta,
            CASE
                WHEN c.is_baseline THEN NULL::DOUBLE
                ELSE ROUND(c.delta_recall, 6)
            END AS recall_delta,
            CASE
                WHEN c.is_baseline THEN NULL::DOUBLE
                ELSE ROUND(c.delta_f1, 6)
            END AS f1_delta
            {top_k_sql_fragments.delta_numeric_columns_sql}
        FROM compared AS c
        {top_k_sql_fragments.join_sql}
        ORDER BY c.scenario_order, c.threshold_match_weight
        """
    )

    if human_readable:
        top_k_human_columns_sql = ""
        if precision_at_cutoffs is not None:
            precision_delta_compact_sql = _k_delta_compact_display_sql(
                is_baseline_sql="c.is_baseline",
                baseline_expr_by_k=[
                    (cutoff, f"tk.precision_at_{cutoff}")
                    for cutoff in precision_at_cutoffs
                ],
                delta_expr_by_k=[
                    (cutoff, f"tk.precision_at_{cutoff}_delta")
                    for cutoff in precision_at_cutoffs
                ],
            )
            recall_delta_compact_sql = _k_delta_compact_display_sql(
                is_baseline_sql="c.is_baseline",
                baseline_expr_by_k=[
                    (cutoff, f"tk.recall_at_{cutoff}") for cutoff in precision_at_cutoffs
                ],
                delta_expr_by_k=[
                    (cutoff, f"tk.recall_at_{cutoff}_delta")
                    for cutoff in precision_at_cutoffs
                ],
            )
            top_k_human_columns_sql = (
                ",\n"
                "            "
                + baseline_or_delta_sql(
                    is_baseline_sql="c.is_baseline",
                    baseline_value_sql="format('{:,}', tk.rows_with_predictions)",
                    delta_value_sql=signed_count_delta_sql(
                        delta_sql="tk.rows_with_predictions_delta"
                    ),
                )
                + " AS rows_with_predictions_delta,\n"
                "            "
                + precision_delta_compact_sql
                + " AS precision_at_k_delta,\n"
                "            " + recall_delta_compact_sql + " AS recall_at_k_delta,\n"
                "            "
                + baseline_or_delta_sql(
                    is_baseline_sql="c.is_baseline",
                    baseline_value_sql="printf('%.3f', tk.mean_reciprocal_rank)",
                    delta_value_sql=_signed_fixed_decimal_sql(
                        "tk.mrr_delta",
                        decimals=3,
                    ),
                )
                + " AS mrr_delta,\n"
                "            "
                + baseline_or_delta_sql(
                    is_baseline_sql="c.is_baseline",
                    baseline_value_sql="printf('%.3f', tk.average_true_rank)",
                    delta_value_sql=_signed_fixed_decimal_sql(
                        "tk.average_true_rank_delta",
                        decimals=3,
                    ),
                )
                + " AS average_true_rank_delta"
            )

        delta_table = con.sql(
            f"""
            WITH compared AS (
                SELECT *
                FROM ({base_compared.sql_query()}) AS c
            )
            SELECT
                {_scenario_display_sql()} AS scenario,
                CASE
                    WHEN c.is_baseline THEN concat(
                        'matched ', format('{{:,}}', c.rows_matched_in_stage),
                        ' | correct ', format('{{:,}}', c.correct_matches),
                        ' | wrong ', format('{{:,}}', c.wrong_matches)
                    )
                    ELSE concat(
                        'matched ', {
                signed_count_delta_sql(delta_sql="c.delta_matched_rows")
            },
                        ' | correct ', {
                signed_count_delta_sql(delta_sql="c.delta_correct_matches")
            },
                        ' | wrong ', {
                signed_count_delta_sql(delta_sql="c.delta_wrong_matches")
            }
                    )
                END AS delta_match_outcome,
                {_build_human_top_1_delta_columns_sql()}
                {top_k_human_columns_sql}
            FROM compared AS c
            {top_k_sql_fragments.join_sql}
            ORDER BY c.scenario_order
            """
        )
    else:
        delta_table = con.sql(
            f"""
            SELECT
                * EXCLUDE(is_baseline)
            FROM ({delta_numeric_table.sql_query()}) AS d
            ORDER BY d.scenario
            """
        )

    return SplinkModelComparisonOutput(
        headline_table=headline_table,
        delta_table=delta_table,
        total_input_rows=total_input_rows,
    )
