import warnings
from typing import Literal

from duckdb import DuckDBPyConnection, DuckDBPyRelation


def _calculate_match_metrics(
    exact_match_results: DuckDBPyRelation,
    *,
    order: Literal["descending", "ascending"] = "descending",
) -> DuckDBPyRelation:
    """Summarise deterministic match counts grouped by ``match_reason``.

    Args:
        exact_match_results: Relation produced by the deterministic match pass
            containing a ``match_reason`` column.
        order: Sort direction for the returned ``match_count`` column. Defaults
            to "descending".

    Returns:
        DuckDBPyRelation with ``match_method``, ``match_count``, and
        ``match_percentage`` columns sorted per ``order``.
    """

    if order not in {"ascending", "descending"}:
        raise ValueError("order must be either 'ascending' or 'descending'.")

    if "match_reason" not in exact_match_results.columns:
        raise ValueError(
            "Expected column 'match_reason' to be present in relation; "
            f"available columns are {exact_match_results.columns}."
        )

    aggregation_query = """
        COALESCE(match_reason, 'unmatched') AS match_reason,
        COUNT(*) AS match_count,
        printf('%.2f%%', 100.0*COUNT(*)/SUM(COUNT(*)) OVER ()) as match_percentage
    """

    order_keyword = "DESC" if order == "descending" else "ASC"

    return exact_match_results.aggregate(
        aggregation_query,
        group_expr="COALESCE(match_reason, 'unmatched')",
    ).order(f"match_count {order_keyword}, match_reason")


def best_matches_with_distinguishability(
    df_predict: DuckDBPyRelation,
    df_addresses_to_match: DuckDBPyRelation,
    con: DuckDBPyConnection,
    distinguishability_thresholds=[1, 5, 10],
    best_match_only: bool = True,
    additional_columns_to_retain=None,
):
    """
    Finds the best match for each messy address and computes the
    distinguishability of the match, defined as the difference in match weight
    between the top and next best match

    Args:
        df_predict: table containing pairwise predictions from either
            `linker.inference.predict` or
            `improve_predictions_using_distinguishing_tokens`
        df_addresses_to_match: table containing addresses to be matched in
            cleaned form cols = (unique_id, ukam_address_id, address_concat, postcode)
        con: DuckDB connection for executing SQL queries
        distinguishability_thresholds: List of thresholds for categorizing match
            distinguishability. Default is [1, 5, 10].
        best_match_only: If True, only return the best match for each address.
            If False, return all matches. Default is True.

    Returns:
        DuckDBPyRelation: A table containing matched addresses with
        distinguishability metrics. Includes ``candidate_rank`` so callers can
        deterministically select the top candidate from the all-candidates
        output.
    """

    if "mw_adjustment" not in df_predict.columns:
        warnings.warn(
            "\nMost users will wish to pass the result of "
            "improve_predictions_using_distinguishing_tokens to this function.\n"
            "You appear to have passed the raw output of linker.inference.predict."
        )

    add_cols_select = ""
    if additional_columns_to_retain:
        for col in additional_columns_to_retain:
            add_cols_select += f"t.{col}_l, t.{col}_r, "

    if "ukam_label_r" in df_predict.columns:
        add_cols_select += "t.ukam_label_r, "

    phase1_score_select = (
        "t.phase1_score" if "phase1_score" in df_predict.columns else "NULL"
    )

    if 0 not in distinguishability_thresholds:
        distinguishability_thresholds.append(0)
    thres_sorted = sorted(distinguishability_thresholds, reverse=True)

    d_case_whens = "\n".join(
        [
            (
                f"WHEN distinguishability > {d} THEN '"
                f"{str(index).zfill(2)}: Distinguishability > {d}'"
            )
            for index, d in enumerate(thres_sorted, start=2)
        ]
    )
    next_label_index = len(thres_sorted) + 2
    next_label_value = f"{str(next_label_index).zfill(2)}."
    nan_label = f"{next_label_value}: NaN (last match in group)"
    zero_label = f"{next_label_value}: Distinguishability = 0"

    best_match_filter = "WHERE candidate_rank = 1" if best_match_only else ""

    if best_match_only:
        sort_str = "ORDER BY distinguishability_category ASC, match_weight DESC"
    else:
        sort_str = "ORDER BY unique_id_r,  match_weight DESC"

    sql = f"""
    WITH
        distinct_canonical_candidates AS (
            SELECT
                *
            FROM ({df_predict.sql_query()}) AS predict_for_distinguishability
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY unique_id_r, unique_id_l
                ORDER BY match_weight DESC, ukam_address_id_l
            ) = 1
        ),
        distinguishability_calc AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY unique_id_r
                    ORDER BY match_weight DESC, unique_id_l, ukam_address_id_l
                ) AS candidate_rank,
                match_weight - LEAD(match_weight) OVER (
                    PARTITION BY unique_id_r
                    ORDER BY match_weight DESC, unique_id_l, ukam_address_id_l
                ) AS distinguishability,
                COUNT(*) OVER (PARTITION BY unique_id_r) AS match_count
            FROM distinct_canonical_candidates
        ),
        categorized_matches AS (
            SELECT
                *,
                CASE
                    WHEN match_count = 1 THEN '01: One match only'
                    WHEN distinguishability IS NULL THEN '{nan_label}'
                    {d_case_whens}
                    WHEN distinguishability = 0 THEN '{zero_label}'
                    ELSE '99: error, uncategorized'
                END AS distinguishability_category
            FROM distinguishability_calc
            {best_match_filter}
        )
    SELECT
        a.unique_id AS unique_id_r,
        t.unique_id_l,
        a.ukam_address_id AS ukam_address_id_r,
        t.ukam_address_id_l,
        a.original_address_concat AS address_concat_r,
        a.postcode AS postcode_r,
        t.original_address_concat_l,
        t.postcode_l,
        t.match_weight,
        {phase1_score_select} AS phase1_score,
        t.distinguishability,
        t.candidate_rank,
        COALESCE(
            t.distinguishability_category, '99: No match'
        ) AS distinguishability_category,
        {add_cols_select}
    FROM ({df_addresses_to_match.sql_query()}) AS a
    LEFT JOIN categorized_matches AS t
    ON a.ukam_address_id = t.ukam_address_id_r
    {sort_str}
    """

    return con.sql(sql)
