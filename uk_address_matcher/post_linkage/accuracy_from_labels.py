import logging

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from IPython.display import display
from splink import Linker

logger = logging.getLogger("uk_address_matcher")


CLEANED_COLS_TO_SELECT = """
    original_address_concat,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    unusual_tokens_arr,
    very_unusual_tokens_arr,
    extremely_unusual_tokens_arr,
    * EXCLUDE (
        original_address_concat,
        flat_positional,
        flat_letter,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        unusual_tokens_arr,
        very_unusual_tokens_arr,
        extremely_unusual_tokens_arr
    )
"""


def inspect_match_results_vs_labels(
    *,
    labels: DuckDBPyRelation,
    df_predict_improved: DuckDBPyRelation,
    df_predict_with_distinguishability: DuckDBPyRelation,
    df_os_addresses: DuckDBPyRelation,
    df_messy_data_clean: DuckDBPyRelation,
    df_os_addresses_clean: DuckDBPyRelation,
    df_predict_original: DuckDBPyRelation,
    linker: Linker,
    con: DuckDBPyConnection,
    unique_id_r: str | None = None,
    example_number: int = 1,
):
    temp_tables = {
        "labels_in": labels,
        "df_predict_improved_in": df_predict_improved,
        "df_predict_with_distinguishability_in": df_predict_with_distinguishability,
        "df_os_addresses_in": df_os_addresses,
        "df_messy_data_clean_in": df_messy_data_clean,
        "df_os_addresses_clean_in": df_os_addresses_clean,
        "df_predict_original_in": df_predict_original,
    }
    for name, rel in temp_tables.items():
        con.register(name, rel)

    target_unique_id_r = unique_id_r

    if target_unique_id_r is None:
        sql_find_nth_fp = f"""
        WITH labeled_dist AS (
            SELECT
                d.unique_id_r,
                d.unique_id_l as predicted_unique_id,
                l.correct_unique_id::VARCHAR as correct_unique_id
            FROM df_predict_with_distinguishability_in as d
            INNER JOIN labels_in as l ON d.unique_id_r = l.unique_id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY d.unique_id_r ORDER BY d.match_weight DESC) = 1
        ), false_positives AS (
            SELECT unique_id_r
            FROM labeled_dist
            WHERE predicted_unique_id != correct_unique_id
            ORDER BY unique_id_r -- Order for consistent example selection
        )
        SELECT unique_id_r
        FROM false_positives
        LIMIT 1 OFFSET {example_number - 1}
        """
        result = con.sql(sql_find_nth_fp).fetchone()
        if result:
            target_unique_id_r = result[0]
        else:
            logger.error(
                f"Error: Could not find false positive example number {example_number}."
            )
            for name in temp_tables:
                con.unregister(name)
            return

    sql = f"""
    SELECT d.*, l.correct_unique_id::VARCHAR as correct_unique_id
    FROM df_predict_improved_in as d
    LEFT JOIN labels_in as l ON d.unique_id_r = l.unique_id
    WHERE d.unique_id_r = '{target_unique_id_r}'
    """
    df_improved_with_label = con.sql(sql)
    con.register("df_improved_with_label", df_improved_with_label)

    sql = f"""
    SELECT d.*, l.correct_unique_id::VARCHAR as correct_unique_id
    FROM df_predict_with_distinguishability_in as d
    LEFT JOIN labels_in as l ON d.unique_id_r = l.unique_id
    WHERE d.unique_id_r = '{target_unique_id_r}'
    """
    df_with_dist_and_label = con.sql(sql)
    con.register("df_with_dist_and_label", df_with_dist_and_label)

    sql = f"""
    SELECT
        d.*,
        o.address_concat as label_address_concat,
        o.postcode as label_postcode
    FROM df_with_dist_and_label as d
    LEFT JOIN df_os_addresses_in as o ON d.correct_unique_id = o.unique_id
    WHERE d.unique_id_r = '{target_unique_id_r}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.unique_id_r ORDER BY d.match_weight DESC) = 1
    """
    this_match_rel = con.sql(sql)
    row_dict_best_match = dict(zip(this_match_rel.columns, this_match_rel.fetchone()))

    sql = f"""
    SELECT *
    FROM df_improved_with_label
    WHERE unique_id_l = correct_unique_id
    AND unique_id_r = '{target_unique_id_r}'
    LIMIT 1
    """
    correct_match_rel = con.sql(sql)
    # Handle case where correct match might not be in the improved predictions
    correct_match_row = correct_match_rel.fetchone()
    if correct_match_row:
        row_dict_correct_match = dict(zip(correct_match_rel.columns, correct_match_row))
    else:
        # Provide dummy values if the true match wasn't scored (e.g., below threshold)
        row_dict_correct_match = {
            "match_weight": float("-inf"),
            "unique_id_l": row_dict_best_match.get("correct_unique_id", "UNKNOWN"),
            "original_address_concat_l": "NOT SCORED",
            "postcode_l": "",
        }

    report_template = """
===========================================================================
unique_id_r:                  {this_id}
{messy_label:<30}{messy_address} {messy_postcode}

{best_match_label:<30}{best_match_address} {best_match_postcode} (UPRN: {best_match_uprn})
{true_match_label:<30}{true_match_address} {true_match_postcode} (UPRN: {true_match_uprn})
Distinguishability:           {distinguishability_value}
===========================================================================
"""

    report = report_template.format(
        this_id=target_unique_id_r,
        messy_label="Messy address:",
        best_match_label=f"Best match (score: {row_dict_best_match.get('match_weight', 'N/A'):,.2f}):",
        true_match_label=f"True match (score: {row_dict_correct_match.get('match_weight', 'N/A'):,.2f}):",
        messy_address=row_dict_best_match.get("address_concat_r", "N/A"),
        best_match_address=row_dict_best_match.get("original_address_concat_l", "N/A"),
        true_match_address=row_dict_best_match.get("label_address_concat", "N/A"),
        distinguishability_value=(
            f"{row_dict_best_match.get('distinguishability', 'N/A'):,.2f}"
            if row_dict_best_match.get("distinguishability") is not None
            else "N/A"
        ),
        messy_postcode=row_dict_best_match.get("postcode_r", ""),
        best_match_postcode=row_dict_best_match.get("postcode_l", ""),
        true_match_postcode=row_dict_best_match.get("label_postcode", ""),
        best_match_uprn=row_dict_best_match.get("unique_id_l", "N/A"),
        true_match_uprn=row_dict_best_match.get("correct_unique_id", "N/A"),
    )
    logger.info(report)

    sql = f"""
    SELECT
        original_address_concat_r,
        CASE
            WHEN unique_id_l = correct_unique_id THEN concat('✅ ', original_address_concat_l)
            ELSE original_address_concat_l
        END as address_concat_l,
        printf('%.2f', match_weight) as final_score,
        printf('%.2f', match_weight_original) as splink_score,
        printf('%.2f', mw_adjustment) as adjustment_score,
        overlapping_tokens_this_l_and_r AS matching_tokens,
        tokens_elsewhere_in_block_but_not_this AS penalty_tokens,
        missing_tokens,
        overlapping_bigrams_this_l_and_r_filtered AS matching_bigrams,
        bigrams_elsewhere_in_block_but_not_this_filtered AS penalty_bigrams,
        unique_id_l as canonical_uprn
    FROM df_improved_with_label
    WHERE unique_id_r = '{target_unique_id_r}'
    ORDER BY match_weight DESC
    LIMIT 10
    """
    con.sql(sql).show(max_width=1000, max_col_width=60)

    best_match_uprn = row_dict_best_match.get("unique_id_l")
    correct_unique_id = row_dict_best_match.get("correct_unique_id")

    cleaned_cols = con.sql(
        f"SELECT {CLEANED_COLS_TO_SELECT} FROM df_messy_data_clean_in LIMIT 0"
    ).columns

    def _build_projection(table_name: str, columns: list[str]) -> str:
        source_cols = set(con.table(table_name).columns)
        return ",\n        ".join(
            col if col in source_cols else f"NULL AS {col}" for col in columns
        )

    messy_projection = _build_projection("df_messy_data_clean_in", cleaned_cols)
    canonical_projection = _build_projection("df_os_addresses_clean_in", cleaned_cols)

    unions = [
        f"""SELECT 'Messy' AS record_type, {messy_projection}
            FROM df_messy_data_clean_in
            WHERE unique_id = '{target_unique_id_r}'"""
    ]
    if best_match_uprn:
        unions.append(
            f"""SELECT 'Best Match' AS record_type, {canonical_projection}
               FROM df_os_addresses_clean_in
               WHERE unique_id = '{best_match_uprn}'"""
        )
    if correct_unique_id:  # Always include true match for false positive inspection
        unions.append(
            f"""SELECT 'True Match' AS record_type, {canonical_projection}
               FROM df_os_addresses_clean_in
               WHERE unique_id = '{correct_unique_id}'"""
        )

    sql = "\n\nUNION ALL\n\n".join(unions)
    con.sql(sql).show(max_width=1000, max_col_width=40)

    best_uprn = row_dict_best_match.get("unique_id_l")
    correct_unique_id = row_dict_best_match.get("correct_unique_id")

    if best_uprn:
        waterfall_header = """
Waterfall chart for messy address vs best match:
{messy_address} {messy_postcode}
{best_match_address} {best_match_postcode}
"""
        logger.info(
            waterfall_header.format(
                messy_address=row_dict_best_match.get("address_concat_r", "N/A"),
                messy_postcode=row_dict_best_match.get("postcode_r", ""),
                best_match_address=row_dict_best_match.get(
                    "original_address_concat_l", "N/A"
                ),
                best_match_postcode=row_dict_best_match.get("postcode_l", ""),
            )
        )

        sql = f"""
        SELECT *
        FROM df_predict_original_in
        WHERE unique_id_r = '{target_unique_id_r}' AND unique_id_l = '{best_uprn}'
        ORDER BY match_weight DESC
        LIMIT 1
        """
        res = con.sql(sql)
        if res.shape[0] > 0:
            display(
                linker.visualisations.waterfall_chart(
                    res.df().to_dict(orient="records"), filter_nulls=False
                )
            )

    if correct_unique_id:
        waterfall_header = """
Waterfall chart for messy address vs true match:
{messy_address} {messy_postcode}
{true_match_address} {true_match_postcode}
"""
        logger.info(
            waterfall_header.format(
                messy_address=row_dict_best_match.get("address_concat_r", "N/A"),
                messy_postcode=row_dict_best_match.get("postcode_r", ""),
                true_match_address=row_dict_best_match.get(
                    "label_address_concat", "N/A"
                ),  # Get address from joined label info
                true_match_postcode=row_dict_best_match.get("label_postcode", ""),
            )
        )

        sql = f"""
        SELECT *
        FROM df_predict_original_in
        WHERE unique_id_r = '{target_unique_id_r}' AND unique_id_l = '{correct_unique_id}'
        ORDER BY match_weight DESC
        LIMIT 1
        """
        res = con.sql(sql)
        if res.shape[0] > 0:
            display(
                linker.visualisations.waterfall_chart(
                    res.df().to_dict(orient="records"), filter_nulls=False
                )
            )

    con.unregister("df_improved_with_label")
    con.unregister("df_with_dist_and_label")
    for name in temp_tables:
        try:
            con.unregister(name)
        except Exception:
            pass


def evaluate_predictions_against_labels(
    *,
    match_candidates: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """
    Calculates the accuracy of match candidates against provided labels.

    Accuracy is defined as the percentage of labelled records where the resolved
    canonical identifier matches the provided ukam_label.

    Args:
        match_candidates: DuckDB relation containing the resolved match for each
            messy record. Expected columns: `ukam_label`, `resolved_canonical_id`.
        con: Active DuckDB connection.

    Returns:
        DuckDBPyRelation: A DataFrame with columns (status, count, percentage, percentage_fmt)
                          and rows ('Correctly Predicted', 'Incorrectly Predicted', 'Total').
                          Returns an empty relation if evaluation cannot be performed.
    """
    candidates_reg_name = "__match_candidates_eval"
    con.register(candidates_reg_name, match_candidates)

    sql = f"""
    WITH labeled_candidates AS (
        SELECT
            ukam_label::VARCHAR AS ukam_label,
            resolved_canonical_id::VARCHAR AS resolved_canonical_id
        FROM {candidates_reg_name}
        WHERE ukam_label IS NOT NULL
    ),
        comparison AS (
            SELECT
                CASE
                    WHEN resolved_canonical_id = ukam_label THEN 'Correctly Predicted'
                    WHEN resolved_canonical_id IS NULL THEN 'Incorrectly Predicted (No match found)'
                    ELSE 'Incorrectly Predicted (Wrong prediction)'
                END AS status
            FROM labeled_candidates
        ),
        status_counts AS (
            SELECT
                status,
                COUNT(*) AS count
            FROM comparison
            GROUP BY CUBE(status) -- Use CUBE to get individual statuses and the total (NULL)
        ),
        total_count_cte AS (
            SELECT count FROM status_counts WHERE status IS NULL
        )
        SELECT
            COALESCE(status, 'Total') AS status,
            count,
            100.0 * count / NULLIF((SELECT count FROM total_count_cte), 0) AS percentage,
            FORMAT(
                '{{:.2f}}%%',
                100.0 * count / NULLIF((SELECT count FROM total_count_cte), 0)
            ) AS percentage_fmt
        FROM status_counts
        ORDER BY status = 'Total', status; -- Sort Total last, then alphabetically
    """

    return con.sql(sql)
