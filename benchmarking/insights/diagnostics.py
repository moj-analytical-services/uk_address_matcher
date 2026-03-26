from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarking.insights.types import BenchmarkOutputOptions, DatasetDiagnostics

if TYPE_CHECKING:
    import duckdb


def _resolve_cleaned_address_expr(
    match_columns: set[str],
    messy_columns: set[str],
) -> str:
    if "clean_full_address" in match_columns:
        return "m.clean_full_address"
    if "clean_full_address" in messy_columns:
        return "messy.clean_full_address"
    return "m.original_address_concat"


def _optional_match_column_expr(match_columns: set[str], column: str) -> str:
    if column in match_columns:
        return f"m.{column}"
    return f"NULL AS {column}"


def _resolve_postcode_expr(match_columns: set[str], messy_columns: set[str]) -> str:
    if "postcode" in match_columns:
        return "m.postcode"
    if "postcode" in messy_columns:
        return "messy.postcode"
    return "NULL::VARCHAR"


def _resolve_splink_id_column(predictions: duckdb.DuckDBPyRelation) -> str:
    columns = set(predictions.columns)
    if "unique_id_r" in columns:
        return "unique_id_r"
    if "unique_id" in columns:
        return "unique_id"
    if "ukam_address_id_r" in columns:
        return "ukam_address_id_r"
    if "ukam_address_id" in columns:
        return "ukam_address_id"
    raise ValueError("Splink predictions table is missing expected unique-id columns.")


def _resolve_unmatched_join_key_expr(
    *,
    match_columns: set[str],
    splink_id_column: str,
) -> str:
    if splink_id_column in {"ukam_address_id_r", "ukam_address_id"}:
        if "ukam_address_id" in match_columns:
            return "CAST(m.ukam_address_id AS VARCHAR)"
        return "NULL::VARCHAR"
    return "CAST(m.unique_id AS VARCHAR)"


def _resolve_optional_ukam_address_id_expr(match_columns: set[str]) -> str:
    if "ukam_address_id" in match_columns:
        return "CAST(m.ukam_address_id AS VARCHAR)"
    return "NULL::VARCHAR"


def build_dataset_diagnostics(
    con: duckdb.DuckDBPyConnection,
    *,
    matches_table_name: str,
    messy_relation: duckdb.DuckDBPyRelation,
    canonical_relation: duckdb.DuckDBPyRelation | None,
    splink_predictions: duckdb.DuckDBPyRelation | None,
    output_options: BenchmarkOutputOptions | None = None,
) -> DatasetDiagnostics:
    output_options = output_options or BenchmarkOutputOptions(
        show_successful_matches=True,
        show_incorrect_matches=True,
        show_similarity_score_checks=True,
        show_unmatched_records=True,
    )
    need_successful_matches = output_options.show_successful_matches
    need_incorrect_matches = (
        output_options.show_incorrect_matches
        or output_options.show_similarity_score_checks
    )
    need_similarity_checks = output_options.show_similarity_score_checks
    need_unmatched_records = output_options.show_unmatched_records
    incorrect_match_sample_size = max(1, output_options.incorrect_match_sample_size)

    table_suffix = "".join(ch if ch.isalnum() else "_" for ch in matches_table_name)
    match_columns = set(con.table(matches_table_name).columns)
    messy_columns = set(messy_relation.columns)
    cleaned_address_expr = _resolve_cleaned_address_expr(match_columns, messy_columns)
    match_weight_expr = _optional_match_column_expr(match_columns, "match_weight")
    postcode_expr = _resolve_postcode_expr(match_columns, messy_columns)

    con.register("__simple_bench_messy__", messy_relation)

    incorrect_projection_without_reason_sql = """
        unique_id,
        ukam_label,
        resolved_canonical_id,
        postcode,
        original_address_concat,
        cleaned_full_address,
        clean_full_address_canonical,
        match_weight,
        similarity_score
    """

    incorrect_projection_sql = """
        unique_id,
        ukam_label,
        resolved_canonical_id,
        postcode,
        original_address_concat,
        cleaned_full_address,
        clean_full_address_canonical,
        match_weight,
        similarity_score,
        match_reason
    """

    suspicious_issue_type_sql = """
        CASE
            WHEN similarity_score IS NULL THEN 'missing_similarity'
            WHEN similarity_score <= 0.60 THEN 'very_low_similarity'
            WHEN similarity_score >= 0.98 THEN 'near_identical_but_wrong_id'
            ELSE 'other_mismatch'
        END
    """

    has_canonical = (
        canonical_relation is not None and "unique_id" in canonical_relation.columns
    )
    if has_canonical and canonical_relation is not None:
        con.register("__simple_bench_canonical__", canonical_relation)
        canonical_columns = set(canonical_relation.columns)
        canonical_clean_column = (
            "clean_full_address" if "clean_full_address" in canonical_columns else None
        )
        canonical_compare_column = (
            "original_address_concat"
            if "original_address_concat" in canonical_columns
            else (
                "clean_full_address"
                if "clean_full_address" in canonical_columns
                else None
            )
        )
        canonical_clean_source_expr = (
            f"c.{canonical_clean_column}" if canonical_clean_column else None
        )
        canonical_compare_source_expr = (
            f"c2.{canonical_compare_column}"
            if canonical_compare_column
            else "NULL::VARCHAR"
        )
        if canonical_clean_source_expr is not None:
            canonical_rollup_value_expr = (
                f"list(DISTINCT {canonical_clean_source_expr}) "
                f"FILTER (WHERE {canonical_clean_source_expr} IS NOT NULL)"
            )
        else:
            canonical_rollup_value_expr = "NULL::VARCHAR[]"

        if need_similarity_checks and canonical_compare_column is not None:
            similarity_score_expr = f"""
                CASE
                    WHEN m.original_address_concat IS NULL
                      OR NOT EXISTS (
                          SELECT 1
                          FROM __simple_bench_canonical__ AS c2
                          WHERE CAST(c2.unique_id AS VARCHAR) =
                                CAST(m.resolved_canonical_id AS VARCHAR)
                            AND {canonical_compare_source_expr} IS NOT NULL
                      )
                        THEN NULL::DOUBLE
                    ELSE (
                        SELECT MAX(
                            jaro_winkler_similarity(
                                m.original_address_concat,
                                {canonical_compare_source_expr}
                            )
                        )
                        FROM __simple_bench_canonical__ AS c2
                        WHERE CAST(c2.unique_id AS VARCHAR) =
                              CAST(m.resolved_canonical_id AS VARCHAR)
                          AND {canonical_compare_source_expr} IS NOT NULL
                    )
                END
            """
        else:
            similarity_score_expr = "NULL::DOUBLE"

        canonical_rollup_cte_sql = f"""
            canonical_rollup AS (
                SELECT
                    CAST(c.unique_id AS VARCHAR) AS canonical_id,
                    {canonical_rollup_value_expr} AS clean_full_address_canonical
                FROM __simple_bench_canonical__ AS c
                GROUP BY 1
            ),
        """
        canonical_rollup_join_sql = """
            LEFT JOIN canonical_rollup AS canonical_match
              ON CAST(canonical_match.canonical_id AS VARCHAR) =
                 CAST(m.resolved_canonical_id AS VARCHAR)
        """
    else:
        canonical_columns = set()
        canonical_clean_source_expr = None
        canonical_rollup_cte_sql = ""
        canonical_rollup_join_sql = ""
        similarity_score_expr = "NULL::DOUBLE"

    if need_successful_matches:
        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE __simple_bench_successful_{table_suffix} AS
            WITH
                {canonical_rollup_cte_sql}
                sampled AS (
                SELECT
                    m.match_reason,
                    m.unique_id,
                    m.ukam_label,
                    m.resolved_canonical_id,
                    {postcode_expr} AS postcode,
                    m.original_address_concat,
                    {cleaned_address_expr} AS cleaned_full_address,
                    canonical_match.clean_full_address_canonical,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.match_reason
                        ORDER BY m.unique_id
                    ) AS rn
                FROM {matches_table_name} AS m
                LEFT JOIN __simple_bench_messy__ AS messy
                    ON CAST(messy.unique_id AS VARCHAR) = CAST(m.unique_id AS VARCHAR)
                {canonical_rollup_join_sql}
                WHERE m.match_reason IS NOT NULL
                  AND m.resolved_canonical_id IS NOT NULL
                  AND CAST(m.ukam_label AS VARCHAR) = CAST(m.resolved_canonical_id AS VARCHAR)
            )
            SELECT
                match_reason,
                unique_id,
                ukam_label,
                resolved_canonical_id,
                postcode,
                original_address_concat,
                cleaned_full_address,
                clean_full_address_canonical
            FROM sampled
            WHERE rn <= 5
            ORDER BY match_reason, unique_id
            """
        )
        successful_matches = con.table(f"__simple_bench_successful_{table_suffix}")
    else:
        successful_matches = con.sql(
            """
            SELECT
                NULL::VARCHAR AS match_reason,
                NULL::VARCHAR AS unique_id,
                NULL::VARCHAR AS ukam_label,
                NULL::VARCHAR AS resolved_canonical_id,
                NULL::VARCHAR AS postcode,
                NULL::VARCHAR AS original_address_concat,
                NULL::VARCHAR AS cleaned_full_address,
                NULL::VARCHAR[] AS clean_full_address_canonical
            WHERE FALSE
            """
        )

    incorrect_filter = ""
    if has_canonical:
        incorrect_filter = """
          AND EXISTS (
              SELECT 1
              FROM __simple_bench_canonical__ AS c_truth
              WHERE CAST(c_truth.unique_id AS VARCHAR) = CAST(m.ukam_label AS VARCHAR)
          )
        """

    if need_incorrect_matches:
        canonical_join_sql = (
            canonical_rollup_join_sql.replace("m.", "base.")
            if need_similarity_checks
            else ""
        )
        similarity_score_sampled_expr = (
            similarity_score_expr.replace("m.", "base.")
            if need_similarity_checks
            else "NULL::DOUBLE"
        )
        canonical_projection_sql = (
            "sampled.clean_full_address_canonical"
            if need_similarity_checks
            else (
                "("
                "SELECT list(DISTINCT c.clean_full_address) "
                "FILTER (WHERE c.clean_full_address IS NOT NULL) "
                "FROM __simple_bench_canonical__ AS c "
                "WHERE CAST(c.unique_id AS VARCHAR) = "
                "CAST(sampled.resolved_canonical_id AS VARCHAR)"
                ")"
                if has_canonical and canonical_clean_source_expr is not None
                else "NULL::VARCHAR[]"
            )
        )
        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE __simple_bench_incorrect_{table_suffix} AS
            WITH
                {canonical_rollup_cte_sql if need_similarity_checks else ""}
                base AS (
                SELECT DISTINCT
                    m.match_reason,
                    m.unique_id,
                    m.ukam_label,
                    m.resolved_canonical_id,
                    {postcode_expr} AS postcode,
                    m.original_address_concat,
                    {cleaned_address_expr} AS cleaned_full_address,
                    {match_weight_expr}
                FROM {matches_table_name} AS m
                LEFT JOIN __simple_bench_messy__ AS messy
                    ON CAST(messy.unique_id AS VARCHAR) = CAST(m.unique_id AS VARCHAR)
                WHERE m.match_reason IS NOT NULL
                  AND m.resolved_canonical_id IS NOT NULL
                  AND CAST(m.ukam_label AS VARCHAR) !=
                      CAST(m.resolved_canonical_id AS VARCHAR)
                  {incorrect_filter}
            ),
                sampled AS (
                SELECT
                    base.match_reason,
                    base.unique_id,
                    base.ukam_label,
                    base.resolved_canonical_id,
                    base.postcode,
                    base.original_address_concat,
                    base.cleaned_full_address,
                    {("canonical_match.clean_full_address_canonical AS clean_full_address_canonical") if need_similarity_checks else "NULL::VARCHAR[] AS clean_full_address_canonical"},
                    base.match_weight,
                    {similarity_score_sampled_expr} AS similarity_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY base.match_reason
                        ORDER BY base.unique_id
                    ) AS rn
                FROM base
                {canonical_join_sql}
            )
            SELECT
                match_reason,
                unique_id,
                ukam_label,
                resolved_canonical_id,
                postcode,
                original_address_concat,
                cleaned_full_address,
                {canonical_projection_sql} AS clean_full_address_canonical,
                match_weight,
                ROUND(similarity_score, 3) AS similarity_score
            FROM sampled
            WHERE rn <= {incorrect_match_sample_size}
            ORDER BY match_reason, unique_id
            """
        )
        incorrect_matches = con.table(f"__simple_bench_incorrect_{table_suffix}")
    else:
        incorrect_matches = con.sql(
            """
            SELECT
                NULL::VARCHAR AS match_reason,
                NULL::VARCHAR AS unique_id,
                NULL::VARCHAR AS ukam_label,
                NULL::VARCHAR AS resolved_canonical_id,
                NULL::VARCHAR AS postcode,
                NULL::VARCHAR AS original_address_concat,
                NULL::VARCHAR AS cleaned_full_address,
                NULL::VARCHAR[] AS clean_full_address_canonical,
                NULL::DOUBLE AS match_weight,
                NULL::DOUBLE AS similarity_score
            WHERE FALSE
            """
        )

    if need_similarity_checks:
        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE
                __simple_bench_lowest_similarity_{table_suffix} AS
            SELECT
                {incorrect_projection_sql}
            FROM __simple_bench_incorrect_{table_suffix}
            ORDER BY similarity_score ASC NULLS LAST, match_reason, unique_id
            LIMIT 500
            """
        )
        lowest_similarity_incorrect = con.table(
            f"__simple_bench_lowest_similarity_{table_suffix}"
        )

        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE
                __simple_bench_highest_similarity_{table_suffix} AS
            SELECT
                {incorrect_projection_sql}
            FROM __simple_bench_incorrect_{table_suffix}
            ORDER BY similarity_score DESC NULLS LAST, match_reason, unique_id
            LIMIT 500
            """
        )
        highest_similarity_incorrect = con.table(
            f"__simple_bench_highest_similarity_{table_suffix}"
        )

        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE
                __simple_bench_suspicious_summary_{table_suffix} AS
            SELECT
                issue_type,
                COUNT(*) AS issue_count
            FROM (
                SELECT
                    {suspicious_issue_type_sql} AS issue_type
                FROM __simple_bench_incorrect_{table_suffix}
            ) AS issues
            GROUP BY issue_type
            ORDER BY issue_count DESC, issue_type
            """
        )
        suspicious_incorrect_summary = con.table(
            f"__simple_bench_suspicious_summary_{table_suffix}"
        )

        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE
                __simple_bench_suspicious_records_{table_suffix} AS
            SELECT
                {incorrect_projection_without_reason_sql},
                {suspicious_issue_type_sql} AS issue_type,
                match_reason
            FROM __simple_bench_incorrect_{table_suffix}
            ORDER BY
                CASE
                    WHEN similarity_score IS NULL THEN 1
                    WHEN similarity_score <= 0.60 THEN 2
                    WHEN similarity_score >= 0.98 THEN 3
                    ELSE 4
                END,
                similarity_score ASC NULLS LAST,
                unique_id
            LIMIT 20
            """
        )
        suspicious_incorrect_records = con.table(
            f"__simple_bench_suspicious_records_{table_suffix}"
        )
    else:
        lowest_similarity_incorrect = con.sql(
            """
            SELECT
                NULL::VARCHAR AS unique_id,
                NULL::VARCHAR AS ukam_label,
                NULL::VARCHAR AS resolved_canonical_id,
                NULL::VARCHAR AS postcode,
                NULL::VARCHAR AS original_address_concat,
                NULL::VARCHAR AS cleaned_full_address,
                NULL::VARCHAR[] AS clean_full_address_canonical,
                NULL::DOUBLE AS match_weight,
                NULL::DOUBLE AS similarity_score,
                NULL::VARCHAR AS match_reason
            WHERE FALSE
            """
        )
        highest_similarity_incorrect = lowest_similarity_incorrect
        suspicious_incorrect_summary = con.sql(
            """
            SELECT
                NULL::VARCHAR AS issue_type,
                NULL::BIGINT AS issue_count
            WHERE FALSE
            """
        )
        suspicious_incorrect_records = con.sql(
            """
            SELECT
                NULL::VARCHAR AS unique_id,
                NULL::VARCHAR AS ukam_label,
                NULL::VARCHAR AS resolved_canonical_id,
                NULL::VARCHAR AS postcode,
                NULL::VARCHAR AS original_address_concat,
                NULL::VARCHAR AS cleaned_full_address,
                NULL::VARCHAR[] AS clean_full_address_canonical,
                NULL::DOUBLE AS match_weight,
                NULL::DOUBLE AS similarity_score,
                NULL::VARCHAR AS issue_type,
                NULL::VARCHAR AS match_reason
            WHERE FALSE
            """
        )

    if need_unmatched_records:
        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE __simple_bench_unmatched_{table_suffix} AS
            SELECT
                m.unique_id,
                {postcode_expr} AS postcode,
                m.original_address_concat,
                {cleaned_address_expr} AS cleaned_full_address
            FROM {matches_table_name} AS m
            LEFT JOIN __simple_bench_messy__ AS messy
                ON CAST(messy.unique_id AS VARCHAR) = CAST(m.unique_id AS VARCHAR)
            WHERE m.match_reason IS NULL
            ORDER BY m.unique_id
            LIMIT 10
            """
        )
        unmatched_records = con.table(f"__simple_bench_unmatched_{table_suffix}")
    else:
        unmatched_records = con.sql(
            """
            SELECT
                NULL::VARCHAR AS unique_id,
                NULL::VARCHAR AS postcode,
                NULL::VARCHAR AS original_address_concat,
                NULL::VARCHAR AS cleaned_full_address
            WHERE FALSE
            """
        )

    unmatched_top_splink: duckdb.DuckDBPyRelation | None = None
    splink_available = splink_predictions is not None and need_unmatched_records

    if splink_predictions is not None and need_unmatched_records:
        con.register("__simple_bench_splink_predictions__", splink_predictions)
        splink_id_column = _resolve_splink_id_column(splink_predictions)
        unmatched_join_key_expr = _resolve_unmatched_join_key_expr(
            match_columns=match_columns,
            splink_id_column=splink_id_column,
        )
        optional_ukam_address_id_expr = _resolve_optional_ukam_address_id_expr(
            match_columns
        )

        con.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE
                __simple_bench_unmatched_splink_{table_suffix} AS
            WITH sampled_unmatched AS (
                SELECT
                    CAST(m.unique_id AS VARCHAR) AS unique_id,
                    {unmatched_join_key_expr} AS unmatched_join_key,
                    {optional_ukam_address_id_expr} AS ukam_address_id,
                    m.original_address_concat,
                    {cleaned_address_expr} AS cleaned_full_address
                FROM {matches_table_name} AS m
                LEFT JOIN __simple_bench_messy__ AS messy
                    ON CAST(messy.unique_id AS VARCHAR) = CAST(m.unique_id AS VARCHAR)
                WHERE m.match_reason IS NULL
                ORDER BY RANDOM()
                LIMIT 10
            ),
            top_splink_candidates AS (
                SELECT
                    CAST(pred.{splink_id_column} AS VARCHAR) AS splink_join_key,
                    pred.match_probability AS highest_splink_comparison,
                    pred.match_weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(pred.{splink_id_column} AS VARCHAR)
                        ORDER BY pred.match_probability DESC NULLS LAST,
                                 pred.match_weight DESC NULLS LAST
                    ) AS rn
                FROM __simple_bench_splink_predictions__ AS pred
                WHERE CAST(pred.{splink_id_column} AS VARCHAR) IN (
                    SELECT unmatched_join_key
                    FROM sampled_unmatched
                    WHERE unmatched_join_key IS NOT NULL
                )
            )
            SELECT
                su.unique_id,
                su.ukam_address_id,
                su.original_address_concat,
                su.cleaned_full_address,
                tsc.highest_splink_comparison,
                tsc.match_weight
            FROM sampled_unmatched AS su
            LEFT JOIN top_splink_candidates AS tsc
                ON su.unmatched_join_key = tsc.splink_join_key
               AND tsc.rn = 1
            ORDER BY su.unique_id
            """
        )
        unmatched_top_splink = con.table(
            f"__simple_bench_unmatched_splink_{table_suffix}"
        )

    con.unregister("__simple_bench_messy__")
    if has_canonical:
        con.unregister("__simple_bench_canonical__")
    if splink_predictions is not None and need_unmatched_records:
        con.unregister("__simple_bench_splink_predictions__")

    return DatasetDiagnostics(
        successful_matches=successful_matches,
        incorrect_matches=incorrect_matches,
        lowest_similarity_incorrect=lowest_similarity_incorrect,
        highest_similarity_incorrect=highest_similarity_incorrect,
        suspicious_incorrect_summary=suspicious_incorrect_summary,
        suspicious_incorrect_records=suspicious_incorrect_records,
        unmatched_records=unmatched_records,
        unmatched_top_splink=unmatched_top_splink,
        splink_available=splink_available,
    )
