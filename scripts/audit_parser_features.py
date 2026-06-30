from __future__ import annotations

import argparse

from benchmarking.config.datasets import load_dataset
from benchmarking.utils.io import setup_connection
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
)


def _print_section(title: str, rows) -> None:
    print(f"\n=== {title} ===")
    for row in rows:
        print(row)


def _queries(table_name: str) -> dict[str, str]:
    return {
        "flat_false_positive_counts": f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '\\b\\d{{1,4}}[A-Z]\\b') THEN 1 ELSE 0 END) AS any_alnum_number_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '\\b\\d{{1,4}}[A-Z]\\b') AND NOT has_flat_indicator THEN 1 ELSE 0 END) AS alnum_number_now_not_flat,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^\\d{{1,4}}[A-Z]\\b') AND NOT has_flat_indicator THEN 1 ELSE 0 END) AS leading_alnum_now_not_flat
            FROM {table_name}
        """,
        "flat_false_positive_samples": f"""
            SELECT
                clean_full_address,
                flat_letter,
                flat_number,
                flat_positional,
                has_flat_indicator,
                numeric_token_1
            FROM {table_name}
            WHERE regexp_matches(clean_full_address, '^\\d{{1,4}}[A-Z]\\b')
              AND NOT has_flat_indicator
            ORDER BY clean_full_address
            LIMIT 15
        """,
        "still_flat_leading_alnum_samples": f"""
            SELECT
                clean_full_address,
                flat_letter,
                flat_number,
                flat_positional,
                has_flat_indicator
            FROM {table_name}
            WHERE regexp_matches(clean_full_address, '^\\d{{1,4}}[A-Z]\\b')
              AND has_flat_indicator
            ORDER BY clean_full_address
            LIMIT 15
        """,
        "business_unit_suppression_counts": f"""
            SELECT
                SUM(CASE WHEN regexp_matches(clean_full_address, '^UNIT\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') THEN 1 ELSE 0 END) AS unit_shell_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^UNIT\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') AND has_business_unit THEN 1 ELSE 0 END) AS unit_shell_current_business,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^UNIT\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') AND regexp_matches(clean_full_address, '\\b(LEFT|RIGHT|FRONT|REAR|FLOOR|BASEMENT|GARDEN)\\b') THEN 1 ELSE 0 END) AS residential_like_unit_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^UNIT\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') AND regexp_matches(clean_full_address, '\\b(LEFT|RIGHT|FRONT|REAR|FLOOR|BASEMENT|GARDEN)\\b') AND has_business_unit THEN 1 ELSE 0 END) AS residential_like_unit_still_business,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^STUDIO\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') THEN 1 ELSE 0 END) AS studio_shell_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^STUDIO\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b') AND has_business_unit THEN 1 ELSE 0 END) AS studio_shell_current_business,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^STUDIO\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\s+\\d') THEN 1 ELSE 0 END) AS residential_like_studio_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^STUDIO\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\s+\\d') AND has_business_unit THEN 1 ELSE 0 END) AS residential_like_studio_still_business
            FROM {table_name}
        """,
        "business_unit_suppression_samples": f"""
            SELECT
                clean_full_address,
                has_business_unit,
                business_unit_type,
                business_unit_id,
                sub_premise_location,
                flat_positional
            FROM {table_name}
            WHERE (
                regexp_matches(clean_full_address, '^UNIT\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\b')
                AND regexp_matches(clean_full_address, '\\b(LEFT|RIGHT|FRONT|REAR|FLOOR|BASEMENT|GARDEN)\\b')
            ) OR regexp_matches(clean_full_address, '^STUDIO\\s+[A-Za-z]?\\d{{1,5}}[A-Za-z]?\\s+\\d')
            ORDER BY clean_full_address
            LIMIT 15
        """,
        "centre_hits": f"""
            SELECT
                clean_full_address,
                sub_premise_location,
                flat_positional,
                flat_letter,
                flat_number,
                has_business_unit,
                business_unit_type
            FROM {table_name}
            WHERE sub_premise_location = 'CENTRE'
            ORDER BY clean_full_address
            LIMIT 25
        """,
        "new_only_subpremise_counts": f"""
            SELECT
                SUM(
                    CASE WHEN sub_premise_location IS NOT NULL
                        AND regexp_matches(clean_full_address, '^(APARTMENT|PENTHOUSE|ROOM|UNIT|STUDIO)\\b')
                        AND flat_positional IS NULL
                        AND flat_letter IS NULL
                        AND flat_number IS NULL
                        AND NOT regexp_matches(clean_full_address, '\\b(FLAT|MAISONETTE)\\b')
                    THEN 1 ELSE 0 END
                ) AS new_only_rows,
                SUM(
                    CASE WHEN sub_premise_location IS NOT NULL
                        AND regexp_matches(clean_full_address, '^UNIT\\b')
                        AND flat_positional IS NULL
                        AND flat_letter IS NULL
                        AND flat_number IS NULL
                        AND NOT regexp_matches(clean_full_address, '\\b(FLAT|MAISONETTE)\\b')
                    THEN 1 ELSE 0 END
                ) AS new_only_unit_rows,
                SUM(
                    CASE WHEN sub_premise_location IS NOT NULL
                        AND regexp_matches(clean_full_address, '^STUDIO\\b')
                        AND flat_positional IS NULL
                        AND flat_letter IS NULL
                        AND flat_number IS NULL
                        AND NOT regexp_matches(clean_full_address, '\\b(FLAT|MAISONETTE)\\b')
                    THEN 1 ELSE 0 END
                ) AS new_only_studio_rows,
                SUM(
                    CASE WHEN sub_premise_location IS NOT NULL
                        AND regexp_matches(clean_full_address, '^APARTMENT\\b')
                        AND flat_positional IS NULL
                        AND flat_letter IS NULL
                        AND flat_number IS NULL
                        AND NOT regexp_matches(clean_full_address, '\\b(FLAT|MAISONETTE)\\b')
                    THEN 1 ELSE 0 END
                ) AS new_only_apartment_rows
            FROM {table_name}
        """,
        "new_only_subpremise_samples": f"""
            SELECT
                clean_full_address,
                sub_premise_location,
                flat_positional,
                flat_letter,
                flat_number
            FROM {table_name}
            WHERE sub_premise_location IS NOT NULL
              AND regexp_matches(clean_full_address, '^(APARTMENT|PENTHOUSE|ROOM|UNIT|STUDIO)\\b')
              AND flat_positional IS NULL
              AND flat_letter IS NULL
              AND flat_number IS NULL
              AND NOT regexp_matches(clean_full_address, '\\b(FLAT|MAISONETTE)\\b')
            ORDER BY clean_full_address
            LIMIT 20
        """,
        "numeric_token_preservation_counts": f"""
            SELECT
                SUM(CASE WHEN regexp_matches(clean_full_address, '^\\d{{1,4}}[A-Z]\\b') THEN 1 ELSE 0 END) AS leading_alnum_rows,
                SUM(CASE WHEN regexp_matches(clean_full_address, '^\\d{{1,4}}[A-Z]\\b') AND regexp_matches(COALESCE(numeric_token_1, ''), '^[0-9]+[A-Z]$') THEN 1 ELSE 0 END) AS leading_alnum_preserved_in_numeric_token_1
            FROM {table_name}
        """,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Audit parser feature deltas on a benchmark dataset without relying on "
            "canonical-side benchmark matches."
        )
    )
    parser.add_argument("--dataset", default="hackney")
    parser.add_argument("--sample-mode", action="store_true")
    parser.add_argument("--num-of-chunks", type=int, default=1)
    args = parser.parse_args()

    con = setup_connection()
    messy = load_dataset(con, dataset_key=args.dataset, sample_mode=args.sample_mode)
    cleaned = clean_data_pre_term_frequencies(
        messy,
        con,
        num_of_chunks=args.num_of_chunks,
        show_progress=False,
    )

    table_name = f"tmp_parser_feature_audit_{args.dataset}"
    con.sql(f"DROP TABLE IF EXISTS {table_name}")
    cleaned.create(table_name)

    print(
        f"Auditing dataset={args.dataset} sample_mode={args.sample_mode} "
        f"num_of_chunks={args.num_of_chunks}"
    )

    for title, sql in _queries(table_name).items():
        _print_section(title, con.sql(sql).fetchall())


if __name__ == "__main__":
    main()
