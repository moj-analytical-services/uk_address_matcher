from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

import duckdb

from benchmarking.constants import DUCKDB_MAX_MEMORY
from uk_address_matcher.cleaning.steps.roadlike_places import (
    ROAD_FEATURE_COLUMNS,
    add_top_1_road_features,
    roadlike_place_prepared_candidate_sql,
    roadlike_place_prepared_input_sql,
)

DEFAULT_SOURCE = Path(
    "benchmarking/results/road_scoring_experiment/"
    "canonical_with_inferred_road/ukam_canonical_addresses.parquet"
)


def benchmark(source_path: Path, modulus: int, remainder: int, threads: int) -> dict:
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{DUCKDB_MAX_MEMORY}'")
    escaped_source = str(source_path).replace("'", "''")

    started = perf_counter()
    con.execute(f"""
        CREATE TEMPORARY TABLE road_parser_sample AS
        SELECT
            * EXCLUDE ({", ".join(ROAD_FEATURE_COLUMNS)}),
            road_1_norm AS expected_road_1_norm,
            road_1_confidence AS expected_road_1_confidence,
            road_1_token_count AS expected_road_1_token_count,
            road_1_margin AS expected_road_1_margin,
            road_1_distinctive_tokens AS expected_road_1_distinctive_tokens
        FROM read_parquet('{escaped_source}')
        WHERE hash(unique_id) % {modulus} = {remainder}
    """)
    sample_seconds = perf_counter() - started
    input_rows = int(con.table("road_parser_sample").count("*").fetchone()[0])

    started = perf_counter()
    con.execute(
        "CREATE TEMPORARY TABLE road_parser_prepared AS "
        + roadlike_place_prepared_input_sql("road_parser_sample")
    )
    con.execute("ANALYZE road_parser_prepared")
    prepare_seconds = perf_counter() - started
    prepared_rows = int(con.table("road_parser_prepared").count("*").fetchone()[0])

    started = perf_counter()
    con.execute(
        "CREATE TEMPORARY TABLE road_parser_candidates AS "
        + roadlike_place_prepared_candidate_sql("road_parser_prepared")
    )
    candidate_seconds = perf_counter() - started
    candidate_summary = con.execute("""
        SELECT
            count(*),
            bit_xor(hash(
                address_id,
                full_postcode,
                postcode_district,
                rightmost_numeric_value,
                numeric_anchor,
                tail_length,
                candidate_start_position,
                candidate_width,
                candidate_end_position,
                candidate_phrase,
                terminal_token
            )),
            sum(hash(
                address_id,
                full_postcode,
                postcode_district,
                rightmost_numeric_value,
                numeric_anchor,
                tail_length,
                candidate_start_position,
                candidate_width,
                candidate_end_position,
                candidate_phrase,
                terminal_token
            )::HUGEINT)
        FROM road_parser_candidates
    """).fetchone()
    candidate_rows = int(candidate_summary[0])

    parser_input = con.sql(
        "SELECT * EXCLUDE ("
        "expected_road_1_norm, expected_road_1_confidence, "
        "expected_road_1_token_count, expected_road_1_margin, "
        "expected_road_1_distinctive_tokens"
        ") FROM road_parser_sample"
    )
    started = perf_counter()
    parsed = add_top_1_road_features(con, parser_input)
    con.register("road_parser_output", parsed)
    parse_seconds = perf_counter() - started
    output = con.execute("""
        SELECT
            count(*) AS output_rows,
            count(*) FILTER (
                WHERE actual.road_1_norm IS DISTINCT FROM expected.expected_road_1_norm
            ) AS road_1_mismatches,
            count(*) FILTER (WHERE
                actual.road_1_norm
                    IS DISTINCT FROM expected.expected_road_1_norm
                OR actual.road_1_confidence
                    IS DISTINCT FROM expected.expected_road_1_confidence
                OR actual.road_1_token_count
                    IS DISTINCT FROM expected.expected_road_1_token_count
                OR actual.road_1_margin
                    IS DISTINCT FROM expected.expected_road_1_margin
                OR actual.road_1_distinctive_tokens
                    IS DISTINCT FROM expected.expected_road_1_distinctive_tokens
            ) AS road_feature_mismatches,
            count(*) FILTER (WHERE actual.road_1_norm IS NOT NULL) AS parsed_rows
        FROM road_parser_output AS actual
        JOIN road_parser_sample AS expected USING (ukam_address_id)
    """).fetchone()
    mismatch_examples = con.execute("""
        SELECT
            actual.unique_id,
            actual.clean_full_address,
            expected.expected_road_1_norm,
            actual.road_1_norm
        FROM road_parser_output AS actual
        JOIN road_parser_sample AS expected USING (ukam_address_id)
        WHERE actual.road_1_norm IS DISTINCT FROM expected.expected_road_1_norm
        ORDER BY actual.unique_id
        LIMIT 10
    """).fetchall()
    con.close()

    return {
        "source": str(source_path),
        "sample_modulus": modulus,
        "sample_remainder": remainder,
        "threads": threads,
        "input_rows": input_rows,
        "prepared_rows": prepared_rows,
        "candidate_rows": candidate_rows,
        "candidate_hash_xor": int(candidate_summary[1]),
        "candidate_hash_sum": int(candidate_summary[2]),
        "output_rows": int(output[0]),
        "parsed_rows": int(output[3]),
        "road_1_mismatches": int(output[1]),
        "road_feature_mismatches": int(output[2]),
        "road_1_mismatch_examples": mismatch_examples,
        "sample_seconds": sample_seconds,
        "prepare_seconds": prepare_seconds,
        "candidate_seconds": candidate_seconds,
        "parse_seconds": parse_seconds,
        "candidate_rows_per_second": candidate_rows / candidate_seconds,
        "input_rows_per_parse_second": input_rows / parse_seconds,
        "successful_parses_per_second": int(output[3]) / parse_seconds,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--modulus", type=int, default=100)
    parser.add_argument("--remainder", type=int, default=0)
    parser.add_argument("--threads", type=int, default=14)
    args = parser.parse_args()
    print(
        json.dumps(
            benchmark(args.source, args.modulus, args.remainder, args.threads),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
