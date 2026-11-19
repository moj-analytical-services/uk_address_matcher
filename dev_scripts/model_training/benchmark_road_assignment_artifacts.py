"""Benchmark the DuckDB-native road-assignment artifact pipeline."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import duckdb

from uk_address_matcher.cleaning.steps.road_resources import sql_text

from .road_assignment_artifacts import (
    create_candidate_table,
    create_phrase_catalog_from_candidates,
    create_ranker_winners,
    load_folded_ranker_model,
    score_candidate_relation_sql,
)


def _count_rows(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return int(con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])


def run_benchmark(
    *,
    source_database: Path,
    folded_ranker_path: Path,
    output_directory: Path,
    threads: int | None,
) -> dict[str, object]:
    """Benchmark serving mechanics using the full national prepared corpus."""
    output_directory.mkdir(parents=True, exist_ok=True)
    output_database = output_directory / "road_assignment_artifacts.duckdb"
    con = duckdb.connect(str(output_database))
    if threads is not None:
        con.execute(f"SET threads TO {threads}")
    con.execute(
        f"ATTACH {sql_text(str(source_database.resolve()))} AS source (READ_ONLY)"
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW benchmark_input AS
        SELECT
            unique_id,
            peeled_address AS clean_full_address,
            full_postcode AS postcode,
            postcode_district,
            peeled_tokens,
            rightmost_numeric_position,
            rightmost_numeric_value,
            list_transform(
                numeric_positions,
                position -> list_extract(peeled_tokens, position)
            ) AS numeric_tokens
        FROM source.all_sector_prepared_addresses
        """
    )
    source_rows = _count_rows(con, "benchmark_input")

    candidate_started = time.perf_counter()
    create_candidate_table(
        con,
        source_relation="benchmark_input",
        candidate_table="benchmark_candidates",
        temporary=True,
        use_prepared_fields=True,
    )
    candidate_seconds = time.perf_counter() - candidate_started
    candidate_rows = _count_rows(con, "benchmark_candidates")

    catalog_started = time.perf_counter()
    create_phrase_catalog_from_candidates(
        con,
        candidate_relation="benchmark_candidates",
        catalog_table="road_assignment_phrase_catalog",
    )
    catalog_seconds = time.perf_counter() - catalog_started
    catalog_rows = _count_rows(con, "road_assignment_phrase_catalog")

    scoring_started = time.perf_counter()
    ranker = load_folded_ranker_model(folded_ranker_path)
    score_rows = score_candidate_relation_sql(
        con,
        model=ranker.model,
        candidate_relation="benchmark_candidates",
        temporary=True,
        feature_definitions=ranker.feature_definitions,
    )
    scoring_seconds = time.perf_counter() - scoring_started

    winner_started = time.perf_counter()
    create_ranker_winners(
        con,
        score_table="road_assignment_additive_scores",
        winner_table="benchmark_winners",
        score_column="ranker_logit",
        temporary=True,
    )
    winner_seconds = time.perf_counter() - winner_started
    winner_rows = _count_rows(con, "benchmark_winners")

    result: dict[str, object] = {
        "source_rows": source_rows,
        "candidate_rows": candidate_rows,
        "catalog_rows": catalog_rows,
        "winner_rows": winner_rows,
        "threads": threads,
        "timings_seconds": {
            "candidate_generation": candidate_seconds,
            "catalog_build": catalog_seconds,
            "sql_scoring": scoring_seconds,
            "winner_selection": winner_seconds,
            "total": candidate_seconds
            + catalog_seconds
            + scoring_seconds
            + winner_seconds,
        },
        "throughput": {
            "candidate_generation_rows_per_second": candidate_rows / candidate_seconds,
            "sql_scoring_rows_per_second": score_rows / scoring_seconds,
            "winner_rows_per_second": winner_rows / winner_seconds,
        },
        "notes": [
            "Uses pre-cleaned national input to isolate road-artifact execution.",
            "Does not train or evaluate model quality.",
            "Candidates, scores, and winners are temporary; only the catalog persists.",
        ],
    }
    (output_directory / "timings.json").write_text(
        json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"
    )
    con.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, required=True)
    parser.add_argument("--folded-ranker-path", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    arguments = parser.parse_args()
    result = run_benchmark(
        source_database=arguments.source_database,
        folded_ranker_path=arguments.folded_ranker_path,
        output_directory=arguments.output_directory,
        threads=arguments.threads,
    )
    print(json.dumps(result, sort_keys=True))  # noqa: T201


if __name__ == "__main__":
    main()
