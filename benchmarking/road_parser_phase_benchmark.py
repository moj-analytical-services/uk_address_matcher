from __future__ import annotations

import argparse
import importlib.resources as pkg_resources
import json
from contextlib import ExitStack
from pathlib import Path
from time import perf_counter

import duckdb

from benchmarking.constants import DUCKDB_MAX_MEMORY
from uk_address_matcher.cleaning.chunking_strategies import (
    _add_canonical_road_blocking_keys,
)
from uk_address_matcher.cleaning.steps.roadlike_places import (
    _road_candidate_feature_sql,
    _road_scorecard_feature_sql,
    add_top_1_road_features,
    derive_rightmost_numeric_position_sql,
    roadlike_place_catalog_sql,
    roadlike_place_prepared_candidate_sql,
    roadlike_place_prepared_input_sql,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_SOURCE = (
    PROJECT_ROOT
    / "benchmarking/results/road_scoring_experiment/"
    "canonical_with_inferred_road/ukam_canonical_addresses.parquet"
)
DEFAULT_TRUTH_SOURCE = (
    WORKSPACE_ROOT
    / "data/analysis_cache/canonical_residential_commercial_addresses.parquet"
)
MODEL_PATHS = {
    "pairwise_logistic_15": WORKSPACE_ROOT
    / "benchmarking/results/all_sector_pairwise_linear_ranker_v1/"
    "pairwise_linear_ranker.json",
    "additive_logistic_42": WORKSPACE_ROOT
    / "benchmarking/results/all_sector_additive_pairwise_ranker_v1/"
    "additive_pairwise_ranker.json",
    "balanced_additive_logistic_86": WORKSPACE_ROOT
    / "benchmarking/results/all_sector_additive_pairwise_ranker_balanced_v1/"
    "additive_pairwise_ranker.json",
}


def _timed_execute(con: duckdb.DuckDBPyConnection, sql: str) -> float:
    started = perf_counter()
    con.execute(sql)
    return perf_counter() - started


def _score_expression(scorecard: dict[str, object]) -> str:
    coefficients = scorecard["coefficients"]
    return " + ".join(
        [repr(float(scorecard["intercept"]))]
        + [
            f"({float(coefficients[feature])!r} * {feature})"
            for feature in scorecard["feature_columns"]
            if float(coefficients[feature]) != 0.0
        ]
    )


def _model_result(
    con: duckdb.DuckDBPyConnection,
    *,
    name: str,
    scorecard: dict[str, object],
    candidate_rows: int,
    base_feature_seconds: float,
) -> dict[str, object]:
    scorecard.setdefault("rules", [])
    score_seconds = _timed_execute(
        con,
        f"""
        CREATE OR REPLACE TEMPORARY TABLE road_phase_scores AS
        WITH scorecard_features AS (
            SELECT
                address_id,
                candidate_phrase,
                candidate_start_position,
                {_road_scorecard_feature_sql(scorecard)}
            FROM road_phase_base_features AS candidate_features
        )
        SELECT
            address_id,
            candidate_phrase,
            candidate_start_position,
            {_score_expression(scorecard)} AS ranker_logit
        FROM scorecard_features
        """,
    )
    winner_seconds = _timed_execute(
        con,
        """
        CREATE OR REPLACE TEMPORARY TABLE road_phase_winners AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY address_id
                    ORDER BY ranker_logit DESC, candidate_phrase,
                        candidate_start_position
                ) AS candidate_rank,
                lead(ranker_logit) OVER (
                    PARTITION BY address_id
                    ORDER BY ranker_logit DESC, candidate_phrase,
                        candidate_start_position
                ) AS runner_up_logit
            FROM road_phase_scores
        )
        SELECT
            address_id,
            candidate_phrase,
            ranker_logit,
            coalesce(ranker_logit - runner_up_logit, ranker_logit) AS ranker_margin
        FROM ranked
        WHERE candidate_rank = 1
        """,
    )
    winner_rows, matching_rows, compared_rows = con.execute("""
        SELECT
            count(winners.address_id),
            count(*) FILTER (
                WHERE winners.candidate_phrase
                    IS NOT DISTINCT FROM input.expected_road_1_norm
            ),
            count(*)
        FROM road_phase_input AS input
        LEFT JOIN road_phase_winners AS winners
            ON CAST(input.unique_id AS VARCHAR) = winners.address_id
    """).fetchone()
    frozen_test = scorecard.get("frozen_test", {})
    return {
        "name": name,
        "artifact_feature_terms": len(scorecard["feature_columns"]),
        "active_feature_terms": sum(
            float(scorecard["coefficients"][feature]) != 0.0
            for feature in scorecard["feature_columns"]
        ),
        "tree_rules": len(scorecard["rules"]),
        "base_feature_seconds": base_feature_seconds,
        "model_score_seconds": score_seconds,
        "standalone_score_seconds": base_feature_seconds + score_seconds,
        "winner_seconds": winner_seconds,
        "candidate_score_rows_per_second": candidate_rows / score_seconds,
        "candidate_standalone_score_rows_per_second": candidate_rows
        / (base_feature_seconds + score_seconds),
        "winner_rows": int(winner_rows),
        "deployed_output_agreement": int(matching_rows) / int(compared_rows),
        "frozen_test_safe_core": frozen_test.get("safe_core_top_one"),
        "frozen_test_exact_top_one": frozen_test.get("exact_top_one"),
    }


def _production_result(
    con: duckdb.DuckDBPyConnection,
    *,
    input_rows: int,
    input_policy: str,
    truth_source: Path | None,
    road_chunks: int,
    validate_output: bool,
    require_catalogue_support: bool,
) -> dict[str, object]:
    if input_policy == "preferred":
        parser_input_sql = """
            SELECT * EXCLUDE (expected_road_1_norm)
            FROM road_phase_input
            QUALIFY row_number() OVER (
                PARTITION BY unique_id
                ORDER BY CASE filename
                    WHEN 'add_gb_builtaddress.parquet' THEN 1
                    WHEN 'add_gb_royalmailaddress.parquet' THEN 2
                    WHEN 'CUSTOM_LEVEL' THEN 3
                    WHEN 'add_gb_builtaddress_altadd.parquet' THEN 4
                    WHEN 'add_gb_prebuildaddress.parquet' THEN 5
                    WHEN 'add_gb_prebuildaddress_altadd.parquet' THEN 6
                    WHEN 'add_gb_nonaddressableobject.parquet' THEN 7
                    ELSE 8
                END,
                clean_full_address,
                postcode,
                ukam_address_id
            ) = 1
        """
        comparison_join = "CAST(expected.unique_id AS VARCHAR) = actual.unique_id"
    else:
        parser_input_sql = """
            SELECT * EXCLUDE (expected_road_1_norm)
            FROM road_phase_input
        """
        comparison_join = "expected.ukam_address_id = actual.ukam_address_id"
    parser_input = con.sql(parser_input_sql)
    if input_policy == "canonical":
        parser_input_rows = int(
            con.sql(f"""
                SELECT count(DISTINCT unique_id)
                FROM ({parser_input.sql_query()})
            """).fetchone()[0]
        )
    else:
        parser_input_rows = int(parser_input.count("*").fetchone()[0])
    started = perf_counter()
    road_key_seconds = None
    canonical_materialization_seconds = None
    if input_policy == "canonical":
        canonical_output = _add_canonical_road_blocking_keys(
            parser_input,
            con,
            num_of_chunks=road_chunks,
            require_catalogue_support=require_catalogue_support,
        )
        road_key_seconds = perf_counter() - started
        materialization_started = perf_counter()
        con.execute(f"""
            CREATE TEMPORARY TABLE road_canonical_output AS
            SELECT * FROM ({canonical_output.sql_query()})
        """)
        canonical_materialization_seconds = perf_counter() - materialization_started
        parsed = con.table("road_canonical_output")
    else:
        parsed = add_top_1_road_features(con, parser_input)
    parse_seconds = perf_counter() - started
    con.register("road_production_output", parsed)
    if validate_output:
        output_rows, parsed_output_rows = con.execute("""
            SELECT
                count(*),
                count(*) FILTER (WHERE road_1_norm IS NOT NULL)
            FROM road_production_output
        """).fetchone()
        compared_rows, road_mismatches, compared_parsed_rows = con.execute(f"""
            SELECT
                count(*),
                count(*) FILTER (
                    WHERE actual.road_1_norm
                        IS DISTINCT FROM expected.expected_road_1_norm
                ),
                count(*) FILTER (WHERE actual.road_1_norm IS NOT NULL)
            FROM road_phase_input AS expected
            LEFT JOIN road_production_output AS actual
                ON {comparison_join}
        """).fetchone()
    else:
        output_rows = input_rows
        parsed_output_rows = None
        compared_rows = None
        road_mismatches = None
        compared_parsed_rows = None
    truth_metrics = None
    if truth_source is not None:
        escaped_truth_source = str(truth_source).replace("'", "''")
        con.execute(f"""
            CREATE TEMPORARY TABLE road_phase_ground_truth AS
            WITH input_ids AS (
                SELECT DISTINCT CAST(unique_id AS VARCHAR) AS unique_id
                FROM road_phase_input
            ), truth_rows AS (
                SELECT
                    CAST(truth.source_unique_id AS VARCHAR) AS unique_id,
                    truth.cohort,
                    truth.street_raw,
                    truth.candidate_status
                FROM read_parquet('{escaped_truth_source}') AS truth
                JOIN input_ids
                    ON CAST(truth.source_unique_id AS VARCHAR) = input_ids.unique_id
            ), preferred AS (
                SELECT *
                FROM truth_rows
                QUALIFY row_number() OVER (
                    PARTITION BY unique_id
                    ORDER BY CASE cohort WHEN 'built' THEN 1 ELSE 2 END, street_raw
                ) = 1
            )
            SELECT
                unique_id,
                NULLIF(trim(regexp_replace(
                    upper(coalesce(street_raw, '')), '[^A-Z0-9]+', ' ', 'g'
                )), '') AS true_road
            FROM preferred
            WHERE candidate_status = 'eligible'
              AND street_raw IS NOT NULL
        """)
        truth_rows, accepted_rows, correct_rows = con.execute("""
            WITH predictions AS (
                SELECT
                    CAST(unique_id AS VARCHAR) AS unique_id,
                    min(road_1_norm) AS predicted_road
                FROM road_production_output
                GROUP BY unique_id
            )
            SELECT
                count(*),
                count(predicted_road),
                count(*) FILTER (WHERE predicted_road = true_road)
            FROM road_phase_ground_truth
            LEFT JOIN predictions USING (unique_id)
        """).fetchone()
        precision = int(correct_rows) / int(accepted_rows)
        recall = int(correct_rows) / int(truth_rows)
        truth_metrics = {
            "truth_rows": int(truth_rows),
            "accepted_rows": int(accepted_rows),
            "correct_rows": int(correct_rows),
            "precision": precision,
            "recall": recall,
            "f1": 2 * precision * recall / (precision + recall),
        }
    return {
        "input_policy": input_policy,
        "input_rows": input_rows,
        "parser_input_rows": parser_input_rows,
        "output_rows": int(output_rows),
        "parsed_rows": (
            None if parsed_output_rows is None else int(parsed_output_rows)
        ),
        "compared_rows": None if compared_rows is None else int(compared_rows),
        "compared_parsed_rows": (
            None if compared_parsed_rows is None else int(compared_parsed_rows)
        ),
        "road_1_mismatches": (
            None if road_mismatches is None else int(road_mismatches)
        ),
        "parse_seconds": parse_seconds,
        "road_key_seconds": road_key_seconds,
        "canonical_materialization_seconds": canonical_materialization_seconds,
        "input_rows_per_second": input_rows / parse_seconds,
        "parser_input_rows_per_second": parser_input_rows / parse_seconds,
        "successful_parses_per_second": (
            None
            if parsed_output_rows is None
            else int(parsed_output_rows) / parse_seconds
        ),
        "truth_metrics": truth_metrics,
    }


def benchmark(
    source_path: Path,
    *,
    modulus: int,
    remainder: int,
    threads: int,
    memory_limit: str,
    input_policy: str = "all",
    truth_source: Path | None = None,
    road_chunks: int = 1,
    validate_output: bool = True,
    precompute_numeric_position: bool = False,
    candidate_profile: Path | None = None,
    require_catalogue_support: bool = False,
    preserve_insertion_order: bool = True,
    production_only: bool = False,
) -> dict[str, object]:
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(
        f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
    )
    con.execute("SET enable_progress_bar = false")
    escaped_source = str(source_path).replace("'", "''")
    sample_filter = (
        "" if modulus == 1 else f"WHERE hash(unique_id) % {modulus} = {remainder}"
    )

    input_table = (
        "road_phase_input_base"
        if precompute_numeric_position
        else "road_phase_input"
    )
    input_seconds = _timed_execute(
        con,
        f"""
        CREATE TEMPORARY TABLE {input_table} AS
        SELECT
            unique_id,
            ukam_address_id,
            clean_full_address,
            postcode,
            numeric_tokens,
            numeric_token_1,
            unusual_tokens_arr,
            filename,
            road_1_norm AS expected_road_1_norm
        FROM read_parquet('{escaped_source}')
        {sample_filter}
        """,
    )
    numeric_position_seconds = 0.0
    if precompute_numeric_position:
        numeric_position_seconds = _timed_execute(
            con,
            "CREATE TEMPORARY TABLE road_phase_input AS "
            + derive_rightmost_numeric_position_sql("road_phase_input_base"),
        )
        con.execute("DROP TABLE road_phase_input_base")
    input_rows = int(con.table("road_phase_input").count("*").fetchone()[0])
    if production_only:
        production = _production_result(
            con,
            input_rows=input_rows,
            input_policy=input_policy,
            truth_source=truth_source,
            road_chunks=road_chunks,
            validate_output=validate_output,
            require_catalogue_support=require_catalogue_support,
        )
        con.close()
        return {
            "source": str(source_path),
            "sample_modulus": modulus,
            "sample_remainder": remainder,
            "threads": threads,
            "memory_limit": memory_limit,
            "preserve_insertion_order": preserve_insertion_order,
            "input_materialization_seconds": input_seconds,
            "numeric_position_seconds": numeric_position_seconds,
            "production": production,
        }

    prepare_seconds = _timed_execute(
        con,
        "CREATE TEMPORARY TABLE road_phase_prepared AS "
        + roadlike_place_prepared_input_sql("road_phase_input"),
    )
    con.execute("ANALYZE road_phase_prepared")
    prepared_rows = int(con.table("road_phase_prepared").count("*").fetchone()[0])
    if candidate_profile is not None:
        candidate_profile.parent.mkdir(parents=True, exist_ok=True)
        escaped_profile = str(candidate_profile).replace("'", "''")
        con.execute("SET enable_profiling = 'json'")
        con.execute(f"SET profiling_output = '{escaped_profile}'")
    candidate_seconds = _timed_execute(
        con,
        "CREATE TEMPORARY TABLE road_phase_candidates AS "
        + roadlike_place_prepared_candidate_sql("road_phase_prepared"),
    )
    if candidate_profile is not None:
        con.execute("SET enable_profiling = 'no_output'")
    candidate_rows = int(con.table("road_phase_candidates").count("*").fetchone()[0])
    generated_catalogue_seconds = _timed_execute(
        con,
        "CREATE TEMPORARY TABLE road_phase_generated_catalogue AS "
        + roadlike_place_catalog_sql("road_phase_candidates"),
    )
    generated_catalogue_rows = int(
        con.table("road_phase_generated_catalogue").count("*").fetchone()[0]
    )

    with ExitStack() as resources:
        catalogue_path = resources.enter_context(
            pkg_resources.as_file(
                pkg_resources.files("uk_address_matcher.data").joinpath(
                    "roadlike_places.parquet"
                )
            )
        )
        selected_model_path = resources.enter_context(
            pkg_resources.as_file(
                pkg_resources.files("uk_address_matcher.data").joinpath(
                    "road_assignment_scorecard_v1.json"
                )
            )
        )
        escaped_catalogue = str(catalogue_path).replace("'", "''")
        con.execute(
            "CREATE TEMPORARY VIEW road_phase_catalogue AS "
            f"SELECT * FROM read_parquet('{escaped_catalogue}')"
        )
        base_feature_seconds = _timed_execute(
            con,
            "CREATE TEMPORARY TABLE road_phase_base_features AS "
            + _road_candidate_feature_sql(
                "road_phase_candidates", "road_phase_catalogue"
            ),
        )

        models = {
            "selected_tree_scorecard_150": json.loads(
                selected_model_path.read_text(encoding="utf-8")
            ),
            **{
                name: json.loads(path.read_text(encoding="utf-8"))
                for name, path in MODEL_PATHS.items()
            },
        }
        model_results = [
            _model_result(
                con,
                name=name,
                scorecard=model,
                candidate_rows=candidate_rows,
                base_feature_seconds=base_feature_seconds,
            )
            for name, model in models.items()
        ]

    con.close()
    selected = model_results[0]
    parser_seconds = (
        prepare_seconds
        + candidate_seconds
        + float(selected["standalone_score_seconds"])
        + float(selected["winner_seconds"])
    )
    return {
        "source": str(source_path),
        "sample_modulus": modulus,
        "sample_remainder": remainder,
        "threads": threads,
        "memory_limit": memory_limit,
        "preserve_insertion_order": preserve_insertion_order,
        "input_rows": input_rows,
        "prepared_rows": prepared_rows,
        "candidate_rows": candidate_rows,
        "input_materialization_seconds": input_seconds,
        "numeric_position_seconds": numeric_position_seconds,
        "prepare_seconds": prepare_seconds,
        "candidate_seconds": candidate_seconds,
        "generated_catalogue_seconds": generated_catalogue_seconds,
        "generated_catalogue_rows": generated_catalogue_rows,
        "base_feature_seconds": base_feature_seconds,
        "selected_parser_seconds_excluding_source_read": parser_seconds,
        "selected_input_rows_per_second": input_rows / parser_seconds,
        "models": model_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--modulus", type=int, default=20)
    parser.add_argument("--remainder", type=int, default=0)
    parser.add_argument("--threads", type=int, default=14)
    parser.add_argument("--memory-limit", default=DUCKDB_MAX_MEMORY)
    parser.add_argument(
        "--input-policy",
        choices=("all", "preferred", "canonical"),
        default="all",
    )
    parser.add_argument("--truth-source", type=Path, default=DEFAULT_TRUTH_SOURCE)
    parser.add_argument("--no-truth", action="store_true")
    parser.add_argument("--road-chunks", type=int, default=1)
    parser.add_argument("--no-validation", action="store_true")
    parser.add_argument("--precompute-numeric-position", action="store_true")
    parser.add_argument("--candidate-profile", type=Path)
    parser.add_argument("--require-catalogue-support", action="store_true")
    parser.add_argument("--no-preserve-insertion-order", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--production-only", action="store_true")
    args = parser.parse_args()
    result = benchmark(
        args.source,
        modulus=args.modulus,
        remainder=args.remainder,
        threads=args.threads,
        memory_limit=args.memory_limit,
        input_policy=args.input_policy,
        truth_source=None if args.no_truth else args.truth_source,
        road_chunks=args.road_chunks,
        validate_output=not args.no_validation,
        precompute_numeric_position=args.precompute_numeric_position,
        candidate_profile=args.candidate_profile,
        require_catalogue_support=args.require_catalogue_support,
        preserve_insertion_order=not args.no_preserve_insertion_order,
        production_only=args.production_only,
    )
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
