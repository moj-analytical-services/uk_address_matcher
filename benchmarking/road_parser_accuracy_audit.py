"""Audit top-k road assignment accuracy and nested-candidate failure modes."""

from __future__ import annotations

import argparse
import importlib.resources as pkg_resources
import json
from contextlib import ExitStack
from pathlib import Path

import duckdb

from benchmarking.constants import DUCKDB_MAX_MEMORY
from uk_address_matcher.cleaning.steps.roadlike_places import (
    _score_road_candidates,
    roadlike_place_prepared_candidate_sql,
    roadlike_place_prepared_input_sql,
)


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_audit(
    *,
    source: Path,
    truth_source: Path,
    modulus: int,
    remainder: int,
    threads: int,
) -> dict[str, object]:
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = {_sql_text(DUCKDB_MAX_MEMORY)}")
    con.execute(
        f"""
        CREATE TEMPORARY TABLE audit_input AS
        SELECT *
        FROM read_parquet({_sql_text(str(source))})
        WHERE hash(unique_id) % {modulus} = {remainder}
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
    )
    con.execute(
        "CREATE TEMPORARY TABLE audit_prepared AS "
        + roadlike_place_prepared_input_sql(
            "audit_input",
            use_precomputed_numeric_position=(
                "rightmost_numeric_position" in con.table("audit_input").columns
            ),
        )
    )
    with ExitStack() as resources:
        catalogue_path = resources.enter_context(
            pkg_resources.as_file(
                pkg_resources.files("uk_address_matcher.data").joinpath(
                    "roadlike_places.parquet"
                )
            )
        )
        scorecard_path = resources.enter_context(
            pkg_resources.as_file(
                pkg_resources.files("uk_address_matcher.data").joinpath(
                    "road_assignment_scorecard_v1.json"
                )
            )
        )
        con.execute(
            "CREATE TEMPORARY VIEW audit_catalogue AS SELECT * FROM read_parquet("
            f"{_sql_text(str(catalogue_path))})"
        )
        scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
        candidates = roadlike_place_prepared_candidate_sql(
            "audit_prepared", catalogue_width_relation="audit_catalogue"
        )
        _score_road_candidates(
            con,
            candidate_relation=f"({candidates})",
            catalogue_view="audit_catalogue",
            scorecard=scorecard,
            output_table="audit_scores",
            require_catalogue_support=True,
        )

    con.execute(
        f"""
        CREATE TEMPORARY TABLE audit_truth AS
        WITH sampled_ids AS (
            SELECT CAST(unique_id AS VARCHAR) AS unique_id FROM audit_input
        ), truth_rows AS (
            SELECT
                CAST(source_unique_id AS VARCHAR) AS unique_id,
                cohort,
                street_raw,
                candidate_status,
                NULLIF(trim(regexp_replace(
                    upper(coalesce(street_raw, '')), '[^A-Z0-9]+', ' ', 'g'
                )), '') AS true_road
            FROM read_parquet({_sql_text(str(truth_source))})
            JOIN sampled_ids ON CAST(source_unique_id AS VARCHAR) = unique_id
        )
        SELECT unique_id, true_road
        FROM (
            SELECT *
            FROM truth_rows
            QUALIFY row_number() OVER (
                PARTITION BY unique_id
                ORDER BY CASE cohort WHEN 'built' THEN 1 ELSE 2 END, street_raw
            ) = 1
        ) AS preferred_truth
        WHERE candidate_status = 'eligible' AND street_raw IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE TEMPORARY TABLE audit_ranked AS
        SELECT
            scores.*,
            truth.true_road,
            row_number() OVER (
                PARTITION BY scores.address_id
                ORDER BY ranker_logit DESC, candidate_phrase,
                    candidate_start_position
            ) AS candidate_rank,
            lead(ranker_logit) OVER (
                PARTITION BY scores.address_id
                ORDER BY ranker_logit DESC, candidate_phrase,
                    candidate_start_position
            ) AS runner_up_logit,
            candidate_phrase = truth.true_road AS candidate_exact,
            candidate_phrase = truth.true_road
                OR starts_with(truth.true_road, candidate_phrase || ' ')
                AS candidate_core
        FROM audit_scores AS scores
        LEFT JOIN audit_truth AS truth ON scores.address_id = truth.unique_id
        """
    )
    con.execute(
        """
        CREATE TEMPORARY TABLE audit_distinct_ranked AS
        WITH deduplicated_phrases AS (
            SELECT *
            FROM audit_ranked
            QUALIFY row_number() OVER (
                PARTITION BY address_id, candidate_phrase
                ORDER BY candidate_rank
            ) = 1
        )
        SELECT
            *,
            row_number() OVER (
                PARTITION BY address_id ORDER BY candidate_rank
            ) AS distinct_candidate_rank
        FROM deduplicated_phrases
        """
    )
    con.execute(
        """
        CREATE TEMPORARY TABLE audit_diverse_ranked AS
        WITH diverse AS (
            SELECT candidate.*
                        FROM audit_distinct_ranked AS candidate
            WHERE NOT EXISTS (
                SELECT 1
                                FROM audit_distinct_ranked AS higher
                WHERE higher.address_id = candidate.address_id
                                    AND higher.distinct_candidate_rank
                                            < candidate.distinct_candidate_rank
                  AND (
                      contains(
                          ' ' || higher.candidate_phrase || ' ',
                          ' ' || candidate.candidate_phrase || ' '
                      )
                      OR contains(
                          ' ' || candidate.candidate_phrase || ' ',
                          ' ' || higher.candidate_phrase || ' '
                      )
                  )
            )
        )
        SELECT
            *,
            row_number() OVER (
                PARTITION BY address_id ORDER BY candidate_rank
            ) AS diverse_rank
        FROM diverse
        """
    )
    summary = con.execute(
        """
        WITH raw_hits AS (
            SELECT
                address_id,
                max((candidate_rank = 1 AND candidate_exact)::INT) AS exact_at_1,
                max((distinct_candidate_rank <= 2 AND candidate_exact)::INT)
                    AS exact_at_2,
                max(candidate_exact::INT) AS exact_reachable,
                max((candidate_rank = 1 AND candidate_core)::INT) AS core_at_1,
                max((distinct_candidate_rank <= 2 AND candidate_core)::INT)
                    AS core_at_2,
                max(candidate_core::INT) AS core_reachable,
                max((distinct_candidate_rank = 2 AND candidate_exact)::INT)
                    AS exact_only_at_2
            FROM audit_distinct_ranked
            GROUP BY address_id
        ), diverse_hits AS (
            SELECT
                address_id,
                max((diverse_rank <= 2 AND candidate_exact)::INT)
                    AS diverse_exact_at_2,
                max((diverse_rank <= 2 AND candidate_core)::INT)
                    AS diverse_core_at_2
            FROM audit_diverse_ranked
            GROUP BY address_id
        )
        SELECT
            count(*) AS truth_rows,
            count(raw_hits.address_id) AS candidate_bearing_rows,
            (SELECT count(*) FROM audit_distinct_ranked
                WHERE distinct_candidate_rank <= 2)
                AS emitted_top_2_keys,
            sum(exact_at_1) AS exact_at_1,
            sum(exact_at_2) AS exact_at_2,
            sum(exact_reachable) AS exact_reachable,
            sum(core_at_1) AS core_at_1,
            sum(core_at_2) AS core_at_2,
            sum(core_reachable) AS core_reachable,
            sum(exact_only_at_2) AS exact_only_at_2,
            sum(diverse_exact_at_2) AS diverse_exact_at_2,
            sum(diverse_core_at_2) AS diverse_core_at_2
        FROM audit_truth AS truth
        LEFT JOIN raw_hits ON truth.unique_id = raw_hits.address_id
        LEFT JOIN diverse_hits ON truth.unique_id = diverse_hits.address_id
        """
    ).fetchone()
    columns = [description[0] for description in con.description]
    metrics = dict(zip(columns, summary, strict=True))
    truth_rows = int(metrics["truth_rows"])
    candidate_bearing_rows = int(metrics["candidate_bearing_rows"])
    for name in (
        "exact_at_1",
        "exact_at_2",
        "exact_reachable",
        "core_at_1",
        "core_at_2",
        "core_reachable",
        "diverse_exact_at_2",
        "diverse_core_at_2",
    ):
        metrics[f"{name}_rate"] = int(metrics[name] or 0) / truth_rows
        metrics[f"{name}_precision"] = int(metrics[name] or 0) / candidate_bearing_rows
    metrics["exact_top_2_key_precision"] = int(metrics["exact_at_2"] or 0) / int(
        metrics["emitted_top_2_keys"]
    )
    metrics["core_top_2_key_precision"] = int(metrics["core_at_2"] or 0) / int(
        metrics["emitted_top_2_keys"]
    )

    operating_points: dict[str, dict[str, float | int] | None] = {}
    for metric_name, correct_column in (
        ("exact", "candidate_exact"),
        ("core", "candidate_core"),
    ):
        for gate_name, gate_expression in (
            ("score", "ranker_logit"),
            (
                "margin",
                "coalesce(ranker_logit - runner_up_logit, ranker_logit)",
            ),
        ):
            row = con.execute(
                f"""
                WITH threshold_groups AS (
                    SELECT
                        {gate_expression} AS threshold,
                        count(*) AS threshold_rows,
                        sum({correct_column}::INT) AS threshold_correct
                    FROM audit_ranked
                    WHERE candidate_rank = 1 AND true_road IS NOT NULL
                    GROUP BY threshold
                ), curve AS (
                    SELECT
                        threshold,
                        sum(threshold_rows) OVER (
                            ORDER BY threshold DESC
                        ) AS accepted_rows,
                        sum(threshold_correct) OVER (
                            ORDER BY threshold DESC
                        ) AS correct_rows
                    FROM threshold_groups
                )
                SELECT
                    threshold,
                    accepted_rows,
                    correct_rows,
                    correct_rows::DOUBLE / accepted_rows AS precision,
                    correct_rows::DOUBLE / {truth_rows} AS recall
                FROM curve
                WHERE correct_rows::DOUBLE / accepted_rows >= 0.95
                ORDER BY accepted_rows DESC
                LIMIT 1
                """
            ).fetchone()
            operating_points[f"{metric_name}_{gate_name}_at_95_precision"] = (
                None
                if row is None
                else dict(
                    zip(
                        (
                            "threshold",
                            "accepted_rows",
                            "correct_rows",
                            "precision",
                            "recall",
                        ),
                        row,
                        strict=True,
                    )
                )
            )

    con.execute(
        """
        CREATE TEMPORARY TABLE audit_top_two_by_address AS
        SELECT
            address_id,
            max(candidate_exact::INT) FILTER (WHERE distinct_candidate_rank = 1)
                AS exact_at_1,
            max(candidate_exact::INT) FILTER (WHERE distinct_candidate_rank = 2)
                AS exact_at_2_only,
            max(candidate_core::INT) FILTER (WHERE distinct_candidate_rank = 1)
                AS core_at_1,
            max(candidate_core::INT) FILTER (WHERE distinct_candidate_rank = 2)
                AS core_at_2_only,
            max(ranker_logit) FILTER (WHERE distinct_candidate_rank = 1)
                AS top_score,
            max(ranker_logit) FILTER (WHERE distinct_candidate_rank = 2)
                AS second_score,
            max(candidate_phrase) FILTER (WHERE distinct_candidate_rank = 1)
                AS top_phrase,
            max(candidate_phrase) FILTER (WHERE distinct_candidate_rank = 2)
                AS second_phrase
        FROM audit_distinct_ranked
        WHERE true_road IS NOT NULL
        GROUP BY address_id
        """
    )
    nested_policy = con.execute(
        """
        WITH policies AS (
            SELECT
                *,
                second_phrase IS NOT NULL AND (
                    contains(' ' || top_phrase || ' ', ' ' || second_phrase || ' ')
                    OR contains(' ' || second_phrase || ' ', ' ' || top_phrase || ' ')
                ) AS emit_second
            FROM audit_top_two_by_address
        )
        SELECT
            count(*) FILTER (WHERE emit_second) AS extra_keys,
            sum((exact_at_1 = 1 OR (emit_second AND exact_at_2_only = 1))::INT)
                AS exact_hits,
            sum((core_at_1 = 1 OR (emit_second AND core_at_2_only = 1))::INT)
                AS core_hits
        FROM policies
        """
    ).fetchone()
    adaptive_top_2: dict[str, object] = {
        "nested_second": {
            "extra_keys": int(nested_policy[0]),
            "exact_precision": int(nested_policy[1]) / candidate_bearing_rows,
            "core_precision": int(nested_policy[2]) / candidate_bearing_rows,
        }
    }
    for metric_name in ("exact", "core"):
        baseline_correct = int(metrics[f"{metric_name}_at_1"])
        row = con.execute(
            f"""
            WITH margin_groups AS (
                SELECT
                    top_score - second_score AS threshold,
                    count(*) AS extra_keys,
                    sum((
                        {metric_name}_at_1 = 0
                        AND {metric_name}_at_2_only = 1
                    )::INT) AS additional_hits
                FROM audit_top_two_by_address
                WHERE second_score IS NOT NULL
                GROUP BY threshold
            ), curve AS (
                SELECT
                    threshold,
                    sum(extra_keys) OVER (ORDER BY threshold) AS extra_keys,
                    sum(additional_hits) OVER (ORDER BY threshold) AS additional_hits
                FROM margin_groups
            )
            SELECT
                threshold,
                extra_keys,
                additional_hits,
                ({baseline_correct} + additional_hits)::DOUBLE
                    / {candidate_bearing_rows} AS precision
            FROM curve
            WHERE ({baseline_correct} + additional_hits)::DOUBLE
                / {candidate_bearing_rows} >= 0.95
            ORDER BY extra_keys
            LIMIT 1
            """
        ).fetchone()
        adaptive_top_2[f"{metric_name}_margin_at_95_precision"] = (
            None
            if row is None
            else dict(
                zip(
                    ("maximum_margin", "extra_keys", "additional_hits", "precision"),
                    row,
                    strict=True,
                )
            )
        )
    calibrated_threshold = con.execute(
        """
        WITH calibration AS (
            SELECT *
            FROM audit_top_two_by_address
            WHERE hash(address_id || ':adaptive-top-2') % 2 = 0
        ), baseline AS (
            SELECT count(*) AS addresses, sum(core_at_1) AS correct
            FROM calibration
        ), margin_groups AS (
            SELECT
                top_score - second_score AS threshold,
                count(*) AS extra_keys,
                sum((core_at_1 = 0 AND core_at_2_only = 1)::INT)
                    AS additional_hits
            FROM calibration
            WHERE second_score IS NOT NULL
            GROUP BY threshold
        ), curve AS (
            SELECT
                threshold,
                sum(extra_keys) OVER (ORDER BY threshold) AS extra_keys,
                sum(additional_hits) OVER (ORDER BY threshold) AS additional_hits
            FROM margin_groups
        )
        SELECT threshold
        FROM curve
        CROSS JOIN baseline
        WHERE (correct + additional_hits)::DOUBLE / addresses >= 0.95
        ORDER BY extra_keys
        LIMIT 1
        """
    ).fetchone()
    if calibrated_threshold is not None:
        threshold = float(calibrated_threshold[0])
        held_out = con.execute(
            f"""
            WITH test AS (
                SELECT
                    *,
                    second_score IS NOT NULL
                        AND top_score - second_score <= {threshold!r}
                        AS emit_second
                FROM audit_top_two_by_address
                WHERE hash(address_id || ':adaptive-top-2') % 2 = 1
            )
            SELECT
                count(*) AS addresses,
                count(*) FILTER (WHERE emit_second) AS extra_keys,
                sum((core_at_1 = 1 OR (emit_second AND core_at_2_only = 1))::INT)
                    AS core_hits,
                sum((exact_at_1 = 1 OR (emit_second AND exact_at_2_only = 1))::INT)
                    AS exact_hits
            FROM test
            """
        ).fetchone()
        adaptive_top_2["calibrated_core_margin_held_out"] = {
            "maximum_margin": threshold,
            "addresses": int(held_out[0]),
            "extra_keys": int(held_out[1]),
            "core_precision": int(held_out[2]) / int(held_out[0]),
            "exact_precision": int(held_out[3]) / int(held_out[0]),
        }

    nested_errors = con.execute(
        """
        SELECT
            CASE
                WHEN starts_with(true_road, candidate_phrase || ' ')
                    THEN 'prediction_is_truth_prefix'
                WHEN starts_with(candidate_phrase, true_road || ' ')
                    THEN 'truth_is_prediction_prefix'
                WHEN contains(' ' || true_road || ' ', ' ' || candidate_phrase || ' ')
                    THEN 'prediction_inside_truth'
                WHEN contains(' ' || candidate_phrase || ' ', ' ' || true_road || ' ')
                    THEN 'truth_inside_prediction'
                ELSE 'non_nested'
            END AS error_type,
            count(*) AS rows
        FROM audit_ranked
        WHERE candidate_rank = 1
          AND true_road IS NOT NULL
          AND NOT candidate_exact
        GROUP BY error_type
        ORDER BY rows DESC
        """
    ).fetchall()
    unreachable_by_width = con.execute(
        """
        WITH reachability AS (
            SELECT
                address_id,
                max(candidate_exact::INT) AS exact_reachable
            FROM audit_distinct_ranked
            GROUP BY address_id
        )
        SELECT
            array_length(string_split(truth.true_road, ' ')) AS truth_token_count,
            count(*) AS truth_rows,
            count(*) FILTER (
                WHERE coalesce(reachability.exact_reachable, 0) = 0
            ) AS exact_unreachable_rows
        FROM audit_truth AS truth
        LEFT JOIN reachability ON truth.unique_id = reachability.address_id
        GROUP BY truth_token_count
        ORDER BY truth_token_count
        """
    ).fetchall()
    con.close()
    return {
        "modulus": modulus,
        "remainder": remainder,
        "metrics": metrics,
        "adaptive_top_2": adaptive_top_2,
        "operating_points": operating_points,
        "top_1_error_types": dict(nested_errors),
        "truth_width_reachability": {
            str(token_count): {
                "truth_rows": int(width_truth_rows),
                "exact_unreachable_rows": int(unreachable_rows),
            }
            for token_count, width_truth_rows, unreachable_rows in unreachable_by_width
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(
            "benchmarking/results/road_scoring_experiment/"
            "canonical_with_inferred_road/ukam_canonical_addresses.parquet"
        ),
    )
    parser.add_argument(
        "--truth-source",
        type=Path,
        default=Path(
            "../data/analysis_cache/canonical_residential_commercial_addresses.parquet"
        ),
    )
    parser.add_argument("--modulus", type=int, default=20)
    parser.add_argument("--remainder", type=int, default=0)
    parser.add_argument("--threads", type=int, default=14)
    args = parser.parse_args()
    print(
        json.dumps(
            run_audit(
                source=args.source,
                truth_source=args.truth_source,
                modulus=args.modulus,
                remainder=args.remainder,
                threads=args.threads,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
