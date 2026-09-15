"""All-sector road candidate generation and held-out ranking experiment."""

# ruff: noqa: E501, T201

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

from uk_address_matcher.cleaning.steps.road_resources import (
    facility_clause_removal_sql,
    sql_text as _sql_text,
    suffix_peel_regex_sql_literal,
    token_pattern as _token_pattern,
    token_policy as _policy,
)

from .hist_gradient_boosting_json import export_hist_gradient_boosting_json

FEATURE_COLUMNS = (
    "start_tail_fraction",
    "end_tail_fraction",
    "width_tail_fraction",
    "candidate_width",
    "tail_length",
    "ends_at_tail",
    "road_syntax_terminal",
    "contains_residence_token",
    "contains_business_token",
    "log_phrase_support",
    "number_diversity_ratio",
    "log_postcode_support",
    "district_count",
    "log_terminal_support",
    "terminal_right_context_diversity",
)
SPLITS = ("train", "calibration", "test")
TRIAL_CONFIGS = (
    ("compact", {"max_leaf_nodes": 15, "learning_rate": 0.10, "l2_regularization": 3.0}),
    ("balanced", {"max_leaf_nodes": 31, "learning_rate": 0.08, "l2_regularization": 2.0}),
    ("wide", {"max_leaf_nodes": 63, "learning_rate": 0.06, "l2_regularization": 4.0}),
)
SCORING_VECTORS_PER_BATCH = 128
SCORING_SOURCE_ROWS_PER_CHUNK = 1_000_000


@dataclass(frozen=True)
class StageMetric:
    stage: str
    rows: int
    wall_seconds: float


def _progress(message: str, progress_log: Path | None = None) -> None:
    line = f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}"
    print(line, flush=True)
    if progress_log is not None:
        with progress_log.open("a", encoding="utf-8") as stream:
            stream.write(f"{line}\n")


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def _count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _record(
    stages: list[StageMetric],
    name: str,
    started: float,
    table_name: str,
    con: duckdb.DuckDBPyConnection,
    progress_log: Path | None = None,
) -> None:
    metric = StageMetric(name, _count(con, table_name), time.perf_counter() - started)
    stages.append(metric)
    _progress(
        f"completed {metric.stage}: {metric.rows:,} rows in {metric.wall_seconds:.1f}s",
        progress_log,
    )


def _configure_connection(
    con: duckdb.DuckDBPyConnection,
    *,
    threads: int,
    memory_limit: str,
    temp_dir: Path,
) -> None:
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET threads TO {int(threads)}")
    con.execute(f"SET memory_limit TO {_sql_text(memory_limit)}")
    con.execute("SET preserve_insertion_order TO false")
    con.execute(f"SET temp_directory TO {_sql_text(str(temp_dir.resolve()))}")


def _prepare_addresses(
    con: duckdb.DuckDBPyConnection,
    *,
    cache_path: Path,
    row_limit: int | None,
) -> None:
    if _table_exists(con, "all_sector_prepared_addresses"):
        return
    pattern = suffix_peel_regex_sql_literal()
    cache_sql = _sql_text(str(cache_path.expanduser().resolve()))
    limit_sql = f"LIMIT {int(row_limit)}" if row_limit is not None else ""
    con.execute(
        f"""
        CREATE TABLE all_sector_prepared_addresses AS
        WITH source_rows AS (
            SELECT
                CAST(source_unique_id AS VARCHAR) AS unique_id,
                cohort,
                full_address_raw,
                postcode_raw,
                street_raw AS true_street,
                candidate_status,
                regexp_replace(upper(coalesce(postcode_raw, '')), '[^A-Z0-9]', '', 'g')
                    AS full_postcode
            FROM read_parquet({cache_sql})
            {limit_sql}
        ), preferred AS (
            SELECT *
            FROM source_rows
            QUALIFY row_number() OVER (
                PARTITION BY unique_id
                ORDER BY CASE cohort WHEN 'built' THEN 1 ELSE 2 END, full_address_raw
            ) = 1
        ), normalised AS (
            SELECT
                *,
                regexp_extract(full_postcode, '^([A-Z]{{1,2}}[0-9][A-Z0-9]?)', 1)
                    AS postcode_district,
                NULLIF(trim(regexp_replace(
                    upper(coalesce(postcode_raw, '')), '[^A-Z0-9]+', ' ', 'g'
                )), '') AS postcode_address_form,
                NULLIF(trim(regexp_replace(
                    upper(coalesce(full_address_raw, '')), '[^A-Z0-9]+', ' ', 'g'
                )), '') AS normalised_address,
                NULLIF(trim(regexp_replace(
                    upper(coalesce(true_street, '')), '[^A-Z0-9]+', ' ', 'g'
                )), '') AS true_street_norm
            FROM preferred
        ), without_postcode AS (
            SELECT
                *,
                NULLIF(trim(regexp_replace(
                    normalised_address,
                    '(^| )' || postcode_address_form || '( |$)',
                    ' ',
                    'g'
                )), '') AS address_without_postcode
            FROM normalised
            WHERE postcode_district != ''
        ), peeled AS (
            SELECT
                *,
                {
            facility_clause_removal_sql(
                f"trim(regexp_replace(address_without_postcode, '{pattern}', ''))"
            )
        } AS peeled_address
            FROM without_postcode
            WHERE address_without_postcode IS NOT NULL
        ), tokenised AS (
            SELECT
                *,
                regexp_split_to_array(peeled_address, '\\s+') AS peeled_tokens
            FROM peeled
        ), numbered AS (
            SELECT
                *,
                list_filter(
                    list_transform(peeled_tokens, (token, position) -> CASE
                        WHEN regexp_matches(token, '^[0-9]+[A-Z]?$|^[0-9]+-[0-9]+[A-Z]?$')
                            THEN position
                    END),
                    position -> position IS NOT NULL
                ) AS numeric_positions
            FROM tokenised
        )
        SELECT
            *,
            list_extract(numeric_positions, -1) AS rightmost_numeric_position,
            list_extract(peeled_tokens, list_extract(numeric_positions, -1))
                AS rightmost_numeric_value,
            CASE
                WHEN hash(unique_id) % 10 BETWEEN 0 AND 7 THEN 'train'
                WHEN hash(unique_id) % 10 = 8 THEN 'calibration'
                ELSE 'test'
            END AS experiment_split
        FROM numbered
        """
    )


def _create_candidates(con: duckdb.DuckDBPyConnection) -> None:
    if _table_exists(con, "all_sector_road_candidates"):
        return
    policy = _policy()
    residence_pattern = _sql_text(
        _token_pattern(tuple(policy["residence_or_non_road_any_token"]))
    )
    road_pattern = _sql_text(_token_pattern(tuple(policy["road_syntax_terminal_tokens"])))
    business_pattern = _sql_text("(^| )(BUSINESS|ESTATE|PARK)( |$)")
    con.execute(
        f"""
        CREATE TABLE all_sector_road_candidates AS
        WITH generated AS (
            SELECT
                prepared.unique_id,
                prepared.experiment_split,
                prepared.postcode_district,
                prepared.full_postcode,
                prepared.candidate_status,
                prepared.rightmost_numeric_value,
                prepared.true_street_norm,
                prepared.rightmost_numeric_position AS numeric_anchor,
                len(prepared.peeled_tokens) - prepared.rightmost_numeric_position AS tail_length,
                starts.start_position AS candidate_start_position,
                widths.width AS candidate_width,
                starts.start_position + widths.width - 1 AS candidate_end_position,
                array_to_string(
                    list_slice(
                        prepared.peeled_tokens,
                        starts.start_position,
                        starts.start_position + widths.width - 1
                    ),
                    ' '
                ) AS candidate_phrase,
                list_extract(
                    prepared.peeled_tokens,
                    starts.start_position + widths.width - 1
                ) AS terminal_token
            FROM all_sector_prepared_addresses AS prepared
            CROSS JOIN range(
                prepared.rightmost_numeric_position + 1,
                len(prepared.peeled_tokens) + 1
            ) starts(start_position)
            CROSS JOIN (VALUES (2), (3)) widths(width)
            WHERE prepared.rightmost_numeric_position IS NOT NULL
              AND starts.start_position + widths.width - 1 <= len(prepared.peeled_tokens)
        ), labelled AS (
            SELECT
                *,
                CASE
                    WHEN coalesce(candidate_status, '') != 'eligible'
                        OR true_street_norm IS NULL THEN NULL
                    WHEN candidate_phrase = true_street_norm THEN 1
                    WHEN starts_with(true_street_norm, candidate_phrase || ' ') THEN 1
                    ELSE 0
                END AS raw_candidate_label
            FROM generated
        ), address_targets AS (
            SELECT
                unique_id,
                max(coalesce(raw_candidate_label, 0)) = 1 AS target_eligible
            FROM labelled
            GROUP BY unique_id
        )
        SELECT
            labelled.* EXCLUDE (raw_candidate_label),
            address_targets.target_eligible,
            CASE
                WHEN address_targets.target_eligible THEN labelled.raw_candidate_label
            END AS candidate_label,
            address_targets.target_eligible
                AND labelled.candidate_phrase = labelled.true_street_norm AS candidate_exact,
            regexp_matches(labelled.candidate_phrase, {residence_pattern}) AS contains_residence_token,
            regexp_matches(labelled.candidate_phrase, {business_pattern}) AS contains_business_token,
            regexp_matches(labelled.terminal_token, {road_pattern}) AS road_syntax_terminal,
            (labelled.candidate_start_position - labelled.numeric_anchor)::DOUBLE
                / greatest(labelled.tail_length, 1) AS start_tail_fraction,
            (labelled.candidate_end_position - labelled.numeric_anchor)::DOUBLE
                / greatest(labelled.tail_length, 1) AS end_tail_fraction,
            labelled.candidate_width::DOUBLE / greatest(labelled.tail_length, 1)
                AS width_tail_fraction
        FROM labelled
        JOIN address_targets USING (unique_id)
        """
    )


def _create_topology(con: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(con, "all_sector_phrase_topology"):
        con.execute(
            """
            CREATE TABLE all_sector_phrase_topology AS
            SELECT
                candidate_phrase,
                COUNT(*) AS phrase_support,
                COUNT(DISTINCT unique_id) AS phrase_uprns,
                approx_count_distinct(rightmost_numeric_value) AS distinct_numbers,
                approx_count_distinct(full_postcode) AS distinct_postcodes,
                approx_count_distinct(postcode_district) AS distinct_districts
            FROM all_sector_road_candidates
            WHERE experiment_split = 'train'
            GROUP BY candidate_phrase
            """
        )
    if not _table_exists(con, "all_sector_terminal_topology"):
        con.execute(
            """
            CREATE TABLE all_sector_terminal_topology AS
            SELECT
                terminal_token,
                COUNT(*) AS terminal_support,
                approx_count_distinct(candidate_phrase) AS distinct_phrases
            FROM all_sector_road_candidates
            WHERE experiment_split = 'train'
            GROUP BY terminal_token
            """
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW all_sector_candidate_features AS
        SELECT
            candidates.*,
            candidates.rowid AS candidate_row_id,
            CAST(candidates.candidate_end_position = candidates.numeric_anchor + candidates.tail_length AS DOUBLE)
                AS ends_at_tail,
            ln(1 + coalesce(phrases.phrase_support, 0)) AS log_phrase_support,
            coalesce(phrases.distinct_numbers, 0)::DOUBLE
                / greatest(coalesce(phrases.phrase_uprns, 0), 1) AS number_diversity_ratio,
            ln(1 + coalesce(phrases.distinct_postcodes, 0)) AS log_postcode_support,
            coalesce(phrases.distinct_districts, 0)::DOUBLE AS district_count,
            ln(1 + coalesce(terminals.terminal_support, 0)) AS log_terminal_support,
            coalesce(terminals.distinct_phrases, 0)::DOUBLE AS terminal_right_context_diversity
        FROM all_sector_road_candidates AS candidates
        LEFT JOIN all_sector_phrase_topology AS phrases USING (candidate_phrase)
        LEFT JOIN all_sector_terminal_topology AS terminals USING (terminal_token)
        """
    )


def _feature_sql(where_clause: str) -> str:
    feature_sql = ",\n                ".join(FEATURE_COLUMNS)
    return f"""
        SELECT
            unique_id,
            candidate_phrase,
            candidate_start_position,
            candidate_end_position,
            candidate_row_id,
            candidate_label,
            candidate_exact,
            target_eligible,
            {feature_sql}
        FROM all_sector_candidate_features
        WHERE {where_clause}
    """


def _sample_training_frame(
    con: duckdb.DuckDBPyConnection,
    max_per_class: int,
) -> tuple[pd.DataFrame, dict[str, int]]:
    counts = con.execute(
        """
        SELECT candidate_label, COUNT(*) AS rows
        FROM all_sector_candidate_features
        WHERE experiment_split = 'train'
          AND target_eligible
          AND candidate_label IN (0, 1)
        GROUP BY candidate_label
        """
    ).fetchall()
    count_by_label = {int(label): int(rows) for label, rows in counts}
    if min(count_by_label.get(0, 0), count_by_label.get(1, 0)) == 0:
        raise ValueError(
            "Training requires both positive and negative reachable candidates"
        )
    modulus = 1_000_000
    positive_cutoff = min(modulus, int(modulus * max_per_class / count_by_label[1]))
    negative_cutoff = min(modulus, int(modulus * max_per_class / count_by_label[0]))
    frame = con.execute(
        _feature_sql(
            f"""
            experiment_split = 'train'
            AND target_eligible
            AND (
                (candidate_label = 1 AND hash(unique_id || candidate_phrase) % {modulus} < {positive_cutoff})
                OR (candidate_label = 0 AND hash(unique_id || candidate_phrase) % {modulus} < {negative_cutoff})
            )
            """
        )
    ).df()
    return frame, {
        "positive_population": count_by_label[1],
        "negative_population": count_by_label[0],
        "positive_sample": int((frame["candidate_label"] == 1).sum()),
        "negative_sample": int((frame["candidate_label"] == 0).sum()),
    }


def _fit_model(
    frame: pd.DataFrame, config: dict[str, float | int]
) -> HistGradientBoostingClassifier:
    model = HistGradientBoostingClassifier(
        max_iter=250,
        early_stopping=True,
        validation_fraction=0.1,
        random_state=20260826,
        **config,
    )
    model.fit(
        frame.loc[:, FEATURE_COLUMNS].to_numpy(dtype=np.float32), frame["candidate_label"]
    )
    return model


def _score_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    model: HistGradientBoostingClassifier,
    where_clause: str,
    output_table: str,
    progress_log: Path | None = None,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS all_sector_scoring_chunks (
            output_table VARCHAR,
            source_start BIGINT,
            source_end BIGINT,
            scored_rows BIGINT,
            completed_at TIMESTAMP
        )
        """
    )
    has_candidate_row_id = bool(
        con.execute(
            """
            SELECT COUNT(*)
            FROM duckdb_columns()
            WHERE table_name = ? AND column_name = 'candidate_row_id'
            """,
            [output_table],
        ).fetchone()[0]
    )
    if _table_exists(con, output_table) and not has_candidate_row_id:
        _progress(
            f"{output_table}: rebuilding pre-checkpoint partial score table", progress_log
        )
        con.execute(f"DROP TABLE {output_table}")
        con.execute(
            "DELETE FROM all_sector_scoring_chunks WHERE output_table = ?", [output_table]
        )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            unique_id VARCHAR,
            candidate_phrase VARCHAR,
            candidate_start_position INTEGER,
            candidate_end_position INTEGER,
            candidate_row_id BIGINT,
            candidate_label INTEGER,
            candidate_exact BOOLEAN,
            target_eligible BOOLEAN,
            probability DOUBLE
        )
        """
    )
    con.execute(
        f"ALTER TABLE {output_table} ADD COLUMN IF NOT EXISTS candidate_row_id BIGINT"
    )
    max_candidate_row_id = int(
        con.execute("SELECT max(rowid) FROM all_sector_road_candidates").fetchone()[0]
    )
    chunk_count = (
        max_candidate_row_id + SCORING_SOURCE_ROWS_PER_CHUNK
    ) // SCORING_SOURCE_ROWS_PER_CHUNK
    _progress(
        f"{output_table}: scoring {chunk_count:,} contiguous candidate chunks "
        f"of {SCORING_SOURCE_ROWS_PER_CHUNK:,} source rows",
        progress_log,
    )
    scored_rows = 0
    started = time.perf_counter()
    for chunk_index, source_start in enumerate(
        range(0, max_candidate_row_id + 1, SCORING_SOURCE_ROWS_PER_CHUNK), start=1
    ):
        source_end = source_start + SCORING_SOURCE_ROWS_PER_CHUNK
        chunk_complete = con.execute(
            """
            SELECT COUNT(*)
            FROM all_sector_scoring_chunks
            WHERE output_table = ? AND source_start = ? AND source_end = ?
            """,
            [output_table, source_start, source_end],
        ).fetchone()[0]
        if chunk_complete:
            continue
        con.execute(
            f"""
            DELETE FROM {output_table}
            WHERE candidate_row_id >= ? AND candidate_row_id < ?
            """,
            [source_start, source_end],
        )
        cursor = con.cursor()
        cursor.execute(
            _feature_sql(
                f"""({where_clause})
                AND candidate_row_id >= {source_start}
                AND candidate_row_id < {source_end}"""
            )
        )
        chunk_rows = 0
        while True:
            batch = cursor.fetch_df_chunk(SCORING_VECTORS_PER_BATCH)
            if batch.empty:
                break
            scored = batch.loc[
                :,
                [
                    "unique_id",
                    "candidate_phrase",
                    "candidate_start_position",
                    "candidate_end_position",
                    "candidate_row_id",
                    "candidate_label",
                    "candidate_exact",
                    "target_eligible",
                ],
            ].copy()
            scored["probability"] = model.predict_proba(
                batch.loc[:, FEATURE_COLUMNS].to_numpy(dtype=np.float32)
            )[:, 1]
            con.register("all_sector_score_batch", scored)
            con.execute(
                f"""
                INSERT INTO {output_table} (
                    unique_id,
                    candidate_phrase,
                    candidate_start_position,
                    candidate_end_position,
                    candidate_row_id,
                    candidate_label,
                    candidate_exact,
                    target_eligible,
                    probability
                )
                SELECT
                    unique_id,
                    candidate_phrase,
                    candidate_start_position,
                    candidate_end_position,
                    candidate_row_id,
                    candidate_label,
                    candidate_exact,
                    target_eligible,
                    probability
                FROM all_sector_score_batch
                """
            )
            con.unregister("all_sector_score_batch")
            chunk_rows += len(scored)
        cursor.close()
        scored_rows += chunk_rows
        con.execute(
            """
            INSERT INTO all_sector_scoring_chunks
            SELECT ?, ?, ?, ?, current_timestamp
            """,
            [output_table, source_start, source_end, chunk_rows],
        )
        _progress(
            f"{output_table}: chunk {chunk_index:,}/{chunk_count:,}; "
            f"{chunk_rows:,} rows this chunk, {scored_rows:,} total; "
            f"{time.perf_counter() - started:.1f}s elapsed",
            progress_log,
        )


def _create_winners(
    con: duckdb.DuckDBPyConnection, score_table: str, winner_table: str
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {winner_table} AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY unique_id
                    ORDER BY probability DESC, candidate_phrase, candidate_start_position
                ) AS candidate_rank,
                lead(probability) OVER (
                    PARTITION BY unique_id
                    ORDER BY probability DESC, candidate_phrase, candidate_start_position
                ) AS runner_up_probability
            FROM {score_table}
        ), address_truth AS (
            SELECT unique_id, max(target_eligible::INT)::BOOLEAN AS target_eligible
            FROM {score_table}
            GROUP BY unique_id
        )
        SELECT
            truth.unique_id,
            truth.target_eligible,
            ranked.candidate_phrase AS winner_phrase,
            ranked.candidate_label AS winner_label,
            ranked.candidate_exact AS winner_exact,
            ranked.probability AS winner_probability,
            coalesce(ranked.probability - ranked.runner_up_probability, ranked.probability)
                AS winner_margin
        FROM address_truth AS truth
        JOIN ranked
          ON truth.unique_id = ranked.unique_id
         AND ranked.candidate_rank = 1
        """
    )


def _threshold_curve(
    con: duckdb.DuckDBPyConnection, winner_table: str, curve_table: str
) -> pd.DataFrame:
    thresholds = pd.DataFrame(
        {
            "minimum_probability": np.linspace(0.05, 0.95, 91),
            "minimum_margin": np.zeros(91),
        }
    )
    margin_grid = pd.DataFrame(
        {
            "minimum_probability": np.repeat(np.linspace(0.05, 0.95, 91), 5),
            "minimum_margin": np.tile(np.array([0.0, 0.01, 0.03, 0.05, 0.10]), 91),
        }
    )
    thresholds = pd.concat([thresholds, margin_grid], ignore_index=True).drop_duplicates()
    con.register("all_sector_thresholds", thresholds)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {curve_table} AS
        SELECT
            thresholds.minimum_probability,
            thresholds.minimum_margin,
            COUNT(*) FILTER (
                WHERE winners.target_eligible
                  AND winners.winner_probability >= thresholds.minimum_probability
                  AND winners.winner_margin >= thresholds.minimum_margin
                  AND winners.winner_label = 1
            ) AS true_positive,
            COUNT(*) FILTER (
                WHERE winners.target_eligible
                  AND winners.winner_probability >= thresholds.minimum_probability
                  AND winners.winner_margin >= thresholds.minimum_margin
                  AND winners.winner_label = 0
            ) AS false_positive,
            COUNT(*) FILTER (WHERE winners.target_eligible) AS reachable_addresses,
            COUNT(*) FILTER (
                WHERE NOT winners.target_eligible
                  AND winners.winner_probability >= thresholds.minimum_probability
                  AND winners.winner_margin >= thresholds.minimum_margin
            ) AS unreachable_accepted
        FROM {winner_table} AS winners
        CROSS JOIN all_sector_thresholds AS thresholds
        GROUP BY thresholds.minimum_probability, thresholds.minimum_margin
        """
    )
    con.unregister("all_sector_thresholds")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {curve_table}_metrics AS
        SELECT
            *,
            true_positive::DOUBLE / nullif(true_positive + false_positive, 0) AS precision,
            true_positive::DOUBLE / nullif(reachable_addresses, 0) AS recall,
            2.0 * true_positive / nullif(2 * true_positive + false_positive
                + reachable_addresses - true_positive, 0) AS f1,
            unreachable_accepted::DOUBLE
                / nullif(reachable_addresses + unreachable_accepted, 0) AS unreachable_acceptance_rate
        FROM {curve_table}
        """
    )
    return con.execute(
        f"SELECT * FROM {curve_table}_metrics ORDER BY f1 DESC, precision DESC"
    ).df()


def _operating_points(curve: pd.DataFrame) -> pd.DataFrame:
    """Choose calibration-only policies, including the requested precision floors."""
    candidates = curve.dropna(subset=["precision", "recall", "f1"])
    selections: list[tuple[str, pd.DataFrame, list[str]]] = [
        ("max_f1", candidates, ["f1", "precision", "recall"]),
        (
            "best_f1_precision_95",
            candidates.loc[candidates["precision"] >= 0.95],
            ["f1", "recall", "precision"],
        ),
        (
            "max_recall_precision_95",
            candidates.loc[candidates["precision"] >= 0.95],
            ["recall", "f1", "precision"],
        ),
        (
            "best_f1_precision_98",
            candidates.loc[candidates["precision"] >= 0.98],
            ["f1", "recall", "precision"],
        ),
    ]
    chosen: list[pd.Series] = []
    for policy, policy_candidates, sort_columns in selections:
        if policy_candidates.empty:
            continue
        point = (
            policy_candidates.sort_values(sort_columns, ascending=False).iloc[0].copy()
        )
        point["policy"] = policy
        chosen.append(point)
    return pd.DataFrame(chosen).loc[
        :,
        [
            "policy",
            "minimum_probability",
            "minimum_margin",
            "precision",
            "recall",
            "f1",
            "true_positive",
            "false_positive",
            "reachable_addresses",
            "unreachable_accepted",
        ],
    ]


def _evaluation_summary(
    con: duckdb.DuckDBPyConnection,
    *,
    winner_table: str,
    operating_points: pd.DataFrame,
    table_name: str,
) -> None:
    con.register("all_sector_evaluation_operating_points", operating_points)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name}_counts AS
        SELECT
            policies.policy,
            policies.minimum_probability,
            policies.minimum_margin,
            count(*) AS addresses,
            count(*) FILTER (WHERE target_eligible) AS reachable_addresses,
            count(*) FILTER (WHERE NOT target_eligible) AS unreachable_addresses,
            count(*) FILTER (
                WHERE target_eligible AND winner_label = 1
            ) AS top1_correct,
            count(*) FILTER (
                WHERE target_eligible
                  AND winner_probability >= policies.minimum_probability
                  AND winner_margin >= policies.minimum_margin
                  AND winner_label = 1
            ) AS accepted_correct,
            count(*) FILTER (
                WHERE target_eligible
                  AND winner_probability >= policies.minimum_probability
                  AND winner_margin >= policies.minimum_margin
                  AND winner_exact
            ) AS accepted_exact,
            count(*) FILTER (
                WHERE target_eligible
                  AND winner_probability >= policies.minimum_probability
                  AND winner_margin >= policies.minimum_margin
                  AND winner_label = 0
            ) AS accepted_wrong,
            count(*) FILTER (
                WHERE NOT target_eligible
                  AND winner_probability >= policies.minimum_probability
                  AND winner_margin >= policies.minimum_margin
            ) AS unreachable_accepted
        FROM {winner_table} AS winners
        CROSS JOIN all_sector_evaluation_operating_points AS policies
        GROUP BY policies.policy, policies.minimum_probability, policies.minimum_margin
        """
    )
    con.unregister("all_sector_evaluation_operating_points")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            accepted_correct::DOUBLE / nullif(accepted_correct + accepted_wrong, 0) AS precision,
            accepted_correct::DOUBLE / nullif(reachable_addresses, 0) AS recall,
            2.0 * accepted_correct / nullif(
                2 * accepted_correct + accepted_wrong + reachable_addresses - accepted_correct,
                0
            ) AS f1,
            accepted_exact::DOUBLE / nullif(accepted_correct, 0) AS exact_share_of_correct,
            unreachable_accepted::DOUBLE / nullif(unreachable_addresses, 0) AS unreachable_acceptance_rate
        FROM {table_name}_counts
        ORDER BY policy
        """
    )


def _feature_importance(
    con: duckdb.DuckDBPyConnection, model: HistGradientBoostingClassifier
) -> pd.DataFrame:
    frame = con.execute(
        _feature_sql(
            """
            experiment_split = 'calibration'
            AND target_eligible
            AND hash(unique_id || ':importance') % 1_000_000 < 50_000
            """
        )
    ).df()
    if frame.empty or frame["candidate_label"].nunique() < 2:
        return pd.DataFrame(
            {"feature": FEATURE_COLUMNS, "importance_mean": 0.0, "importance_std": 0.0}
        )
    result = permutation_importance(
        model,
        frame.loc[:, FEATURE_COLUMNS].to_numpy(dtype=np.float32),
        frame["candidate_label"],
        n_repeats=1,
        random_state=20260826,
        n_jobs=1,
    )
    return pd.DataFrame(
        {
            "feature": FEATURE_COLUMNS,
            "importance_mean": result.importances_mean,
            "importance_std": result.importances_std,
        }
    ).sort_values("importance_mean", ascending=False)


def _create_test_audit(
    con: duckdb.DuckDBPyConnection,
    *,
    minimum_probability: float,
    minimum_margin: float,
) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE all_sector_test_top_candidates AS
        WITH ranked AS (
            SELECT
                scores.*,
                row_number() OVER (
                    PARTITION BY unique_id
                    ORDER BY probability DESC, candidate_phrase, candidate_start_position
                ) AS candidate_rank,
                lead(probability) OVER (
                    PARTITION BY unique_id
                    ORDER BY probability DESC, candidate_phrase, candidate_start_position
                ) AS runner_up_probability
            FROM all_sector_test_scores AS scores
        )
        SELECT
            ranked.unique_id,
            prepared.postcode_district,
            prepared.candidate_status,
            prepared.full_address_raw,
            prepared.true_street,
            prepared.true_street_norm,
            ranked.candidate_rank,
            ranked.candidate_phrase,
            ranked.candidate_start_position,
            ranked.candidate_end_position,
            ranked.candidate_label,
            ranked.candidate_exact,
            ranked.target_eligible,
            ranked.probability,
            coalesce(ranked.probability - ranked.runner_up_probability, ranked.probability)
                AS candidate_margin,
            features.start_tail_fraction,
            features.end_tail_fraction,
            features.width_tail_fraction,
            features.candidate_width,
            features.tail_length,
            features.ends_at_tail,
            features.road_syntax_terminal,
            features.contains_residence_token,
            features.contains_business_token,
            features.log_phrase_support,
            features.number_diversity_ratio,
            features.log_postcode_support,
            features.district_count,
            features.log_terminal_support,
            features.terminal_right_context_diversity
        FROM ranked
        JOIN all_sector_candidate_features AS features
          ON ranked.unique_id = features.unique_id
         AND ranked.candidate_start_position = features.candidate_start_position
         AND ranked.candidate_end_position = features.candidate_end_position
                JOIN all_sector_prepared_addresses AS prepared
                    ON ranked.unique_id = prepared.unique_id
        WHERE ranked.candidate_rank <= 3
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE all_sector_test_conflicts AS
        SELECT *
        FROM all_sector_test_top_candidates
        WHERE candidate_rank = 1
          AND target_eligible
          AND candidate_label = 0
          AND probability >= ?
                    AND candidate_margin >= ?
        ORDER BY probability DESC
        LIMIT 1000
        """,
        [minimum_probability, minimum_margin],
    )


def _district_evaluation(
    con: duckdb.DuckDBPyConnection,
    *,
    winner_table: str,
    operating_points: pd.DataFrame,
    table_name: str,
) -> None:
    con.register("all_sector_district_operating_points", operating_points)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name}_counts AS
        SELECT
            prepared.postcode_district,
            policies.policy,
            policies.minimum_probability,
            policies.minimum_margin,
            count(*) FILTER (WHERE winners.target_eligible) AS reachable_addresses,
            count(*) FILTER (
                WHERE winners.target_eligible
                  AND winners.winner_probability >= policies.minimum_probability
                  AND winners.winner_margin >= policies.minimum_margin
                  AND winners.winner_label = 1
            ) AS accepted_correct,
            count(*) FILTER (
                WHERE winners.target_eligible
                  AND winners.winner_probability >= policies.minimum_probability
                  AND winners.winner_margin >= policies.minimum_margin
                  AND winners.winner_label = 0
            ) AS accepted_wrong
        FROM {winner_table} AS winners
        JOIN all_sector_prepared_addresses AS prepared USING (unique_id)
        CROSS JOIN all_sector_district_operating_points AS policies
        GROUP BY
            prepared.postcode_district,
            policies.policy,
            policies.minimum_probability,
            policies.minimum_margin
        """
    )
    con.unregister("all_sector_district_operating_points")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            accepted_correct::DOUBLE / nullif(accepted_correct + accepted_wrong, 0) AS precision,
            accepted_correct::DOUBLE / nullif(reachable_addresses, 0) AS recall,
            2.0 * accepted_correct / nullif(
                2 * accepted_correct + accepted_wrong + reachable_addresses - accepted_correct,
                0
            ) AS f1
        FROM {table_name}_counts
        """
    )


def _markdown(frame: pd.DataFrame, limit: int | None = None) -> str:
    if limit is not None:
        frame = frame.head(limit)
    if frame.empty:
        return "_No rows._"
    columns = [str(column) for column in frame.columns]
    rows = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame.itertuples(index=False, name=None):
        values = []
        for value in row:
            if pd.isna(value):
                values.append("")
            elif isinstance(value, float):
                values.append(f"{value:.4f}")
            else:
                values.append(str(value).replace("|", "\\|"))
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join(rows)


def _write_report(
    output_dir: Path,
    *,
    stages: list[StageMetric],
    trial_results: pd.DataFrame,
    calibration_curve: pd.DataFrame,
    calibration_operating_points: pd.DataFrame,
    test_curve: pd.DataFrame,
    evaluation: pd.DataFrame,
    feature_importance: pd.DataFrame,
    sector_count: int,
    selected_threshold: pd.Series,
) -> None:
    output_dir.joinpath("all_sector_road_identifier_experiment.md").write_text(
        f"""# All-Sector Road Identifier Experiment

## Scope

- Outward postcode districts: {sector_count}
- Candidate generator: every width-2 and width-3 span after the rightmost numeric anchor.
- Candidate generation retains records with missing or unreachable oracle streets. Those rows have no supervised road target and are reported separately.
- Split: 80% deterministic UPRN training, 10% calibration, 10% final test.
- Model: histogram gradient-boosted candidate classifier; winner is the highest probability candidate per address.

## Model Selection

{_markdown(trial_results)}

The selected acceptance rule (`{selected_threshold["policy"]}`) comes from calibration only: probability >= {selected_threshold["minimum_probability"]:.4f}, margin >= {selected_threshold["minimum_margin"]:.4f}.

## Calibration Operating Points

{_markdown(calibration_operating_points)}

## Calibration Precision/Recall Curve

{_markdown(calibration_curve, 25)}

## Final-Test Precision/Recall Curve

{_markdown(test_curve, 25)}

## Final-Test Summary

{_markdown(evaluation)}

## Permutation Feature Importance

{_markdown(feature_importance)}

## Runtime

{_markdown(pd.DataFrame(asdict(stage) for stage in stages))}

## Stored Objects

- `all_sector_road_identifier.duckdb`: prepared addresses, every generated roadlike span, topology, scores, winners, and metrics.
- `models/`: selected tree model and machine-readable model shape.
- Parquet tables: outward-district metrics, curves, feature importance, top-three test-candidate audit, and high-confidence test conflicts.

This is an oracle-evaluated experiment. It does not change production matching or its existing candidate selection.
""",
        encoding="utf-8",
    )


def run_all_sector_experiment(
    *,
    cache_path: Path,
    output_dir: Path,
    threads: int = 4,
    memory_limit: str = "16GB",
    row_limit: int | None = None,
    max_training_per_class: int = 2_000_000,
    trial_training_per_class: int = 500_000,
) -> dict[str, Any]:
    """Run the all-sector candidate experiment with resumable DuckDB tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    models_dir = output_dir / "models"
    models_dir.mkdir(exist_ok=True)
    progress_log = output_dir / "run_progress.log"
    _progress("starting all-sector experiment", progress_log)
    database_path = output_dir / "all_sector_road_identifier.duckdb"
    con = duckdb.connect(str(database_path))
    _configure_connection(
        con,
        threads=threads,
        memory_limit=memory_limit,
        temp_dir=output_dir / "duckdb_tmp",
    )
    stages: list[StageMetric] = []

    started = time.perf_counter()
    _prepare_addresses(con, cache_path=cache_path, row_limit=row_limit)
    _record(
        stages,
        "prepared addresses",
        started,
        "all_sector_prepared_addresses",
        con,
        progress_log,
    )
    started = time.perf_counter()
    _create_candidates(con)
    _record(
        stages,
        "generated roadlike candidates",
        started,
        "all_sector_road_candidates",
        con,
        progress_log,
    )
    started = time.perf_counter()
    _create_topology(con)
    _record(
        stages,
        "training topology",
        started,
        "all_sector_phrase_topology",
        con,
        progress_log,
    )

    trial_rows: list[dict[str, Any]] = []
    for trial_name, config in TRIAL_CONFIGS:
        started = time.perf_counter()
        _progress(f"{trial_name} trial: sampling training candidates", progress_log)
        training_frame, sample_counts = _sample_training_frame(
            con, trial_training_per_class
        )
        _progress(
            f"{trial_name} trial: fitting {len(training_frame):,} sampled candidates",
            progress_log,
        )
        model = _fit_model(training_frame, config)
        _progress(f"{trial_name} trial: scoring selection slice", progress_log)
        score_table = f"all_sector_{trial_name}_selection_scores"
        winner_table = f"all_sector_{trial_name}_selection_winners"
        _score_relation(
            con,
            model=model,
            where_clause=(
                "experiment_split = 'calibration' "
                "AND hash(unique_id || ':model_selection') % 20 = 0"
            ),
            output_table=score_table,
            progress_log=progress_log,
        )
        _create_winners(con, score_table, winner_table)
        curve = _threshold_curve(
            con, winner_table, f"all_sector_{trial_name}_selection_curve"
        )
        best = curve.iloc[0]
        trial_rows.append(
            {
                "trial": trial_name,
                **config,
                **sample_counts,
                "selection_precision": float(best["precision"]),
                "selection_recall": float(best["recall"]),
                "selection_f1": float(best["f1"]),
                "selection_probability": float(best["minimum_probability"]),
                "selection_margin": float(best["minimum_margin"]),
                "wall_seconds": time.perf_counter() - started,
            }
        )
    trial_results = pd.DataFrame(trial_rows).sort_values(
        ["selection_f1", "selection_precision"], ascending=False
    )
    con.register("all_sector_model_trials_frame", trial_results)
    con.execute(
        "CREATE OR REPLACE TABLE all_sector_model_trials AS SELECT * FROM all_sector_model_trials_frame"
    )
    con.unregister("all_sector_model_trials_frame")

    selected = trial_results.iloc[0]
    selected_config = next(
        config for name, config in TRIAL_CONFIGS if name == selected["trial"]
    )
    started = time.perf_counter()
    _progress("final model: sampling training candidates", progress_log)
    final_training_frame, final_sample_counts = _sample_training_frame(
        con, max_training_per_class
    )
    _progress(
        f"final model: fitting {len(final_training_frame):,} sampled candidates",
        progress_log,
    )
    final_model = _fit_model(final_training_frame, selected_config)
    joblib.dump(final_model, models_dir / "all_sector_hist_gradient_boosting.joblib")
    model_metadata = {
        "selected_trial": selected["trial"],
        "config": selected_config,
        "feature_columns": FEATURE_COLUMNS,
        "training_sample": final_sample_counts,
    }
    (models_dir / "all_sector_hist_gradient_boosting.json").write_text(
        json.dumps(model_metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    export_hist_gradient_boosting_json(
        final_model,
        models_dir / "_road_ranker_hist_gradient_boosting_v1.json",
        feature_columns=FEATURE_COLUMNS,
        metadata=model_metadata,
    )
    _record(
        stages, "final model fit", started, "all_sector_model_trials", con, progress_log
    )

    started = time.perf_counter()
    _progress("calibration: scoring candidates", progress_log)
    _score_relation(
        con,
        model=final_model,
        where_clause="experiment_split = 'calibration'",
        output_table="all_sector_calibration_scores",
        progress_log=progress_log,
    )
    _create_winners(
        con, "all_sector_calibration_scores", "all_sector_calibration_winners"
    )
    calibration_curve = _threshold_curve(
        con,
        "all_sector_calibration_winners",
        "all_sector_calibration_curve",
    )
    calibration_operating_points = _operating_points(calibration_curve)
    con.register(
        "all_sector_calibration_operating_points_frame", calibration_operating_points
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE all_sector_calibration_operating_points AS
        SELECT * FROM all_sector_calibration_operating_points_frame
        """
    )
    con.unregister("all_sector_calibration_operating_points_frame")
    selected_policy = "best_f1_precision_95"
    if selected_policy not in set(calibration_operating_points["policy"]):
        selected_policy = "max_f1"
    selected_threshold = calibration_operating_points.loc[
        calibration_operating_points["policy"] == selected_policy
    ].iloc[0]
    _record(
        stages,
        "calibration scoring",
        started,
        "all_sector_calibration_scores",
        con,
        progress_log,
    )

    started = time.perf_counter()
    _progress("test: scoring candidates", progress_log)
    _score_relation(
        con,
        model=final_model,
        where_clause="experiment_split = 'test'",
        output_table="all_sector_test_scores",
        progress_log=progress_log,
    )
    _create_winners(con, "all_sector_test_scores", "all_sector_test_winners")
    test_curve = _threshold_curve(con, "all_sector_test_winners", "all_sector_test_curve")
    _evaluation_summary(
        con,
        winner_table="all_sector_test_winners",
        operating_points=calibration_operating_points,
        table_name="all_sector_test_evaluation",
    )
    _district_evaluation(
        con,
        winner_table="all_sector_test_winners",
        operating_points=calibration_operating_points,
        table_name="all_sector_test_district_metrics",
    )
    _create_test_audit(
        con,
        minimum_probability=float(selected_threshold["minimum_probability"]),
        minimum_margin=float(selected_threshold["minimum_margin"]),
    )
    _record(stages, "test scoring", started, "all_sector_test_scores", con, progress_log)

    started = time.perf_counter()
    _progress("computing permutation feature importance", progress_log)
    importance = _feature_importance(con, final_model)
    con.register("all_sector_feature_importance_frame", importance)
    con.execute(
        "CREATE OR REPLACE TABLE all_sector_feature_importance AS SELECT * FROM all_sector_feature_importance_frame"
    )
    con.unregister("all_sector_feature_importance_frame")
    _record(
        stages,
        "feature importance",
        started,
        "all_sector_feature_importance",
        con,
        progress_log,
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE all_sector_generation_metrics AS
        WITH candidate_counts AS (
            SELECT
                unique_id,
                max(target_eligible::INT)::BOOLEAN AS target_eligible,
                count(*) AS candidate_count
            FROM all_sector_road_candidates
            GROUP BY unique_id
        )
        SELECT
            prepared.experiment_split,
            prepared.postcode_district,
            count(*) AS prepared_addresses,
            count(*) FILTER (
                WHERE prepared.rightmost_numeric_position IS NOT NULL
            ) AS numeric_anchor_addresses,
            count(*) FILTER (
                WHERE candidate_counts.candidate_count IS NOT NULL
            ) AS roadlike_candidate_addresses,
            count(*) FILTER (
                WHERE candidate_counts.target_eligible
            ) AS reachable_target_addresses,
            count(*) FILTER (
                WHERE NOT coalesce(candidate_counts.target_eligible, false)
            ) AS unreachable_or_non_target_addresses,
            avg(coalesce(candidate_counts.candidate_count, 0)) AS mean_candidate_count,
            quantile_cont(coalesce(candidate_counts.candidate_count, 0), 0.95)
                AS p95_candidate_count
        FROM all_sector_prepared_addresses AS prepared
        LEFT JOIN candidate_counts USING (unique_id)
        GROUP BY prepared.experiment_split, prepared.postcode_district
        """
    )
    export_tables = (
        "all_sector_model_trials",
        "all_sector_generation_metrics",
        "all_sector_calibration_curve_metrics",
        "all_sector_calibration_operating_points",
        "all_sector_test_curve_metrics",
        "all_sector_test_evaluation",
        "all_sector_test_district_metrics",
        "all_sector_feature_importance",
        "all_sector_test_top_candidates",
        "all_sector_test_conflicts",
    )
    for table_name in export_tables:
        con.execute(
            f"COPY {table_name} TO {_sql_text(str(output_dir / f'{table_name}.parquet'))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    evaluation = con.execute("SELECT * FROM all_sector_test_evaluation").df()
    sector_count = int(
        con.execute(
            "SELECT COUNT(DISTINCT postcode_district) FROM all_sector_prepared_addresses"
        ).fetchone()[0]
    )
    _write_report(
        output_dir,
        stages=stages,
        trial_results=trial_results,
        calibration_curve=calibration_curve,
        calibration_operating_points=calibration_operating_points,
        test_curve=test_curve,
        evaluation=evaluation,
        feature_importance=importance,
        sector_count=sector_count,
        selected_threshold=selected_threshold,
    )
    (output_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "cache_path": str(cache_path.resolve()),
                "database_path": str(database_path.resolve()),
                "policy_resource": "uk_address_matcher.data/road_candidate_token_policy.json",
                "outward_postcode_districts": sector_count,
                "row_limit": row_limit,
                "threads": threads,
                "memory_limit": memory_limit,
                "selected_trial": selected["trial"],
                "selected_threshold": {
                    "policy": selected_threshold["policy"],
                    "minimum_probability": float(
                        selected_threshold["minimum_probability"]
                    ),
                    "minimum_margin": float(selected_threshold["minimum_margin"]),
                },
                "calibration_operating_points": json.loads(
                    calibration_operating_points.to_json(orient="records")
                ),
                "stages": [asdict(stage) for stage in stages],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _progress("all-sector experiment complete", progress_log)
    con.close()
    return {
        "output_dir": output_dir,
        "database_path": database_path,
        "selected_trial": selected["trial"],
        "selected_threshold": selected_threshold.to_dict(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=Path(
            "data/analysis_cache/canonical_residential_commercial_addresses.parquet"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarking/results/all_sector_road_identifier"),
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--row-limit", type=int)
    parser.add_argument("--max-training-per-class", type=int, default=2_000_000)
    parser.add_argument("--trial-training-per-class", type=int, default=500_000)
    args = parser.parse_args(argv)
    run_all_sector_experiment(
        cache_path=args.cache_path,
        output_dir=args.output_dir,
        threads=args.threads,
        memory_limit=args.memory_limit,
        row_limit=args.row_limit,
        max_training_per_class=args.max_training_per_class,
        trial_training_per_class=args.trial_training_per_class,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
