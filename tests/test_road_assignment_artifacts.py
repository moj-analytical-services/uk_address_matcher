from __future__ import annotations

import json

import duckdb

from uk_address_matcher.model_training.additive_pairwise_road_assignment import (
    ADDITIVE_FEATURES,
)
from uk_address_matcher.model_training.linear_road_assignment import FoldedLogisticModel
from uk_address_matcher.model_training.road_assignment_artifacts import (
    DEFAULT_EXCLUDED_CLASSIFICATION_PREFIXES,
    build_catalog,
    canonical_candidate_sql,
    create_candidate_table,
    create_phrase_catalog,
    create_ranker_winners,
    online_feature_sql,
    prepared_candidate_sql,
    score_candidate_relation_sql,
)


def test_prepared_address_candidates_start_after_rightmost_numeric_token() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE canonical AS
        SELECT * FROM (
            VALUES
                (1, 'UNIT 2A B Z BUSINESS PARK INTERNATIONAL VIEW', 'AB10 0BJ', ['2A']),
                (2, '12 GREAT FIELD COLINDALE', 'NW9 5AA', ['12']),
                (3, '12 GREAT FIELD COLINDALE ABERDEEN', 'AB10 1AA', ['12'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
        """
    )

    create_phrase_catalog(con, source_relation="canonical")
    features = con.execute(online_feature_sql("canonical")).fetchdf()

    assert set(features.loc[features["address_id"] == "1", "candidate_phrase"]) == {
        "Z BUSINESS PARK",
        "BUSINESS PARK",
        "PARK INTERNATIONAL VIEW",
        "INTERNATIONAL VIEW",
    }
    assert features["log_phrase_support"].gt(0).all()
    assert (
        not features.loc[features["address_id"] == "3", "candidate_phrase"]
        .str.contains("ABERDEEN")
        .any()
    )


def test_candidate_generation_abstains_for_ambiguous_text_and_excluded_classes() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE canonical AS
        SELECT * FROM (
            VALUES
                (1, '12 HIGH STREET LONDON', 'SW1A 1AA', ['12'], 'RD06'),
                (2, 'CARAVAN 7 RIVERSIDE ROAD LONDON', 'SW1A 1AA', ['7'], 'RD01'),
                (3, 'HOUSEBOAT 4 QUAYSIDE ROAD LONDON', 'SW1A 1AA', ['4'], 'RD06'),
                (4, 'GARAGE 3 MEWS ROAD LONDON', 'SW1A 1AA', ['3'], 'RG01'),
                (5, '14 OFFICE PARK ROAD LONDON', 'SW1A 1AA', ['14'], 'CO01'),
                (6, 'BEACH HUT 15 GOLF COURSE ROAD', 'AB1 2CD', ['15'], 'CL09'),
                (7, 'TENNIS 100M FROM COURT GATE CLOSE', 'AB1 2CD', ['100M'], 'CL06QS'),
                (8, 'REAR OF 12 QUEENS CIRCUS LONDON', 'SW1A 1AA', ['12'], 'CO01'),
                (9, 'UNIT 7 BLOCK F DRAY WALK LONDON', 'SW1A 1AA', ['7'], 'CO01'),
                (10, 'UNIT 8 SHOPPING CENTRE HIGH ROAD', 'SW1A 1AA', ['8'], 'CR08'),
                (11, 'UNIT 9 INDUSTRIAL ESTATE PARK ROAD', 'SW1A 1AA', ['9'], 'CI03'),
                (
                    12,
                    'COSTA 29 SHERWOOD STREET TELFORD '
                    'SHOPPING CENTRE TELFORD',
                    'TF3 4BX', ['29'], 'CR10')
        ) AS rows(
            unique_id,
            clean_full_address,
            postcode,
            numeric_tokens,
            classificationcode
        )
        """
    )

    candidates = con.execute(
        canonical_candidate_sql(
            "canonical",
            classification_code_column="classificationcode",
            excluded_classification_prefixes=DEFAULT_EXCLUDED_CLASSIFICATION_PREFIXES,
        )
    ).fetchdf()

    assert set(candidates["address_id"]) == {"1", "5", "9", "10", "11", "12"}
    assert (
        not candidates["candidate_phrase"]
        .str.contains(
            r"(?:^| )(?:BLOCK|SHOPPING CENTRE|INDUSTRIAL ESTATE|INDUSTRIAL PARK)(?: |$)"
        )
        .any()
    )
    assert set(candidates.loc[candidates["address_id"] == "10", "candidate_phrase"]) == {
        "HIGH ROAD"
    }
    assert set(candidates.loc[candidates["address_id"] == "11", "candidate_phrase"]) == {
        "PARK ROAD"
    }
    assert set(candidates.loc[candidates["address_id"] == "12", "candidate_phrase"]) == {
        "SHERWOOD STREET"
    }


def test_folded_sql_score_uses_materialized_candidates() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE canonical AS
        SELECT * FROM (
            VALUES (1, '12 GREAT FIELD COLINDALE', 'NW9 5AA', ['12'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
        """
    )
    create_candidate_table(con, source_relation="canonical")
    create_phrase_catalog(con, source_relation="canonical")

    scored = score_candidate_relation_sql(
        con,
        model=FoldedLogisticModel(0.0, {"candidate_end_position": 1.0}),
        candidate_relation="road_assignment_candidates",
    )
    create_ranker_winners(
        con,
        score_table="road_assignment_additive_scores",
        winner_table="road_assignment_additive_winners",
        score_column="ranker_logit",
    )

    assert scored == 3
    assert con.execute(
        "SELECT candidate_phrase FROM road_assignment_additive_winners"
    ).fetchone() == ("FIELD COLINDALE",)


def test_prepared_candidates_reuse_tokens_and_keep_facility_fallback() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE prepared AS
        SELECT * FROM (
            VALUES
                (
                    1,
                    '12 GREAT FIELD COLINDALE',
                    'NW9 5AA',
                    'NW9',
                    ['12', 'GREAT', 'FIELD', 'COLINDALE'],
                    1,
                    '12',
                    ['12']
                ),
                (
                    2,
                    'COSTA 29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD',
                    'TF3 4BX',
                    'TF3',
                    ['COSTA', '29', 'SHERWOOD', 'STREET', 'TELFORD', 'SHOPPING',
                     'CENTRE', 'TELFORD'],
                    2,
                    '29',
                    ['29']
                )
        ) AS rows(
            unique_id,
            clean_full_address,
            postcode,
            postcode_district,
            peeled_tokens,
            rightmost_numeric_position,
            rightmost_numeric_value,
            numeric_tokens
        )
        """
    )

    candidates = con.execute(prepared_candidate_sql("prepared")).fetchdf()

    assert set(candidates.loc[candidates["address_id"] == "1", "candidate_phrase"]) == {
        "GREAT FIELD",
        "GREAT FIELD COLINDALE",
        "FIELD COLINDALE",
    }
    assert set(candidates.loc[candidates["address_id"] == "2", "candidate_phrase"]) == {
        "SHERWOOD STREET"
    }


def test_build_catalog_assigns_canonical_rows_and_writes_manifest(tmp_path) -> None:
    canonical_path = tmp_path / "canonical.parquet"
    output_database = tmp_path / "artifacts.duckdb"
    folded_model_path = tmp_path / "ranker.json"
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE canonical AS
        SELECT * FROM (
            VALUES (1, '12 GREAT FIELD COLINDALE', 'NW9 5AA', ['12'], 'RD06')
        ) AS rows(
            unique_id,
            clean_full_address,
            postcode,
            numeric_tokens,
            classificationcode
        )
        """
    )
    con.execute(f"COPY canonical TO '{canonical_path}' (FORMAT PARQUET)")
    con.close()
    feature_columns = [name for name, _ in ADDITIVE_FEATURES]
    folded_model_path.write_text(
        json.dumps(
            {
                "model_type": "additive_pairwise_logistic_candidate_ranker",
                "feature_columns": feature_columns,
                "intercept": 0.0,
                "coefficients": {
                    feature: float(feature == "candidate_end_position")
                    for feature in feature_columns
                },
            }
        ),
        encoding="utf-8",
    )

    build_catalog(
        canonical_path=canonical_path,
        output_database=output_database,
        threads=1,
        explain=False,
        input_path=None,
        folded_ranker_path=folded_model_path,
        assign_canonical=True,
        classification_code_column="classificationcode",
        excluded_classification_prefixes=(),
    )

    con = duckdb.connect(str(output_database), read_only=True)
    assert con.execute(
        "SELECT candidate_phrase FROM road_assignment_canonical_labels"
    ).fetchone() == ("FIELD COLINDALE",)
    assert con.execute(
        "SELECT count(*) FROM road_assignment_phrase_catalog"
    ).fetchone() == (3,)
    assert con.execute(
        "SELECT table_name FROM duckdb_tables() ORDER BY table_name"
    ).fetchall() == [
        ("road_assignment_canonical_labels",),
        ("road_assignment_phrase_catalog",),
    ]
    con.close()
    manifest_path = output_database.with_suffix(".road_assignment_manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["catalog_rows"] == 3
    assert manifest["winner_rows"] == 1
    assert manifest["ranker_type"] == "additive_pairwise_logistic_candidate_ranker"
