import duckdb
import pytest
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.comparison_library import ExactMatch

from uk_address_matcher import AddressMatcher, SplinkStage
from uk_address_matcher.post_linkage.distinguishing_features.postcode_recovery import (
    apply_postcode_recovery,
)
from uk_address_matcher.prepare_canonical import prepare_canonical_folder


def test_road_recovery_scores_only_physical_aliases():
    with duckdb.connect() as con:
        canonical = con.sql("""
            SELECT ukam_address_id, unique_id, postcode, road_1_norm, clean_full_address,
                12 AS numeric_token_1, .02 AS tf_numeric_token_1,
                CASE WHEN ukam_address_id=4 THEN [] ELSE [12] END AS numeric_tokens,
                NULL::VARCHAR AS flat_identity,
                NULL::VARCHAR AS sub_premise_role,
                NULL::VARCHAR AS sub_premise_identifier,
                NULL::VARCHAR AS business_unit_id, ['PONTYPRIDD'] AS unusual_tokens_arr,
                map()::MAP(VARCHAR,BIGINT) AS signature_unique_hits_map
            FROM (VALUES
                (1, 'a', 'CF37 2AA', 'WILLOW ROAD', '12 WILLOW ROAD PONTYPRIDD'),
                (2, 'a', 'CF37 2AA', 'WILLOW ROAD', '12 WILLOW ROAD PONTYPRIDD'),
                (3, 'b', 'CF37 3AA', 'CHESTNUT GARDENS AVENUE',
                    '12 CHESTNUT GARDENS AVENUE PONTYPRIDD'),
                (4, 'c', 'CF37 4AA', NULL, 'PINE VIEW MANOR PONTYPRIDD')
            ) t(ukam_address_id, unique_id, postcode, road_1_norm, clean_full_address)
        """)
        messy = con.sql(f"""
            SELECT c.* REPLACE (id AS ukam_address_id, id::VARCHAR AS unique_id,
                pc AS postcode, road AS road_1_norm, address AS clean_full_address,
                nums AS numeric_tokens)
            FROM ({canonical.sql_query()}) c CROSS JOIN (VALUES
                (101, 'CF37 1ZZ', 'WILOW ROAD', '12 WILOW ROAD PONTYPRIDD', [12]),
                (102, 'CF37 1ZZ', 'CHESTNUT GARDENS',
                    '12 CHESTNUT GARDENS PONTYPRIDD', [12]),
                (103, 'CF38 1ZZ', 'WILOW ROAD', '12 WILOW ROAD PONTYPRIDD', [12]),
                (104, 'CF37 1ZZ', 'WILOW ROAD', '13 WILOW ROAD PONTYPRIDD', [13]),
                (105, 'CF37 1ZZ', 'WILOW ROAD', '12 WILOW ROAD CARDIFF', [12]),
                (106, 'CF37 1ZZ', 'CHESTNUT GARDENS LANE',
                    '12 CHESTNUT GARDENS LANE PONTYPRIDD', [12]),
                (107, 'CF37 1ZZ', NULL, 'PINE VIEW MANOR PONTYPRIDD', [])
            ) q(id,pc,road,address,nums)
            WHERE c.ukam_address_id=1
        """)
        db_api = DuckDBAPI(connection=con)
        linker = Linker(
            [
                db_api.register(canonical, dataset_display_name="c_"),
                db_api.register(messy, dataset_display_name="m_"),
            ],
            SettingsCreator(
                link_type="link_only",
                unique_id_column_name="ukam_address_id",
                blocking_rules_to_generate_predictions=[block_on("numeric_token_1")],
                comparisons=[
                    ExactMatch("postcode").configure(
                        m_probabilities=[0.9, 0.1],
                        u_probabilities=[0.1, 0.9],
                    ),
                    ExactMatch("numeric_token_1").configure(
                        term_frequency_adjustments=True,
                        m_probabilities=[0.9, 0.1],
                        u_probabilities=[0.1, 0.9],
                    ),
                ],
            ),
        )
        tf_lookup = db_api.register(
            canonical.project("numeric_token_1, tf_numeric_token_1").distinct()
        )
        linker.table_management.register_term_frequency_lookup(
            tf_lookup, "numeric_token_1"
        )
        raw_predictions = linker.inference.predict().as_duckdbpyrelation()
        stock = con.sql("""
            SELECT NULL::INTEGER AS ukam_address_id,
                NULL::VARCHAR AS resolved_canonical_id,
                NULL::INTEGER AS canonical_ukam_address_id, NULL::VARCHAR AS match_reason,
                NULL::DOUBLE AS match_weight, NULL::DOUBLE AS distinguishability
            WHERE false
        """)
        best = con.sql("""
            SELECT NULL::INTEGER AS ukam_address_id_l, NULL::INTEGER AS ukam_address_id_r,
                NULL::DOUBLE AS match_weight, NULL::VARCHAR AS postcode_l,
                NULL::VARCHAR AS postcode_r, NULL::INTEGER AS candidate_rank WHERE false
        """)
        owned = []
        decisions = apply_postcode_recovery(
            con,
            linker,
            stock,
            best,
            messy,
            canonical,
            [messy, canonical],
            owned,
        )
        projection = (
            "ukam_address_id, canonical_ukam_address_id, "
            "postcode_recovery_route, distinguishability"
        )
        assert decisions.project(projection).order("ukam_address_id").fetchall() == [
            (101, 1, "spelling", None),
            (102, 3, "stem", None),
        ]
        rescored = con.sql(f"""
            SELECT r.match_weight, p.match_weight
            FROM ({decisions.sql_query()}) r
            JOIN ({raw_predictions.sql_query()}) p
              ON p.ukam_address_id_l=r.canonical_ukam_address_id
              AND p.ukam_address_id_r=r.ukam_address_id
        """).fetchall()
        assert len(rescored) == 2
        for recovery_score, stock_score in rescored:
            assert recovery_score == pytest.approx(stock_score)
        for frame in owned:
            frame.drop_table_from_database_and_remove_from_cache(
                force_non_splink_table=True
            )


def test_postcode_recovery_respects_canonical_filter_and_preserves_input(tmp_path):
    with duckdb.connect() as con:
        canonical_source = con.sql("""
            SELECT * FROM (VALUES
                ('101', '17 Rowan Avenue Llandaff Cardiff', 'CF5 2AA'),
                ('102', '18 Rowan Avenue Llandaff Cardiff', 'CF5 2AA'),
                ('103', '9 Willow Road Canton Cardiff', 'CF5 1AA')
            ) t(unique_id,address_concat,postcode)
        """)
        canonical = tmp_path / "prepared"
        prepare_canonical_folder(canonical_source, output_folder=canonical, con=con)
        messy = con.sql("""
            SELECT * FROM (VALUES
                ('a', '17 Rowan Avenue Llandaff Cardiff', 'CF5 2BB'),
                ('b', '18 Rowan Avenue Llandaff Cardiff', 'CF5 2AA'),
                ('c', '9 Willow Road Canton Cardiff', 'CF5 1AA'),
                ('d', '16 Rowan Avenue Llandaff Cardiff', 'CF5 2AA')
            ) t(unique_id,address_concat,postcode)
        """)

        def winners(chunks: int, canonical_filter: str | None = None, threshold=-20):
            result = AddressMatcher(
                canonical,
                messy,
                con=con,
                matching_num_chunks=chunks,
                stages=[
                    SplinkStage(
                        include_full_postcode_block=True,
                        include_outside_postcode_block=False,
                        final_match_weight_threshold=threshold,
                    )
                ],
                canonical_address_filter=canonical_filter,
            ).match()
            try:
                return (
                    result.matches(all_columns=True)
                    .project(
                        "address_id, canonical_address_id, match_reason, match_weight,"
                        "distinguishability, postcode"
                    )
                    .order("address_id")
                    .fetchall()
                )
            finally:
                result.close()

        enabled = winners(2)
        assert enabled == winners(1)
        assert enabled[0][1] == "101"
        assert enabled[0][-1] == "CF5 2BB"
        assert enabled[1][1] == "102"
        assert winners(1, "unique_id <> '101'")[0][1] is None
        assert winners(1, threshold=100)[0][1] is None
