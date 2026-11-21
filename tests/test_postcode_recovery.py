import duckdb

from uk_address_matcher import AddressMatcher, SplinkStage
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.prepare_canonical import prepare_canonical_folder


def test_wrong_postcode_promotion_respects_road_and_premise_guards():
    with duckdb.connect() as con:
        df_predict = con.sql("""
            SELECT * FROM (VALUES
                (1, 10, 'q1', 1, '12 OAK LANE PONTYPRIDD', 'CF37 2BB',
                 '12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB', 'OAK LANE', 'WILLOW ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 18.0::DOUBLE, 0.99::DOUBLE),
                (2, 20, 'q1', 1, '12 WILLOW ROAD PONTYPRIDD', 'CF37 2AA',
                 '12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB', 'WILLOW ROAD', 'WILLOW ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 11.0::DOUBLE, 0.90::DOUBLE),
                (3, 30, 'q2', 2, '12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB',
                 '12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB', 'WILLOW ROAD', 'WILLOW ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 18.0::DOUBLE, 0.99::DOUBLE),
                (4, 40, 'q2', 2, '12 WILLOW ROAD PONTYPRIDD', 'CF37 2AA',
                 '12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB', 'WILLOW ROAD', 'WILLOW ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 11.0::DOUBLE, 0.90::DOUBLE),
                (5, 50, 'q3', 3, '12 OAK LANE PONTYPRIDD', 'CF37 2BB',
                 'FLAT 1 12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB',
                 'OAK LANE', 'WILLOW ROAD',
                 NULL::VARCHAR, '1', 20.0::DOUBLE, 0.99::DOUBLE),
                (6, 60, 'q3', 3, 'FLAT 2 12 WILLOW ROAD PONTYPRIDD', 'CF37 2AA',
                 'FLAT 1 12 WILLOW ROAD PONTYPRIDD', 'CF37 2BB',
                 'WILLOW ROAD', 'WILLOW ROAD',
                 '2', '1', 1.0::DOUBLE, 0.90::DOUBLE)
            ) t(
                ukam_address_id_l, unique_id_l, unique_id_r, ukam_address_id_r,
                clean_full_address_l, postcode_l, clean_full_address_r, postcode_r,
                road_1_norm_l, road_1_norm_r, flat_identity_l, flat_identity_r,
                match_weight, match_probability
            )
        """)
        improved = improve_predictions_using_distinguishing_tokens(
            df_predict=df_predict,
            con=con,
            match_weight_threshold=-20.0,
        )
        winners = con.sql(f"""
            SELECT unique_id_r, unique_id_l
            FROM ({improved.sql_query()})
            QUALIFY row_number() OVER (
                PARTITION BY unique_id_r ORDER BY match_weight DESC
            ) = 1
            ORDER BY unique_id_r
        """).fetchall()
        # q1 promotes wrong-postcode match; q2 keeps same-road; q3 blocks flat conflict
        assert winners == [("q1", 20), ("q2", 30), ("q3", 50)]


def test_wrong_postcode_recovery_end_to_end(tmp_path):
    with duckdb.connect() as con:
        canonical_source = con.sql("""
            SELECT * FROM (VALUES
                ('101', '17 Rowan Avenue Llandaff Cardiff', 'CF5 2AA'),
                ('102', '18 Rowan Avenue Llandaff Cardiff', 'CF5 2AA'),
                ('103', '9 Willow Road Canton Cardiff', 'CF5 1AA'),
                ('104', '12 Chestnut Gardens Avenue Llandaff Cardiff', 'CF5 3AA')
            ) t(unique_id, address_concat, postcode)
        """)
        canonical = tmp_path / "prepared"
        prepare_canonical_folder(canonical_source, output_folder=canonical, con=con)
        messy = con.sql("""
            SELECT * FROM (VALUES
                ('a', '17 Rowan Avenue Llandaff Cardiff', 'CF5 2BB'),
                ('b', '18 Rowen Avenue Llandaff Cardiff', 'CF5 2BB'),
                ('c', '12 Chestnut Gardens Llandaff Cardiff', 'CF5 2BB'),
                ('d', '9 Willow Road Canton Cardiff', 'CF5 1AA')
            ) t(unique_id, address_concat, postcode)
        """)

        def winners(chunks: int, canonical_filter: str | None = None):
            result = AddressMatcher(
                canonical,
                messy,
                con=con,
                matching_num_chunks=chunks,
                stages=[SplinkStage(final_match_weight_threshold=-20)],
                canonical_address_filter=canonical_filter,
            ).match()
            try:
                return (
                    result.matches(all_columns=True)
                    .project("address_id, canonical_address_id, postcode")
                    .order("address_id")
                    .fetchall()
                )
            finally:
                result.close()

        matched = winners(1)
        assert matched == winners(2)
        assert matched == [
            ("a", "101", "CF5 2BB"),
            ("b", "102", "CF5 2BB"),
            ("c", "104", "CF5 2BB"),
            ("d", "103", "CF5 1AA"),
        ]
        assert winners(1, "unique_id <> '101'")[0][1] is None
