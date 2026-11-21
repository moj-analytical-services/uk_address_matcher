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


def test_wrong_postcode_promotion_active_locality_disagreement_only():
    with duckdb.connect() as con:
        df_predict = con.sql("""
            SELECT * FROM (VALUES
                -- q1: active locality disagreement (NEW BYTH vs CUMINESTOWN), same road,
                -- close gap -> promotes right-locality contender
                (1, 10, 'q1', 1, '34 MAIN STREET CUMINESTOWN TURRIFF', 'AB53 5YJ',
                 '34 MAIN STREET NEW BYTH TURRIFF', 'AB53 5YJ',
                 'MAIN STREET', 'MAIN STREET',
                 NULL::VARCHAR, NULL::VARCHAR, 27.0::DOUBLE, 0.99::DOUBLE),
                (2, 20, 'q1', 1, '34 MAIN STREET NEW BYTH TURRIFF', 'AB53 5XD',
                 '34 MAIN STREET NEW BYTH TURRIFF', 'AB53 5YJ',
                 'MAIN STREET', 'MAIN STREET',
                 NULL::VARCHAR, NULL::VARCHAR, 20.0::DOUBLE, 0.95::DOUBLE),
                -- q2: locality absent on one side (LONDON vs HACKNEY LONDON) -> blocked
                (3, 30, 'q2', 2, '10 KENMURE ROAD HACKNEY LONDON', 'E8 1JU',
                 '10 KENMURE ROAD LONDON', 'E8 1JU',
                 'KENMURE ROAD', 'KENMURE ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 27.0::DOUBLE, 0.99::DOUBLE),
                (4, 40, 'q2', 2, '10 KENMURE ROAD LONDON', 'E8 1JX',
                 '10 KENMURE ROAD LONDON', 'E8 1JU',
                 'KENMURE ROAD', 'KENMURE ROAD',
                 NULL::VARCHAR, NULL::VARCHAR, 20.0::DOUBLE, 0.95::DOUBLE),
                -- q3: whitespace-split place name + Welsh prefix (HEOL Y) -> blocked
                (5, 50, 'q3', 3, '16 HEOL Y NANT LLANILID PONT Y CLUN', 'CF72 4AB',
                 '16 HEOL Y LLINOS LLANILID PONTYCLUN', 'CF72 4AB',
                 'HEOL Y', 'HEOL Y',
                 NULL::VARCHAR, NULL::VARCHAR, 40.0::DOUBLE, 0.99::DOUBLE),
                (6, 60, 'q3', 3, '16 HEOL Y LLINOS LLANILID PONTYCLUN', 'CF72 4BA',
                 '16 HEOL Y LLINOS LLANILID PONTYCLUN', 'CF72 4AB',
                 'HEOL Y', 'HEOL Y',
                 NULL::VARCHAR, NULL::VARCHAR, 18.0::DOUBLE, 0.95::DOUBLE),
                -- q4: low-MW cross-district active locality disagreement
                -- (GELLI PENTRE vs LLWYNYPIA TONYPANDY) -> promotes
                (7, 70, 'q4', 4, '10 TREM Y RHEILFFORDD LLWYNYPIA TONYPANDY', 'CF40 2TJ',
                 '10 SWN YR AFON DOROTHY STREET GELLI PENTRE', 'CF40 2TJ',
                 'RAILWAY VIEW', 'DOROTHY STREET',
                 NULL::VARCHAR, NULL::VARCHAR, -0.8::DOUBLE, 0.40::DOUBLE),
                (8, 80, 'q4', 4, '10 SWN YR AFON GELLI PENTRE', 'CF41 7UN',
                 '10 SWN YR AFON DOROTHY STREET GELLI PENTRE', 'CF40 2TJ',
                 'SWN YR', 'DOROTHY STREET',
                 NULL::VARCHAR, NULL::VARCHAR, -6.5::DOUBLE, 0.05::DOUBLE)
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
        assert winners == [("q1", 20), ("q2", 30), ("q3", 50), ("q4", 80)]


def test_wrong_postcode_promotion_uses_three_way_locality_evidence():
    with duckdb.connect() as con:
        df_predict = con.sql("""
            SELECT * FROM (VALUES
                -- Exact and extended locality: promote despite the old whole-address
                -- JW improvement guards.
                (1, 10, 'exact', 1,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL LAMBETH LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (2, 11, 'exact', 1,
                 '69 GIPSY HILL LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL LAMBETH LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                (3, 20, 'extended', 2,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 26.0::DOUBLE, 0.99::DOUBLE),
                (4, 21, 'extended', 2,
                 '69 GIPSY HILL LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 20.0::DOUBLE, 0.95::DOUBLE),
                (5, 30, 'short', 3,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 28.0::DOUBLE, 0.99::DOUBLE),
                (6, 31, 'short', 3,
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 24.0::DOUBLE, 0.95::DOUBLE),
                -- A missing postcode activates a separate conservative path.
                (7, 40, 'missing_pc', 4,
                 '69 GIPSY HILL HACKNEY LONDON', 'E8 1AA',
                 '69 GIPSY HILL NORWOOD', NULL::VARCHAR,
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 28.0::DOUBLE, 0.99::DOUBLE),
                (8, 41, 'missing_pc', 4,
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD', NULL::VARCHAR,
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                -- Omitted detail on the input side is not a contradiction.
                (9, 50, 'one_sided', 5,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (10, 51, 'one_sided', 5,
                 '69 GIPSY HILL LONDON', 'SW9 9AB',
                 '69 GIPSY HILL LONDON', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                -- Input and contender must directly corroborate one locality.
                (11, 60, 'no_corroboration', 6,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (12, 61, 'no_corroboration', 6,
                 '69 GIPSY HILL LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                -- Spacing-only place-name differences must abstain.
                (13, 70, 'spacing', 7,
                 '69 GIPSY HILL PONTYCLUN', 'CF72 4AA',
                 '69 GIPSY HILL PONT Y CLUN', 'CF72 4AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 34.0::DOUBLE, 0.99::DOUBLE),
                (14, 71, 'spacing', 7,
                 '69 GIPSY HILL PONT Y CLUN', 'CF72 4AB',
                 '69 GIPSY HILL PONT Y CLUN', 'CF72 4AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 18.0::DOUBLE, 0.95::DOUBLE),
                -- Numeric and flat conflicts remain hard guards.
                (15, 80, 'house_number', 8,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL LAMBETH', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (16, 81, 'house_number', 8,
                 '70 GIPSY HILL LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL LAMBETH', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                (17, 90, 'flat', 9,
                 'FLAT A 69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 'FLAT A 69 GIPSY HILL LAMBETH', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', 'A', 'A',
                 29.0::DOUBLE, 0.99::DOUBLE),
                (18, 91, 'flat', 9,
                 'FLAT B 69 GIPSY HILL LAMBETH LONDON', 'SW9 9AB',
                 'FLAT A 69 GIPSY HILL LAMBETH', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', 'B', 'A',
                 29.0::DOUBLE, 0.95::DOUBLE),
                -- A road that is not a whole-token substring produces no strict tail.
                (19, 100, 'road_failure', 10,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GYPSY HILL', 'GYPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (20, 101, 'road_failure', 10,
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GYPSY HILL', 'GYPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                -- Duplicate/compound tokens and a shared suffix remain safe.
                (21, 110, 'compound', 11,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD NORWOOD LONDON BRIDGE SOUTHWARK',
                 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 29.0::DOUBLE, 0.99::DOUBLE),
                (22, 111, 'compound', 11,
                 '69 GIPSY HILL NORWOOD LONDON BRIDGE SOUTHWARK LONDON',
                 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD NORWOOD LONDON BRIDGE SOUTHWARK',
                 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 23.0::DOUBLE, 0.95::DOUBLE),
                -- Deterministic ordering is retained with multiple plausible contenders.
                (23, 120, 'multiple', 12,
                 '69 GIPSY HILL HACKNEY LONDON', 'SW9 9AA',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 28.0::DOUBLE, 0.99::DOUBLE),
                (24, 121, 'multiple', 12,
                 '69 GIPSY HILL NORWOOD LAMBETH LONDON', 'SW9 9AB',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 24.0::DOUBLE, 0.95::DOUBLE),
                (25, 122, 'multiple', 12,
                 '69 GIPSY HILL NORWOOD SOUTHWARK LONDON', 'SW9 9AC',
                 '69 GIPSY HILL NORWOOD', 'SW9 9AA',
                 'GIPSY HILL', 'GIPSY HILL', NULL::VARCHAR, NULL::VARCHAR,
                 25.0::DOUBLE, 0.95::DOUBLE)
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
        winners = dict(
            con.sql(f"""
                SELECT unique_id_r, unique_id_l
                FROM ({improved.sql_query()})
                QUALIFY row_number() OVER (
                    PARTITION BY unique_id_r ORDER BY match_weight DESC
                ) = 1
            """).fetchall()
        )

        assert winners == {
            "compound": 111,
            "exact": 11,
            "extended": 21,
            "flat": 90,
            "house_number": 80,
            "missing_pc": 41,
            "multiple": 122,
            "no_corroboration": 60,
            "one_sided": 50,
            "road_failure": 100,
            "short": 31,
            "spacing": 70,
        }


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
