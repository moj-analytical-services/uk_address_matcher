from uk_address_matcher.linking_model.matching.stages.base_stage import (
    _update_results_table,
)


def test_update_results_table_preserves_extra_column_types(duck_con):
    duck_con.execute("""
        CREATE TABLE stage_results (
            ukam_address_id BIGINT,
            unique_id VARCHAR,
            resolved_canonical_id VARCHAR,
            canonical_ukam_address_id VARCHAR,
            match_reason VARCHAR
        )
    """)
    duck_con.execute("""
        INSERT INTO stage_results VALUES
            (1, 'm-1', NULL, NULL, NULL)
    """)

    matches = duck_con.sql("""
        SELECT
            1::BIGINT AS ukam_address_id,
            'c-1'::VARCHAR AS canonical_ukam_address_id,
            'canon-1'::VARCHAR AS resolved_canonical_id,
            'splink'::VARCHAR AS match_reason,
            19.25::DOUBLE AS match_weight,
            3::INTEGER AS trigram_hit_count,
            [10::BIGINT, 20::BIGINT] AS supporting_trigram_hashes
    """)

    _update_results_table(
        con=duck_con,
        results_table="stage_results",
        matches=matches,
        stage_name="splink",
    )

    type_row = duck_con.sql("""
        SELECT
            typeof(match_weight) AS match_weight_type,
            typeof(trigram_hit_count) AS trigram_hit_count_type,
            typeof(supporting_trigram_hashes) AS supporting_hashes_type
        FROM stage_results
        WHERE ukam_address_id = 1
    """).fetchone()

    assert type_row == ("DOUBLE", "INTEGER", "BIGINT[]")
