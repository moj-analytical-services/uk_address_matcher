from __future__ import annotations

import logging
import re
from contextlib import ExitStack
from time import perf_counter
from typing import TYPE_CHECKING

from duckdb import ColumnExpression
from splink import block_on
from sqlglot import exp, parse_one

from uk_address_matcher.cleaning.steps.road_resources import sql_text, token_policy
from uk_address_matcher.sql_pipeline.helpers import (
    _drop_table_and_registered_aliases,
    _uid,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb
    from splink import Linker

logger = logging.getLogger(__name__)

_DECISION_MACROS = (
    "pc_compact(a) AS translate(a, ' ' || chr(9) || chr(10) || chr(12) || chr(13), '')",
    "pc_bag(a) AS array_to_string(list_sort(string_split(trim(a), ' ')), ' ')",
    "pc_same(a,b) AS (nullif(a,'') = nullif(b,'')) IS TRUE",
    "pc_diff(a,b) AS (nullif(a,'') <> nullif(b,'')) IS TRUE",
    "pc_num(a,b) AS (len(a)>0 AND len(b)>0 AND a<>b) IS TRUE",
    """pc_conflict(r1,i1,u1,f1,r2,i2,u2,f2) AS
        ((pc_same(r1,r2) AND pc_diff(i1,i2)) OR pc_diff(u1,u2) OR pc_diff(f1,f2))""",
    "pc_mask(a) AS regexp_replace(a,'[0-9]+','!','g')",
    """pc_context(a,r) AS list_filter(list_distinct(regexp_extract_all(
        replace(a,r,''), '[A-Z]{2,}')), lambda x: len(x)>=4 AND NOT list_contains(
        ['CLOSE','STREET','ROAD','AVENUE','LANE','TERRACE','COURT','PLACE',
         'BUILDING','HOUSE','FLAT','FLOOR','GROUND','FIRST','SECOND','BUNGALOW','BLOCK'],
        x))""",
)
_MACRO_CALL = re.compile(r"\b(pc_\w+)\(")
_PREMISE = "pc_conflict(q.role,q.ident,q.unit_id,q.flat,c.role,c.ident,c.unit_id,c.flat)"


def _scope_sql(sql: str, prefix: str) -> str:
    return _MACRO_CALL.sub(lambda match: f"{prefix}{match[0]}", sql)


def _recovery_address_columns(id_name: str) -> str:
    return f"""
        ukam_address_id::BIGINT AS {id_name},
        clean_full_address AS address, road_1_norm AS road, numeric_tokens AS nums,
        flat_identity AS flat, sub_premise_role AS role, sub_premise_identifier AS ident,
        business_unit_id AS unit_id, replace(coalesce(postcode, ''), ' ', '') AS pc
    """


@pipeline_stage(name="postcode_recovery_nominations")
def _nominations(prefix: str) -> list[CTEStep]:
    # Shortlist uses already-scored alternatives; road routes can find new aliases.
    shortlist_sql = """
        WITH contenders AS MATERIALIZED (
            SELECT a.ukam_address_id_r::BIGINT AS qid, a.ukam_address_id_l::BIGINT AS aid,
                b.ukam_address_id_l::BIGINT AS bid, a.match_weight AS w,
                b.match_weight-a.match_weight AS gap,
                length(a.postcode_r)>3 AND left(replace(a.postcode_r,' ',''),-3)
                    =left(replace(a.postcode_l,' ',''),-3) AS district
            FROM {pc_shortlist} a JOIN {pc_shortlist} b USING (ukam_address_id_r)
            JOIN {pc_eligible} q ON q.qid=a.ukam_address_id_r
            WHERE b.candidate_rank=1 AND a.candidate_rank>1
                AND b.postcode_l=b.postcode_r AND a.postcode_l<>a.postcode_r
                AND b.match_weight<25 AND a.match_weight>=-8
                AND b.match_weight-a.match_weight<=12
        ), aliases AS MATERIALIZED (
            SELECT c.alias_id,c.uprn,c.address,c.nums,c.flat,c.role,c.ident
            FROM {pc_canonical} c SEMI JOIN (
                SELECT aid AS alias_id FROM contenders
                UNION ALL
                SELECT bid FROM contenders
            ) ids USING (alias_id)
        ), evidence AS (
            SELECT x.*, a.uprn, jaro_winkler_similarity(q.address,a.address) AS sim,
                jaro_winkler_similarity(pc_mask(q.address),pc_mask(a.address))
                -jaro_winkler_similarity(pc_mask(q.address),pc_mask(b.address)) AS gain,
                coalesce(map_extract_value(q.hits,a.uprn::VARCHAR),0) AS hits_a,
                coalesce(map_extract_value(q.hits,b.uprn::VARCHAR),0) AS hits_b
            FROM contenders x JOIN {pc_evidence} q USING (qid)
            JOIN aliases a ON a.alias_id=x.aid
            JOIN aliases b ON b.alias_id=x.bid
            WHERE NOT pc_num(q.nums,a.nums) AND NOT pc_diff(q.flat,a.flat)
              AND NOT (pc_same(q.role,a.role) AND pc_diff(q.ident,a.ident))
        ), tiered AS (
            SELECT *, CASE
                WHEN district AND w>=6 AND sim>=.83 AND gain>=.08 AND gap<=12 THEN 1
                WHEN NOT district AND w>=-6 AND sim>=.85 AND hits_a>0 AND hits_a>hits_b
                    AND gain>=.10 AND gap<=10 THEN 2
                WHEN NOT district AND w>=-8 AND sim>=.85 AND gain>=.10 AND gap<=7 THEN 3
            END AS tier
            FROM evidence
        ), nominated AS (
            SELECT * FROM tiered WHERE tier IS NOT NULL
            QUALIFY row_number() OVER (
                PARTITION BY qid ORDER BY tier,w DESC,gain DESC,uprn,aid
            )=1
        )
        SELECT n.qid,n.uprn,n.aid AS alias_id,n.w,'guarded_shortlist' AS route
        FROM nominated n JOIN {pc_evidence} q USING (qid)
        ANTI JOIN {pc_catalogue} c USING (qid)
        WHERE n.sim>=q.stock_text+.05
    """
    steps = [
        CTEStep(
            "pc_inputs",
            f"""
            SELECT {_recovery_address_columns("qid")}, signature_unique_hits_map AS hits
            FROM {{pc_messy}} m
            ANTI JOIN (
                SELECT ukam_address_id AS strong_id FROM {{pc_stock}}
                WHERE match_weight>=25
            ) s ON m.ukam_address_id=s.strong_id
            WHERE nullif(replace(postcode,' ',''),'') IS NOT NULL
              AND nullif(clean_full_address,'') IS NOT NULL
              AND nullif(road_1_norm,'') IS NOT NULL
            """,
        ),
        CTEStep(
            "pc_requested_road_support",
            """
            SELECT c.pc,c.road,min(c.uprn)<>max(c.uprn) AS multiple
            FROM {pc_canonical} c SEMI JOIN (
                SELECT left(pc,-3) AS outward,road FROM {pc_inputs}
            ) requests USING (outward,road)
            WHERE c.uprn IS NOT NULL
            GROUP BY c.pc,c.road
            """,
        ),
        CTEStep(
            "pc_eligible",
            """
            SELECT q.* FROM {pc_inputs} q
            ANTI JOIN {pc_requested_road_support} p USING (pc,road)
        """,
        ),
        CTEStep(
            "pc_evidence",
            f"""
            WITH incumbent AS (
                SELECT q.qid, s.resolved_canonical_id AS uprn, s.match_weight,
                    max(jaro_winkler_similarity(q.address,c.address)) AS text_score,
                    bool_or(len(pc_compact(q.address))>=10 AND (
                        pc_compact(q.address)=pc_compact(c.address)
                        OR (pc_bag(q.address)=pc_bag(c.address)
                            AND NOT pc_num(q.nums,c.nums)))) AS full_identity,
                    bool_or(pc_same(q.road,c.road) AND len(q.nums)>0
                        AND q.nums=c.nums AND NOT {_PREMISE}) AS road_number,
                    bool_and({_PREMISE} OR pc_num(q.nums,c.nums)
                        OR pc_diff(q.road,c.road)) AS conflicts
                FROM {{pc_eligible}} q
                JOIN {{pc_stock}} s ON s.ukam_address_id::BIGINT=q.qid
                JOIN {{pc_canonical}} c ON c.uprn=s.resolved_canonical_id
                GROUP BY q.qid,s.resolved_canonical_id,s.match_weight
            )
            SELECT q.*, s.uprn AS stock_uprn, s.match_weight AS stock_weight,
                s.text_score AS stock_text,
                coalesce(s.full_identity,false) AS full_identity,
                coalesce(s.road_number,false) AS road_number,
                s.uprn IS NULL OR (NOT coalesce(s.road_number,false)
                    AND (coalesce(s.conflicts,false) OR s.text_score<.88)) AS weak
            FROM {{pc_eligible}} q LEFT JOIN incumbent s USING (qid)
        """,
        ),
        CTEStep(
            "pc_street_inputs",
            """
        SELECT *, left(pc,-3) AS outward FROM {pc_evidence}
        WHERE length(pc)>3 AND NOT full_identity AND weak IS TRUE AND len(nums)>0
          AND (stock_uprn IS NULL OR stock_weight<25)
        """,
        ),
        CTEStep(
            "pc_road_seed",
            """
        SELECT q.*, left(pc,-3) AS outward
        FROM {pc_evidence} q
        WHERE length(pc)>3 AND len(pc_compact(road))>=6 AND len(nums)>0
          AND NOT full_identity AND NOT road_number
          AND (stock_uprn IS NULL OR stock_weight<25)
        """,
        ),
        CTEStep(
            "pc_road_aliases",
            """
        SELECT c.*, pc_compact(c.road) AS road_key
        FROM {pc_canonical} c SEMI JOIN (
            SELECT outward,nums FROM {pc_street_inputs}
            UNION
            SELECT outward,nums FROM {pc_road_seed}
        ) requested USING (outward,nums)
        WHERE nullif(c.road,'') IS NOT NULL
        """,
        ),
        CTEStep(
            "pc_catalogue",
            f"""
            WITH street_scores AS (
                SELECT q.qid,c.uprn,c.alias_id,
                    jaro_winkler_similarity(q.address,c.address) AS sim,
                    len(list_intersect(regexp_extract_all(q.address,'[A-Z]{{3,}}'),
                                       regexp_extract_all(c.address,'[A-Z]{{3,}}')))
                        -len(list_distinct(regexp_extract_all(q.road,'[A-Z]{{3,}}')))
                        AS context_count,
                    {_PREMISE} AS conflict,c.pc,c.pc<>q.pc AS different
                FROM {{pc_street_inputs}} q
                JOIN {{pc_road_aliases}} c USING (outward,road,nums)
            ), street_uprns AS (
                SELECT qid,uprn,first(alias_id ORDER BY sim DESC,alias_id) AS alias_id,
                    first(pc ORDER BY sim DESC,alias_id) AS pc,
                    max(sim) AS sim,max(context_count) AS context_count,
                    bool_and(different) AS different
                FROM street_scores GROUP BY qid,uprn HAVING NOT bool_and(conflict)
            ), street_ranked AS (
                SELECT *,row_number() OVER w AS rn,lead(sim) OVER w AS runner
                FROM street_uprns
                WINDOW w AS (PARTITION BY qid ORDER BY sim DESC,context_count DESC,uprn)
            ), street_eligible AS (
                SELECT n.*,q.road FROM street_ranked n JOIN {{pc_street_inputs}} q
                    USING (qid)
                WHERE rn=1 AND uprn IS DISTINCT FROM stock_uprn AND different
                  AND sim>=.89 AND sim>=coalesce(stock_text,0)+.05 AND context_count>=1
                  AND (runner IS NULL OR sim-runner>=.012)
            ), unique_postcodes AS (
                SELECT c.uprn FROM {{pc_canonical}} c
                SEMI JOIN street_eligible USING (uprn)
                GROUP BY c.uprn HAVING min(c.pc)=max(c.pc)
            )
            SELECT qid,uprn,alias_id,NULL::DOUBLE AS w,'catalogue_street_number' AS route
            FROM street_eligible SEMI JOIN unique_postcodes USING (uprn)
            SEMI JOIN (
                SELECT pc,road FROM {{pc_requested_road_support}} WHERE multiple
            ) supported_roads USING (pc,road)
        """,
        ),
        CTEStep("pc_guarded_shortlist", shortlist_sql),
        CTEStep(
            "pc_road_inputs",
            """
            SELECT q.*, pc_context(q.address,q.road) AS road_ctx,
                pc_compact(q.road) AS road_key
            FROM {pc_road_seed} q
            ANTI JOIN {pc_catalogue} USING (qid)
            ANTI JOIN {pc_guarded_shortlist} USING (qid)
        """,
        ),
        CTEStep(
            "pc_ranked_roads",
            f"""
            WITH coarse_pairs AS MATERIALIZED (
                SELECT q.qid,c.alias_id,c.uprn,
                    q.address AS q_address,c.address AS c_address,
                    q.road_ctx AS q_context,q.road AS q_road,c.road AS c_road,
                    q.road_key AS q_key,c.road_key AS c_key,
                    q.stock_uprn,q.stock_text
                FROM {{pc_road_inputs}} q JOIN {{pc_road_aliases}} c USING (outward,nums)
                WHERE c.pc<>q.pc AND NOT {_PREMISE}
            ), keyed_pairs AS (
                SELECT qid,alias_id,uprn,q_address,c_address,q_context,c_road,
                    stock_uprn,stock_text,q_key,c_key,
                    CASE WHEN abs(length(q_key)-length(c_key))<=1
                        THEN levenshtein(q_key,c_key) END AS spelling_edits,
                    CASE
                        WHEN starts_with(c_key,q_key) THEN pc_missing_type(q_road,c_road)
                        WHEN starts_with(q_key,c_key) THEN pc_missing_type(c_road,q_road)
                        ELSE false
                    END AS matches_stem
                FROM coarse_pairs
            ), candidates AS MATERIALIZED (
                SELECT * FROM keyed_pairs WHERE spelling_edits<=1 OR matches_stem
            ), pairs AS (
                SELECT qid,alias_id,uprn,route,
                    coalesce(spelling_edits,levenshtein(q_key,c_key)) AS road_edits,
                    len(list_intersect(q_context,pc_context(c_address,c_road)))
                        AS context_count,
                    jaro_winkler_similarity(q_address,c_address) AS sim,
                    stock_uprn,stock_text
                FROM candidates
                CROSS JOIN (VALUES ('spelling'), ('stem')) routes(route)
                WHERE (route='spelling' AND spelling_edits<=1)
                   OR (route='stem' AND matches_stem)
            ), one_alias AS (
                SELECT * FROM pairs WHERE context_count>=1
                QUALIFY row_number() OVER (
                    PARTITION BY route,qid,uprn
                    ORDER BY sim DESC,road_edits,context_count DESC,alias_id
                )=1
            ), ranked AS (
                SELECT *,row_number() OVER w AS rn,lead(sim) OVER w AS runner
                FROM one_alias
                WINDOW w AS (
                    PARTITION BY route,qid
                    ORDER BY sim DESC,road_edits,context_count DESC,uprn
                )
            )
            SELECT qid,uprn,alias_id,NULL::DOUBLE AS w,route
            FROM ranked
            WHERE rn=1 AND uprn IS DISTINCT FROM stock_uprn
                    AND sim>=coalesce(stock_text,0)+.05
                    AND ((route='spelling' AND sim>=.90
                        AND (runner IS NULL OR sim-runner>=.025))
                      OR (route='stem' AND sim>=.85
                        AND (runner IS NULL OR sim-runner>=.04)))
        """,
        ),
        CTEStep(
            "pc_winners",
            """
            SELECT * FROM {pc_catalogue}
            UNION ALL BY NAME SELECT * FROM {pc_guarded_shortlist}
            UNION ALL BY NAME
            SELECT * FROM {pc_ranked_roads}
            QUALIFY row_number() OVER (PARTITION BY qid ORDER BY route)=1
        """,
        ),
    ]
    return [CTEStep(step.name, _scope_sql(step.sql, prefix)) for step in steps]


def apply_postcode_recovery(
    con: duckdb.DuckDBPyConnection,
    linker: Linker,
    stock: duckdb.DuckDBPyRelation,
    best: duckdb.DuckDBPyRelation,
    messy: duckdb.DuckDBPyRelation,
    canonical: duckdb.DuckDBPyRelation,
    aligned_inputs: list[duckdb.DuckDBPyRelation],
    owned_frames: list,
) -> duckdb.DuckDBPyRelation:
    db_api = linker._db_api
    phase_start = perf_counter()
    with ExitStack() as cleanup:
        prefix = f"__ukam_pc_{_uid()}_"
        terminals = ", ".join(
            sql_text(token) for token in token_policy()["road_syntax_terminal_tokens"]
        )
        for macro in (
            *_DECISION_MACROS,
            f"pc_has_type(r) AS (contains([{terminals}], split_part(r,' ',-1)) "
            "AND contains(r,' '))",
            "pc_missing_type(a,b) AS (pc_has_type(b) AND len(pc_compact(a))>=6 "
            "AND pc_compact(a)=array_to_string(list_slice(string_split(b,' '),1,-2),''))",
        ):
            con.execute(f"CREATE TEMP MACRO {_scope_sql(macro, prefix)}")
            cleanup.callback(con.execute, f"DROP MACRO {prefix}{macro.split('(', 1)[0]}")

        projected = canonical.project(
            f"{_recovery_address_columns('alias_id')}, unique_id AS uprn"
        ).project("*, left(pc,-3) AS outward")

        bindings = [
            InputBinding("pc_messy", messy),
            InputBinding("pc_canonical", projected),
            InputBinding("pc_stock", stock),
            InputBinding("pc_shortlist", best),
        ]
        existing_names = {name for (name,) in con.execute("SHOW TABLES").fetchall()}
        pipeline = create_sql_pipeline(
            con,
            bindings,
            stage_specs=[_nominations(prefix)],
            pipeline_name="Postcode recovery nominations",
        )
        for alias in set(pipeline.input_alias_map.values()) - existing_names:
            cleanup.callback(con.unregister, alias)
        winners_name = prefix + "winners"
        con.execute(f"CREATE TEMP TABLE {winners_name} AS {pipeline.run().sql_query()}")
        logger.info(
            "Postcode recovery nomination SQL and setup: %.3fs",
            perf_counter() - phase_start,
        )
        phase_start = perf_counter()
        cleanup.callback(_drop_table_and_registered_aliases, con, winners_name)
        winners = con.table(winners_name)
        if not winners.limit(1).fetchall():
            return stock

        expected = winners.filter("w IS NULL").project("qid, alias_id")
        scored_sql = """
            SELECT NULL::BIGINT AS qid, NULL::BIGINT AS alias_id,
                NULL::DOUBLE AS match_weight WHERE false
        """
        if expected.limit(1).fetchall():
            messy_aligned, canonical_aligned = aligned_inputs
            # predict_between skips registering a lookup if it sees a hardcoded TF
            # column, but still emits the lookup join in Splink 5.0. Use the same
            # registered numeric lookups as stock without the redundant columns.
            settings = linker._settings_obj
            tf_columns = {
                column.tf_name.strip('"') for column in settings._term_frequency_columns
            }
            blocking_projection = ", ".join(settings._columns_to_select_for_blocking)
            score_columns = {
                column.name
                for column in parse_one(
                    f"SELECT {blocking_projection}", read="duckdb"
                ).find_all(exp.Column)
            } - tf_columns
            messy_aligned, canonical_aligned = (
                side.select(
                    *(
                        ColumnExpression(col)
                        for col in side.columns
                        if col in score_columns
                    )
                )
                for side in (messy_aligned, canonical_aligned)
            )
            subsets = (
                con.sql(f"""
                    SELECT c.*, c.ukam_address_id::BIGINT AS recovery_alias_id
                    FROM ({canonical_aligned.sql_query()}) c
                    SEMI JOIN ({expected.sql_query()}) e
                        ON c.ukam_address_id::BIGINT=e.alias_id
                """),
                con.sql(f"""
                    SELECT q.*, e.alias_id AS recovery_alias_id
                    FROM ({messy_aligned.sql_query()}) q JOIN ({expected.sql_query()}) e
                        ON q.ukam_address_id::BIGINT=e.qid
                """),
            )
            frames = []
            for side, subset in zip(("c_", "m_"), subsets, strict=True):
                subset_name = prefix + side
                con.execute(f"CREATE TEMP TABLE {subset_name} AS {subset.sql_query()}")
                cleanup.callback(_drop_table_and_registered_aliases, con, subset_name)
                frame = db_api.register(
                    subset_name, dataset_display_name=side, table_name=subset_name
                )
                frames.append(frame)
            scores = linker.inference.predict_between(
                *frames,
                blocking_rules_to_generate_predictions=[block_on("recovery_alias_id")],
                threshold_match_weight=None,
                warning_mode="never",
            )
            cleanup.callback(scores.drop_table_from_database_and_remove_from_cache)
            logger.info(
                "Postcode recovery frame setup and scoring: %.3fs",
                perf_counter() - phase_start,
            )
            scored_sql = (
                scores.as_duckdbpyrelation()
                .project("""
                ukam_address_id_r::BIGINT AS qid, ukam_address_id_l::BIGINT AS alias_id,
                match_weight
            """)
                .sql_query()
            )

        winner = con.sql(f"""
            SELECT a.*, coalesce(a.w,s.match_weight,
                error('Missing score for postcode recovery alias')) AS recovery_weight
            FROM {winners_name} a LEFT JOIN ({scored_sql}) s USING (qid,alias_id)
        """)
        decision_name = prefix + "decisions"
        con.execute(f"CREATE TEMP TABLE {decision_name} AS {winner.sql_query()}")
        decision_frame = db_api.register(decision_name, table_name=decision_name)
        owned_frames.append(decision_frame)

    return con.sql(f"""
        WITH stock AS ({stock.sql_query()})
        SELECT s.* FROM stock s
        ANTI JOIN {decision_name} d ON s.ukam_address_id::BIGINT=d.qid
        UNION ALL BY NAME
        SELECT d.qid::{stock.types[0]} AS ukam_address_id,
            d.uprn AS resolved_canonical_id,
            d.alias_id::{stock.types[2]} AS canonical_ukam_address_id,
            '{MatchReason.SPLINK.value}' AS match_reason,
            d.recovery_weight AS match_weight,
            NULL::DOUBLE AS distinguishability,
            d.route AS postcode_recovery_route,
            s.resolved_canonical_id AS postcode_recovery_stock_uprn
        FROM {decision_name} d LEFT JOIN stock s ON s.ukam_address_id::BIGINT=d.qid
    """)
