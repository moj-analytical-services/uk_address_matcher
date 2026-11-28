"""Bounded score adjustment for the existing SQL reranker."""

# ruff: noqa: E501

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


def learned_reranker_relation(
    con: DuckDBPyConnection, reranked: DuckDBPyRelation
) -> DuckDBPyRelation:
    """Project the learnt score from an already materialised candidate table."""
    table = reranked.alias.replace('"', '""')
    columns = set(reranked.columns)
    range_bits = (
        "coalesce(numeric_range_adjustment, 0)"
        if "numeric_range_adjustment" in columns
        else "0.0"
    )
    lexical_bits = (
        "distinguishing_token_adjustment"
        if "distinguishing_token_adjustment" in columns
        else "mw_adjustment"
    )
    return con.sql(f"""
        WITH query_stats AS (
            SELECT ukam_address_id_r,
                max(match_weight, 2) AS top_scores,
                max(match_weight_original + {range_bits})
                    FILTER (WHERE nullif(postcode_l, '') = nullif(postcode_r, ''))
                    AS same_raw,
                max(match_weight_original + {range_bits})
                    FILTER (WHERE NOT coalesce(
                        nullif(postcode_l, '') = nullif(postcode_r, ''), false))
                    AS other_raw,
                max(match_weight)
                    FILTER (WHERE nullif(postcode_l, '') = nullif(postcode_r, ''))
                    AS same_final,
                max(match_weight)
                    FILTER (WHERE NOT coalesce(
                        nullif(postcode_l, '') = nullif(postcode_r, ''), false))
                    AS other_final,
                bool_or(
                    {range_bits} <> 0
                    OR abs(match_weight - match_weight_original
                        - {range_bits}
                        - {lexical_bits}) > 1e-7
                ) AS protected
            FROM "{table}"
            GROUP BY ukam_address_id_r
        ), eligible AS MATERIALIZED (
            SELECT ukam_address_id_r, same_raw, top_scores[1] AS best
            FROM query_stats
            WHERE len(top_scores) > 1
              AND top_scores[1] BETWEEN 5 AND 37
              AND (top_scores[1] - top_scores[2] <= 8 OR top_scores[1] <= 14)
              AND NOT protected
              AND same_raw > coalesce(other_raw, -1e6) + 10
              AND same_final > coalesce(other_final, -1e6) + 3
        ), contenders AS MATERIALIZED (
            SELECT c.ukam_address_id_r, c.ukam_address_id_l,
                upper(c.clean_full_address_l) AS l,
                upper(c.clean_full_address_r) AS r,
                c.match_weight, c.match_weight_original,
                c.token_reward, c.token_absence_penalty, c.bigram_reward,
                c.bigram_absence_penalty, e.same_raw, e.best
            FROM "{table}" c JOIN eligible e USING (ukam_address_id_r)
            WHERE nullif(c.postcode_l, '') = nullif(c.postcode_r, '')
              AND c.match_weight >= e.best - 9
        ), features AS (
            SELECT ukam_address_id_r, ukam_address_id_l,
                coalesce(len(list_intersect(list_slice(string_split(l,' '),1,3),
                    list_slice(string_split(r,' '),1,3))), 0)::DOUBLE / 3.0
                    AS prefix_hits,
                coalesce(jaro_winkler_similarity(l, r), 0)::DOUBLE
                    AS address_similarity,
                least(token_reward / 8, 2)::DOUBLE AS token_reward_scaled,
                least(token_absence_penalty / 6, 2)::DOUBLE AS token_absence_scaled,
                least(bigram_reward / 6, 2)::DOUBLE AS bigram_reward_scaled,
                least(bigram_absence_penalty / 4, 2)::DOUBLE AS bigram_absence_scaled,
                (least(greatest(same_raw-match_weight_original, 0),15)/10)::DOUBLE
                    AS raw_gap_scaled,
                (least(greatest(best-match_weight, 0),15)/10)::DOUBLE AS final_gap_scaled,
                (match_weight/20)::DOUBLE AS mw_scaled
            FROM contenders
        ), deltas AS (
            SELECT ukam_address_id_r, ukam_address_id_l, {MODEL_DELTA_SQL} AS delta
            FROM features
        )
        SELECT c.* REPLACE (
            c.match_weight + coalesce(d.delta, 0.0) AS match_weight
        ), coalesce(d.delta, 0.0) AS learned_reranker_adjustment
        FROM "{table}" c LEFT JOIN deltas d
            USING (ukam_address_id_r, ukam_address_id_l)
    """)


MODEL_DELTA_SQL = """\
greatest(-2.5,least(2.5,(1.0*((CASE WHEN final_gap_scaled::FLOAT <= 0.40472222864627838 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.066656015813350677 THEN (CASE WHEN bigram_reward_scaled::FLOAT <= 0.31828702986240387 THEN -0.16679828928266965 ELSE -0.36000000000000004 END) ELSE 0.16632135043538435 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.42515625059604645 THEN 0.36000000000000004 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.60021340847015381 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.48471014201641083 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.076465394347906113 THEN -0.19478944958772093 ELSE 0.12750634716005882 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.42515625059604645 THEN 0.36000000000000004 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.60021340847015381 THEN 0.36000000000000004 ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) END) END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.60155057907104492 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.40472222864627838 THEN (CASE WHEN address_similarity::FLOAT <= 0.84569615125656128 THEN -0.23767619042577165 ELSE 0.016268125105036831 END) ELSE 0.36000000000000004 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.61520558595657349 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.28319497406482697 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN -0.26047859262596668 ELSE -0.010522819511742668 END) ELSE 0.29066814951596409 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.6296532154083252 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.1366177499294281 THEN (CASE WHEN token_absence_scaled::FLOAT <= 0.13750000298023224 THEN -0.048971653825098868 ELSE -0.34627597399534726 END) ELSE 0.11173211863494877 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.62109372019767761 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN (CASE WHEN bigram_reward_scaled::FLOAT <= 0.28136573731899261 THEN -0.13858790084234193 ELSE -0.36000000000000004 END) ELSE 0.021573963345535629 END) ELSE (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.36000000000000004 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.69562456011772156 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.60155057907104492 THEN (CASE WHEN address_similarity::FLOAT <= 0.79541057348251343 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.022998125292360783 THEN -0.2919459466357896 ELSE -0.054228618747594076 END) ELSE 0.0059912975282221262 END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.18793950855679456 ELSE 0.36000000000000004 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.40472222864627838 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN (CASE WHEN bigram_reward_scaled::FLOAT <= 0.28136573731899261 THEN -0.13877892181084858 ELSE -0.36000000000000004 END) ELSE -0.0036275062613403694 END) ELSE (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN 0.10337568043871914 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.69562456011772156 THEN 0.36000000000000004 ELSE 0.36000000000000004 END) END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.60155057907104492 THEN (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN -0.14720606450312043 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.13909675180912018 THEN -0.026834067144360093 ELSE 0.13605395534035056 END) END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.59473958611488342 THEN (CASE WHEN bigram_absence_scaled::FLOAT <= 0.43124999105930328 THEN 0.097372048929204036 ELSE 0.36000000000000004 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.40472222864627838 THEN (CASE WHEN address_similarity::FLOAT <= 0.87968230247497559 THEN (CASE WHEN token_absence_scaled::FLOAT <= 0.41250000894069672 THEN -0.093180129701867453 ELSE -0.31287673437936125 END) ELSE 0.031437785164711761 END) ELSE (CASE WHEN raw_gap_scaled::FLOAT <= 1.25341796875 THEN (CASE WHEN bigram_absence_scaled::FLOAT <= 0.43124999105930328 THEN 0.026538147709754763 ELSE 0.35642739520668987 END) ELSE 0.36000000000000004 END) END)
  + (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN (CASE WHEN address_similarity::FLOAT <= 0.86015301942825317 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.022998125292360783 THEN (CASE WHEN token_absence_scaled::FLOAT <= 0.13750000298023224 THEN -0.12116282818804705 ELSE -0.34804203535100786 END) ELSE -0.075177832133686354 END) ELSE 0.030940054657274785 END) ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.69562456011772156 THEN 0.0040504889093447875 ELSE 0.36000000000000004 END) END)
  + (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN (CASE WHEN address_similarity::FLOAT <= 0.86015301942825317 THEN -0.13324727929643973 ELSE 0.016942034036621006 END) ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.6326134204864502 THEN (CASE WHEN prefix_hits::FLOAT <= 0.8333333432674408 THEN (CASE WHEN address_similarity::FLOAT <= 0.92928305268287659 THEN -0.046042836409195681 ELSE 0.15141884657010363 END) ELSE 0.10780040218483938 END) ELSE 0.34429727345814615 END) END)
  + (CASE WHEN final_gap_scaled::FLOAT <= 0.29267339408397675 THEN (CASE WHEN token_absence_scaled::FLOAT <= 0.13750000298023224 THEN -0.028430310322692748 ELSE (CASE WHEN mw_scaled::FLOAT <= 0.64189520478248596 THEN (CASE WHEN final_gap_scaled::FLOAT <= 0.0023726715007796884 THEN -0.36000000000000004 ELSE -0.1015373831168167 END) ELSE -0.065408939216160644 END) END) ELSE (CASE WHEN token_reward_scaled::FLOAT <= 0.39197917282581329 THEN 0.012519259783364378 ELSE 0.24395249712004824 END) END)
  + (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN -0.099992848084313399 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.69562456011772156 THEN (CASE WHEN prefix_hits::FLOAT <= 0.8333333432674408 THEN (CASE WHEN bigram_reward_scaled::FLOAT <= 0.55891203880310059 THEN (CASE WHEN address_similarity::FLOAT <= 0.93461436033248901 THEN -0.030468257522549065 ELSE 0.15725697996109031 END) ELSE -0.2562672870124223 END) ELSE 0.10015330459187995 END) ELSE 0.25113142168146591 END) END)
  + (CASE WHEN prefix_hits::FLOAT <= 0.50000001490116119 THEN -0.094829084465433783 ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.3839397132396698 THEN (CASE WHEN prefix_hits::FLOAT <= 0.8333333432674408 THEN (CASE WHEN bigram_reward_scaled::FLOAT <= 0.55891203880310059 THEN (CASE WHEN address_similarity::FLOAT <= 0.91382959485054016 THEN -0.055036066076835088 ELSE 0.078057992019626474 END) ELSE -0.24340626209455896 END) ELSE 0.085329795828822552 END) ELSE 0.13281643632171222 END) END)
  + (CASE WHEN address_similarity::FLOAT <= 0.79541057348251343 THEN (CASE WHEN token_absence_scaled::FLOAT <= 0.41250000894069672 THEN (CASE WHEN bigram_absence_scaled::FLOAT <= 0.43124999105930328 THEN -0.092657048970515021 ELSE 0.1183907844878378 END) ELSE (CASE WHEN final_gap_scaled::FLOAT <= 0.0063366335816681385 THEN (CASE WHEN mw_scaled::FLOAT <= 0.57480570673942566 THEN -0.36000000000000004 ELSE -0.19718739268357022 END) ELSE -0.050450540987213778 END) END) ELSE -0.0043970982807063448 END))+0.5)))
"""
