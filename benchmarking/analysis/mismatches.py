from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def analyse_mismatches(
    ukam_matches: duckdb.DuckDBPyRelation,
    ukam_canonical: duckdb.DuckDBPyRelation,
    ukam_messy: duckdb.DuckDBPyRelation,
    samples_per_reason: int = 10,
    top_worst: int = 10,
    top_high_confidence: int = 10,
    top_same_building: int = 10,
    exclude_unmatchable: bool = True,
) -> dict[str, duckdb.DuckDBPyRelation]:
    """Analyse mismatches from multiple angles to identify failure patterns.

    For incorrect matches (where ukam_label != resolved_canonical_id), this provides:
    1. Random samples for each match_reason
    2. Worst mismatches (lowest similarity scores) - completely wrong predictions
    3. High-confidence wrong matches (highest similarity) - subtle differences
    4. Same-building mismatches - very high similarity, likely flat/unit confusion
    5. Summary of unmatchable records (ground truth UPRN not in canonical)

    Parameters
    ----------
    ukam_matches:
        Match results with unique_id, resolved_canonical_id, match_reason, and
        ukam_address_id. Ground truth address fields are sourced from
        ``ukam_messy``.
    ukam_canonical:
        Canonical dataset with ukam_address_id, original_address_concat, and
        clean_full_address.
    ukam_messy:
        Messy dataset with ukam_address_id, original_address_concat, and
        clean_full_address.
    samples_per_reason:
        Number of random samples to return per match_reason.
    top_worst:
        Number of worst mismatches (lowest similarity) to return.
    top_high_confidence:
        Number of high-confidence wrong matches (highest similarity) to return.
    top_same_building:
        Number of same-building mismatches (similarity > 0.9) to return.
    exclude_unmatchable:
        If True (default), excludes records where the ground truth ukam_label
        doesn't exist in the canonical dataset from the mismatch analysis.
        These records are instead summarised separately.

    Returns
    -------
    dict[str, duckdb.DuckDBPyRelation]
        Dictionary with keys for each analysis type, including
        'unmatchable_summary' if exclude_unmatchable is True.
    """

    results: dict[str, duckdb.DuckDBPyRelation] = {}

    # 0. Unmatchable records summary (ground truth UPRN not in canonical)
    if exclude_unmatchable:
        unmatchable_sql = """
        WITH unmatchable_records AS (
            SELECT
                matches_input.unique_id,
                matches_input.ukam_label,
                matches_input.resolved_canonical_id,
                matches_input.match_reason,
                messy.original_address_concat,
                messy.postcode
            FROM matches_input
            LEFT JOIN ukam_messy AS messy
              ON matches_input.ukam_address_id = messy.ukam_address_id
            WHERE matches_input.match_reason IS NOT NULL
              AND matches_input.ukam_label != matches_input.resolved_canonical_id
              AND NOT EXISTS (
                  SELECT 1 FROM ukam_canonical AS c_check
                  WHERE c_check.unique_id = matches_input.ukam_label
              )
        )
        SELECT
            match_reason,
            COUNT(*) AS unmatchable_count
        FROM unmatchable_records
        GROUP BY match_reason
        ORDER BY unmatchable_count DESC
        """
        results["unmatchable_summary"] = ukam_matches.query(
            "matches_input", unmatchable_sql
        )

    # Build and MATERIALISE the base mismatch data once to avoid repeated EXISTS checks
    unmatchable_filter = (
        """
          AND EXISTS (
              SELECT 1 FROM ukam_canonical AS c_check
              WHERE c_check.unique_id = matches_input.ukam_label
          )"""
        if exclude_unmatchable
        else ""
    )

    # Create materialised view of mismatches with canonical and messy data
    # Join to get clean_full_address from both messy and canonical datasets
    base_mismatches_sql = f"""
    SELECT
        im.unique_id,
        im.ukam_label,
        im.resolved_canonical_id,
        im.ukam_address_id,
        im.canonical_ukam_address_id,
        im.match_reason,
                messy.original_address_concat AS messy_original_address_concat,
        messy.clean_full_address AS messy_clean_full_address,
                messy.postcode AS postcode_messy,
        c.original_address_concat AS canonical_original_address_concat,
        c.clean_full_address AS canonical_clean_full_address,
        c.postcode AS postcode_canonical,
        jaro_winkler_similarity(
                        messy.original_address_concat,
            c.original_address_concat
        ) AS similarity_score
    FROM (
        SELECT
            matches_input.unique_id,
            matches_input.ukam_label,
            matches_input.resolved_canonical_id,
            matches_input.ukam_address_id,
            matches_input.canonical_ukam_address_id,
                        matches_input.match_reason
        FROM matches_input
        WHERE matches_input.match_reason IS NOT NULL
          AND matches_input.ukam_label != matches_input.resolved_canonical_id{unmatchable_filter}
    ) AS im
    LEFT JOIN ukam_canonical AS c
      ON im.canonical_ukam_address_id = c.ukam_address_id
    LEFT JOIN ukam_messy AS messy
      ON im.ukam_address_id = messy.ukam_address_id
    """

    # Materialise by fetching into a new relation (forces evaluation once)
    mismatches_base = ukam_matches.query("matches_input", base_mismatches_sql)

    # Force materialisation by creating a temporary table
    # This avoids re-running the EXISTS check for every subsequent query
    mismatches_base.query(
        "m", "CREATE OR REPLACE TEMP TABLE _mismatches_materialised AS SELECT * FROM m"
    )
    mismatches_materialised = ukam_matches.query(
        "matches_input", "SELECT * FROM _mismatches_materialised"
    )

    # 1. Random samples by match_reason (using materialised table)
    # Address order: messy_original, messy_clean, canonical_clean, canonical_original
    random_samples_sql = f"""
    WITH sampled AS (
        SELECT
            match_reason,
            unique_id,
            ukam_label,
            ukam_address_id,
            resolved_canonical_id,
            postcode_messy,
            postcode_canonical,
            messy_original_address_concat,
            messy_clean_full_address,
            canonical_clean_full_address,
            canonical_original_address_concat,
            similarity_score,
            ROW_NUMBER() OVER (
                PARTITION BY match_reason
                ORDER BY RANDOM()
            ) AS rn
        FROM m
    )
    SELECT
        match_reason,
        unique_id,
        ukam_label,
        resolved_canonical_id,
        ukam_address_id,
        postcode_messy,
        postcode_canonical,
        messy_original_address_concat,
        messy_clean_full_address,
        canonical_clean_full_address,
        canonical_original_address_concat,
        ROUND(similarity_score, 3) AS similarity_score
    FROM sampled
    WHERE rn <= {samples_per_reason}
    ORDER BY match_reason, similarity_score
    """
    random_samples = mismatches_materialised.query("m", random_samples_sql)

    # 2. Worst mismatches (lowest similarity) - completely wrong predictions
    # Address order: messy_original, messy_clean, canonical_clean, canonical_original
    worst_mismatches_sql = f"""
    SELECT
        unique_id,
        ukam_label,
        ukam_address_id,
        resolved_canonical_id,
        postcode_messy,
        postcode_canonical,
        messy_original_address_concat,
        messy_clean_full_address,
        canonical_clean_full_address,
        canonical_original_address_concat,
        ROUND(similarity_score, 3) AS similarity_score,
        match_reason
    FROM m
    ORDER BY similarity_score ASC, match_reason
    LIMIT {top_worst}
    """
    worst_mismatches = mismatches_materialised.query("m", worst_mismatches_sql)

    # 3. High-confidence wrong matches (highest similarity) - subtle differences
    # These are the trickiest: the algorithm was confident but wrong
    # Address order: messy_original, messy_clean, canonical_clean, canonical_original
    high_confidence_wrong_sql = f"""
    SELECT
        unique_id,
        ukam_label,
        ukam_address_id,
        resolved_canonical_id,
        postcode_messy,
        postcode_canonical,
        messy_original_address_concat,
        messy_clean_full_address,
        canonical_clean_full_address,
        canonical_original_address_concat,
        ROUND(similarity_score, 3) AS similarity_score,
        match_reason
    FROM m
    WHERE similarity_score < 1.0  -- Exclude exact string matches (data issues)
    ORDER BY similarity_score DESC, match_reason
    LIMIT {top_high_confidence}
    """
    high_confidence_wrong = mismatches_materialised.query(
        "m", high_confidence_wrong_sql
    )

    # 4. Same-building mismatches (similarity > 0.9) - likely flat/unit confusion
    # Very high similarity suggests same building, wrong flat/unit
    # Address order: messy_original, messy_clean, canonical_clean, canonical_original
    same_building_sql = f"""
    SELECT
        unique_id,
        ukam_label,
        ukam_address_id,
        resolved_canonical_id,
        postcode_messy,
        postcode_canonical,
        messy_original_address_concat,
        messy_clean_full_address,
        canonical_clean_full_address,
        canonical_original_address_concat,
        ROUND(similarity_score, 3) AS similarity_score,
        match_reason
    FROM m
    WHERE similarity_score >= 0.9
      AND similarity_score < 1.0
    ORDER BY similarity_score DESC, RANDOM()
    LIMIT {top_same_building}
    """
    same_building = mismatches_materialised.query("m", same_building_sql)

    # 5. Similarity bucket distribution - understand where errors concentrate
    similarity_buckets_sql = """
    SELECT
        CASE
            WHEN similarity_score >= 0.95 THEN '0.95-1.00 (near-identical)'
            WHEN similarity_score >= 0.90 THEN '0.90-0.95 (same building?)'
            WHEN similarity_score >= 0.80 THEN '0.80-0.90 (similar)'
            WHEN similarity_score >= 0.60 THEN '0.60-0.80 (partial match)'
            ELSE '< 0.60 (very different)'
        END AS similarity_bucket,
        COUNT(*) AS mismatch_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_mismatches
    FROM m
    GROUP BY 1
    ORDER BY
        CASE similarity_bucket
            WHEN '0.95-1.00 (near-identical)' THEN 1
            WHEN '0.90-0.95 (same building?)' THEN 2
            WHEN '0.80-0.90 (similar)' THEN 3
            WHEN '0.60-0.80 (partial match)' THEN 4
            ELSE 5
        END
    """
    similarity_buckets = mismatches_materialised.query("m", similarity_buckets_sql)

    results["random_samples"] = random_samples
    results["worst_mismatches"] = worst_mismatches
    results["high_confidence_wrong"] = high_confidence_wrong
    results["same_building"] = same_building
    results["similarity_buckets"] = similarity_buckets

    return results


def print_mismatch_analysis(
    analysis_results: dict[str, duckdb.DuckDBPyRelation],
) -> None:
    """Pretty-print mismatch analysis results.

    Parameters
    ----------
    analysis_results:
        Dictionary returned from analyse_mismatches().
    """
    print("\n" + "=" * 80)
    print("MISMATCH ANALYSIS")
    print("=" * 80)

    # Show unmatchable records summary first if present
    if "unmatchable_summary" in analysis_results:
        unmatchable = analysis_results["unmatchable_summary"]
        total_unmatchable = unmatchable.aggregate("SUM(unmatchable_count)").fetchone()
        if total_unmatchable and total_unmatchable[0]:
            print("\n--- Unmatchable Records (Ground Truth UPRN Not in Canonical) ---")
            print(
                "These records are excluded from mismatch analysis as they could "
                "never be matched correctly.\n"
            )
            unmatchable.show(max_width=50000)
            print(f"\nTotal unmatchable records: {int(total_unmatchable[0]):,}")
        else:
            print("\n✓ All ground truth UPRNs exist in canonical dataset.\n")

    # Similarity bucket distribution - gives overview
    print("\n--- Mismatch Distribution by Similarity ---\n")
    analysis_results["similarity_buckets"].show(max_width=50000)

    print("\n--- Random Sample of Mismatches by Match Reason ---\n")
    random_samples = analysis_results["random_samples"]

    # Count by reason first
    reason_counts = (
        random_samples.aggregate("match_reason, COUNT(*) as sample_count")
        .order("match_reason")
        .fetchall()
    )

    for reason, count in reason_counts:
        print(f"\n{reason} ({count} samples):")
        print("-" * 80)

        reason_samples = random_samples.filter(f"match_reason = '{reason}'")
        reason_samples.select(
            "unique_id",
            "ukam_label",
            "resolved_canonical_id",
            "postcode_messy",
            "postcode_canonical",
            "messy_original_address_concat",
            "messy_clean_full_address",
            "canonical_clean_full_address",
            "canonical_original_address_concat",
            "similarity_score",
        ).show(max_width=50000)

    print("\n" + "=" * 80)
    print("WORST MISMATCHES (Lowest Similarity Scores)")
    print("These are completely wrong predictions - useful for finding parsing bugs")
    print("=" * 80 + "\n")
    analysis_results["worst_mismatches"].show(max_width=50000)

    print("\n" + "=" * 80)
    print("HIGH-CONFIDENCE WRONG MATCHES (Highest Similarity Scores)")
    print("Subtle differences the algorithm missed - often flat/unit issues")
    print("=" * 80 + "\n")
    analysis_results["high_confidence_wrong"].show(max_width=50000)

    print("\n" + "=" * 80)
    print("SAME-BUILDING MISMATCHES (Similarity >= 0.9)")
    print("Very similar addresses - likely matched to wrong flat/unit in same building")
    print("=" * 80 + "\n")
    analysis_results["same_building"].show(max_width=50000)

    print()
