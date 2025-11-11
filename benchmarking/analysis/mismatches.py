from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def analyse_mismatches(
    matches: duckdb.DuckDBPyRelation,
    canonical: duckdb.DuckDBPyRelation,
    samples_per_reason: int = 10,
    top_worst: int = 10,
) -> dict[str, duckdb.DuckDBPyRelation]:
    """Analyse mismatches with address similarity scores.

    For incorrect matches (where unique_id != resolved_canonical_id), this:
    1. Joins with canonical data to get predicted address text
    2. Calculates Jaro-Winkler similarity between ground truth and prediction
    3. Returns random samples for each match_reason
    4. Returns the worst mismatches (lowest similarity scores)

    Parameters
    ----------
    matches:
        Match results with unique_id, resolved_canonical_id, match_reason, and
        original_address_concat (ground truth address).
    canonical:
        Canonical dataset with ukam_address_id and original_address_concat.
    samples_per_reason:
        Number of random samples to return per match_reason.
    top_worst:
        Number of worst mismatches (lowest similarity) to return.

    Returns
    -------
    dict[str, duckdb.DuckDBPyRelation]
        Dictionary with keys:
        - 'random_samples': Random samples of mismatches by match_reason
        - 'worst_mismatches': Top N worst mismatches by similarity score
    """
    # Build complete query for random samples
    random_samples_sql = f"""
    WITH incorrect_matches AS (
        SELECT
            m.unique_id,
            m.resolved_canonical_id,
            m.ukam_address_id,
            m.canonical_ukam_address_id,
            m.match_reason,
            m.ukam_address_id,
            m.original_address_concat,
            m.postcode
        FROM matches AS m
        WHERE m.match_reason IS NOT NULL
          AND m.unique_id != m.resolved_canonical_id
    ),
    mismatches_with_canonical AS (
        SELECT
            im.unique_id,
            im.resolved_canonical_id,
            im.ukam_address_id,
            im.canonical_ukam_address_id,
            im.match_reason,
            im.original_address_concat AS ground_truth_address,
            im.postcode,
            c.original_address_concat AS predicted_address,
            jaro_winkler_similarity(
                im.original_address_concat,
                c.original_address_concat
            ) AS similarity_score
        FROM incorrect_matches AS im
        LEFT JOIN canonical AS c
          ON im.canonical_ukam_address_id = c.ukam_address_id
    ),
    sampled AS (
        SELECT
            match_reason,
            unique_id,
            ukam_address_id,
            resolved_canonical_id,
            postcode,
            ground_truth_address,
            predicted_address,
            similarity_score,
            ROW_NUMBER() OVER (
                PARTITION BY match_reason
                ORDER BY RANDOM()
            ) AS rn
        FROM mismatches_with_canonical
    )
    SELECT
        match_reason,
        unique_id,
        resolved_canonical_id,
        ukam_address_id,
        postcode,
        ground_truth_address,
        predicted_address,
        ROUND(similarity_score, 3) AS similarity_score
    FROM sampled
    WHERE rn <= {samples_per_reason}
    ORDER BY match_reason, similarity_score
    """

    random_samples = matches.query("random_samples", random_samples_sql)

    # Build complete query for worst mismatches
    worst_mismatches_sql = f"""
    WITH incorrect_matches AS (
        SELECT
            m.unique_id,
            m.resolved_canonical_id,
            m.ukam_address_id,
            m.canonical_ukam_address_id,
            m.match_reason,
            m.original_address_concat,
            m.postcode
        FROM matches AS m
        WHERE m.match_reason IS NOT NULL
          AND m.unique_id != m.resolved_canonical_id
    ),
    mismatches_with_canonical AS (
        SELECT
            im.unique_id,
            im.resolved_canonical_id,
            im.ukam_address_id,
            im.canonical_ukam_address_id,
            im.match_reason,
            im.original_address_concat AS ground_truth_address,
            im.postcode,
            c.original_address_concat AS predicted_address,
            jaro_winkler_similarity(
                im.original_address_concat,
                c.original_address_concat
            ) AS similarity_score
        FROM incorrect_matches AS im
        LEFT JOIN canonical AS c
          ON im.canonical_ukam_address_id = c.ukam_address_id
    )
    SELECT
    unique_id,
    ukam_address_id,
        resolved_canonical_id,
        postcode,
        ground_truth_address,
        predicted_address,
        ROUND(similarity_score, 3) AS similarity_score,
        match_reason
    FROM mismatches_with_canonical
    ORDER BY similarity_score ASC, match_reason
    LIMIT {top_worst}
    """

    worst_mismatches = matches.query("worst_mismatches", worst_mismatches_sql)

    return {
        "random_samples": random_samples,
        "worst_mismatches": worst_mismatches,
    }


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
            "resolved_canonical_id",
            "postcode",
            "ground_truth_address",
            "predicted_address",
            "similarity_score",
        ).show(max_width=500)

    print("\n" + "=" * 80)
    print("WORST MISMATCHES (Lowest Similarity Scores)")
    print("=" * 80 + "\n")

    worst = analysis_results["worst_mismatches"]
    worst.show(max_width=500)

    print()
