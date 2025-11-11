# Benchmark Analysis

This module provides comprehensive analysis tools for evaluating address matching performance.

## Components

### `accuracy.py`

Calculate accuracy metrics by comparing ground truth identifiers with predicted matches.

**Key function:** `calculate_accuracy_metrics(matches)`

Returns accuracy statistics including:
- Total matched records
- Correct matches (where `unique_id == resolved_canonical_id`)
- Incorrect matches (mismatches)
- Accuracy percentage

Results are broken down by `match_reason` and include an overall summary.

### `mismatches.py`

Analyse incorrect matches using Jaro-Winkler similarity scores.

**Key function:** `analyse_mismatches(matches, canonical, samples_per_reason=10, top_worst=10)`

For mismatched records:
1. Joins with canonical data to retrieve predicted address text
2. Calculates Jaro-Winkler similarity between ground truth and prediction
3. Returns random samples for each match_reason
4. Returns the worst mismatches (lowest similarity scores)

**Helper function:** `print_mismatch_analysis(analysis_results)`

Pretty-prints the mismatch analysis with:
- Random samples grouped by match_reason
- Top N worst mismatches overall

### `reporting.py`

Display utilities for benchmark runs.

**Key function:** `print_benchmark_header(dataset_name, variant_name, enabled_stages)`

Prints a clear header showing:
- Dataset being benchmarked
- Pipeline variant name
- Matching techniques being used

## Usage Example

```python
from benchmarking.analysis import (
    calculate_accuracy_metrics,
    analyse_mismatches,
    print_benchmark_header,
)
from benchmarking.analysis.mismatches import print_mismatch_analysis

# Print header
print_benchmark_header(
    dataset_name="Lambeth Council",
    variant_name="exact_match_then_trie",
    enabled_stages=[StageName.TRIE]
)

# Run pipeline
matches = run_deterministic_pipeline(con, df_messy, df_canonical)

# Calculate accuracy
accuracy = calculate_accuracy_metrics(matches)
accuracy.show()

# Analyse mismatches
mismatch_results = analyse_mismatches(
    matches=matches,
    canonical=df_canonical,
    samples_per_reason=10,
    top_worst=10,
)
print_mismatch_analysis(mismatch_results)
```

## Understanding the Metrics

### Accuracy Calculation

Accuracy compares the `unique_id` column (ground truth) with `resolved_canonical_id` (predicted match):

- **Correct match**: `unique_id == resolved_canonical_id`
- **Incorrect match**: `unique_id != resolved_canonical_id`
- **Unmatched**: `match_reason IS NULL` (excluded from accuracy calculation)

### Jaro-Winkler Similarity

For incorrect matches, we calculate the Jaro-Winkler similarity between:
- **Ground truth address**: `original_address_concat` from messy input
- **Predicted address**: `original_address_concat` from canonical data (joined via `canonical_ukam_address_id`)

Lower similarity scores indicate worse mismatches where the predicted address text differs significantly from the ground truth.

## Output Schema

### Accuracy Metrics

```
match_reason | total_matched | correct_matches | incorrect_matches | accuracy_pct
```

### Mismatch Analysis

```
match_reason | unique_id | resolved_canonical_id | postcode | ground_truth_address | predicted_address | similarity_score
```

## Design Rationale

**SQL-based analysis**: All calculations use DuckDB SQL for performance and consistency with the matching pipeline.

**Lazy evaluation**: Results are returned as `DuckDBPyRelation` objects, allowing further filtering or aggregation before materialization.

**Composable**: Functions can be used independently or together, depending on analysis needs.

**Informative**: Provides both high-level metrics and detailed examples for debugging and improvement.
