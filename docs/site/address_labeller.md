# Address labelling tool

The UKAM Address Labeller is a standalone browser tool for reviewing candidate
matches and creating manually labelled address data. It runs locally in the
browser; the candidate CSV is not uploaded to a server.

[Open the UKAM Address Labeller](assets/ukam_address_labeller.html)

You can also download `docs/site/assets/ukam_address_labeller.html` and open it
directly in a browser.

## Prepare the candidate CSV

After running UK Address Matcher, use the following code in the same Python
session as the DuckDB connection (`con`) and match result (`result`). It combines
matches from the earlier deterministic stages with the five highest-scoring
Splink candidates for each source address.

```python
# Final matches from all UKAM stages
all_matches = result.matches(all_columns=True)

# Multiple candidates from the Splink stage
splink_predictions = result._splink_predictions()

html_candidates = con.sql("""
    WITH earlier_stage_matches AS (
        SELECT
            CAST(unique_id AS VARCHAR) AS source_id,

            -- The original messy address already contains its postcode
            original_address_concat AS messy_address,

            CAST(resolved_canonical_id AS VARCHAR) AS candidate_uprn,

            -- Canonical address plus postcode
            CONCAT_WS(
                ' ',
                original_address_concat_canonical,
                postcode_canonical
            ) AS candidate_address,

            match_weight AS match_score,
            match_reason

        FROM all_matches

        WHERE resolved_canonical_id IS NOT NULL
          AND match_reason NOT LIKE 'splink:%'
    ),

    ranked_splink_candidates AS (
        SELECT
            CAST(unique_id_r AS VARCHAR) AS source_id,

            -- The original messy address already contains its postcode
            original_address_concat_r AS messy_address,

            CAST(unique_id_l AS VARCHAR) AS candidate_uprn,

            -- Canonical candidate plus postcode
            CONCAT_WS(
                ' ',
                original_address_concat_l,
                postcode_l
            ) AS candidate_address,

            match_weight AS match_score,
            'splink candidate' AS match_reason,

            ROW_NUMBER() OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC
            ) AS candidate_rank

        FROM splink_predictions
    ),

    splink_candidates AS (
        SELECT
            source_id,
            messy_address,
            candidate_uprn,
            candidate_address,
            match_score,
            match_reason

        FROM ranked_splink_candidates

        WHERE candidate_rank <= 5
    ),

    combined_candidates AS (
        SELECT * FROM earlier_stage_matches

        UNION ALL

        SELECT * FROM splink_candidates
    )

    SELECT
        source_id,
        messy_address,
        candidate_uprn,
        candidate_address,
        match_score,
        match_reason

    FROM combined_candidates

    ORDER BY
        source_id,
        match_score DESC NULLS LAST
""")

OUTPUT_PATH = "your_output_path.csv"

html_candidates.write_csv(
    OUTPUT_PATH,
    header=True,
    sep=",",
    overwrite=True
)

row_count = html_candidates.count("*").fetchone()[0]

print(f"Saved to: {OUTPUT_PATH}")
print(f"Candidate rows: {row_count:,}")

display(
    html_candidates
        .limit(100)
        .df()
        .astype(str)
)
```

The code uses `result._splink_predictions()`, which is currently a private API
and may change between UKAM releases.

## Required CSV columns

The labeller expects these columns:

- `source_id`
- `messy_address`
- `candidate_uprn`
- `candidate_address`

It also displays `match_score` and `match_reason` when they are present.

## Label addresses

1. Open the labeller in a modern browser.
2. Choose the candidate CSV created above.
3. Review each source address and select the best candidate, **No suitable match**,
   or **Unsure / needs further review**.
4. Select **Export labels CSV** when finished.

Keyboard shortcuts are available while reviewing:

- `1` to `9`: select the numbered candidate and move to the next address
- `0`: select no suitable match
- `U`: mark the address as unsure
- Left arrow: return to the previous address

Progress is saved in browser local storage. The exported file contains
`source_id`, `messy_address`, `selected_uprn`, `selected_address`, and
`reviewed_at`.
