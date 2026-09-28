# PR Summary: Reranker Runtime Optimisation

## Question

Can the normal Hackney run be made faster without reducing precision, while preserving the local evidence reranker?

## Changes

- Align the Splink prediction floor with the reranker floor: `-50` to `-20`.
- Make relation-marker reranking opt-in; prefilter distinct source addresses before normalising markers.
- Move bigram overlap statistics to the block-level query and keep the typed `use_bigrams=False` path.
- Derive compact, numeric-context, and numberless signatures once per input row; reuse them in the existing Splink blocking and comparison rules.
- Record `use_relation_marker_reranker` in the labelling manifest.
- Retained `improve_top_n_matches=5`, `improve_use_bigrams=True`, and numeric-range repair enabled.

## Runs

Full Hackney dataset, `SAMPLE_MODE=False`, normal residential canonical filter; no canonical rebuild.

| Metric | Before `a6ddd47f9db0faa3` | After `2c10146f58d086bb` |
| --- | ---: | ---: |
| Total runtime | 21.922s | 14.505s |
| Match pipeline | 21.703s | 14.292s |
| Splink stage | 16.463s | 9.544s |
| Rows entering Splink | 68,091 | 68,091 |
| Rows matched at MW10 | 112,909 | 112,911 |
| Correct labelled rows | 112,314 | 112,316 |
| False positives at MW10 | 595 | 595 |
| Precision | 99.4730% | 99.4730% |
| Recall | 98.3778% | 98.3796% |
| PR-AUC | 0.581544 | 0.581562 |

The match pipeline improved by 7.412s (34.2%), and Splink by 6.919s (42.1%). A warm repeat measured 13.82s total and 13.61s in the match pipeline.

The precision-recall curves are close, with mixed movement across 606 shared thresholds: precision was slightly higher at 134, lower at 457, and equal at 15. At MW10 precision was unchanged, recall improved slightly, and false positives did not increase. The overall PR-AUC rose by 0.000018.

## Reference Check

This checkout did not reproduce the supplied reference counts. The local run entered Splink with 68,091 rows, versus the expected 68,185; after the changes it produced 1,334,824 raw pairs and 216,292 reranked rows, versus 1,343,834 and 216,809 expected. At MW10 it produced 112,911 rows, 112,316 correct, and 595 false positives, versus 113,050, 112,454, and 596 expected. The cause was not established, so exact equivalence to the supplied reference remains unverified.

The locally computed SHA-256 for normalised, sorted, all-column final result rows is `b8b1a21f761de50822f2cbf0a52996ca4f686bbd49ee8211fc5b481620db8c8c`; it does not match the supplied `288d32aab25a157e4ddc2b1225a1d9619d5c8f22efe34a593a2d81bbb7e06c76`. The repository does not define the reference fingerprint serialisation. Whole-pipeline and Splink-stage runtimes improved; the final local-score query was not profiled separately.

## Validation

- Ruff lint: passed; Ruff format check: all 164 files already formatted.
- `uv run pytest tests/ --ignore=tests/labelling -q -o log_cli=false --tb=short`: 453 passed, 4 warnings.
- `uv run pytest tests/test_bigrams.py tests/test_numeric_range_reranker.py -q`: 15 passed.
- `tests/test_linker.py`: 29 passed; focused stage and manifest checks passed.
- `git diff --check`: passed.

## Artefacts

- [Before/after comparison report](../../benchmarking/results/hackney/2026-09-28/2c10146f58d086bb/comparison_report_a6ddd47f9db0faa3_vs_2c10146f58d086bb.md)
- [Comparison summary JSON](../../benchmarking/results/hackney/2026-09-28/2c10146f58d086bb/comparison_summary_a6ddd47f9db0faa3_vs_2c10146f58d086bb.json)
- [Precision-recall overlay specification](../../benchmarking/results/hackney/2026-09-28/2c10146f58d086bb/charts/precision_recall_overlay_a6ddd47f9db0faa3_vs_2c10146f58d086bb.vl.json)
- [After-run manifest](../../benchmarking/results/hackney/2026-09-28/2c10146f58d086bb/manifest.json)