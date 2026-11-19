# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [1.1.2] - 2026-04-29

### Fixed

- Removed automatic `splink_udfs` installation because the matcher only uses DuckDB built-ins and the test suite passes without the community extension.
- Removed unnecessary Splink input rematerialisation of `address_without_numbers`, which could trigger large DuckDB temporary spill files before `predict()`.

## [1.1.1] - 2026-04-18

### Added

- New benchmarking datasets: Aberdeenshire council tax, Mid Sussex business rates, and Rhondda.
- Excel loader support for DuckDB-backed benchmarking datasets.
- No-whitespace matching logic in the exact match stage to catch addresses that differ only by spacing.
- Flat handling improvements: retraction of redundant `FLAT`/`APARTMENT` tokens during exact matching and parsing of additional flat-style variations.
- Benchmarking README and agent-facing instructions describing how to run benchmark experiments and audits.
- Time-based persistence hashes for benchmarking runs, with overlay precision–recall charts when comparing persisted runs.

### Changed

- Simplified the `run_selected_datasets` API and removed the separate `enable_diagnostics` flag.
- Renamed dataset keys from `s3_key` to `file_name` and extracted path configuration into dedicated helpers.
- Warn (rather than error) when a benchmarking run references an unknown persistence hash.

### Fixed

- Accuracy table now correctly accounts for unmatched records when reporting matched-row counts and precision.
- Resolved a performance regression in benchmark diagnostics output.

## [1.1.0] - 2026-04-03

### Added

- Stage timings emitted at debug level to make pipeline performance easier to monitor.
- `ukam_label` validation decorator to catch mislabelled inputs early.
- Inverted index exposed on the address matcher class for downstream inspection.
- Splink top-k comparison mode in benchmarking, alongside richer accuracy and stage-diagnostics outputs.
- Precision–recall, stacked, and overlay charts for comparing runs, with support for HTML chart inputs and waterfall-as-text rendering.
- "EXCLUDING" token handling (including basement scenarios) and expansion of `RR` / `R/O` abbreviations to `rear`.
- Reporting of alternative matches and a false-positive report view.

### Changed

- Simplified loading of the Splink `Linker` object and tightened table management around predictions (`_splink_predictions`).
- Removed support for pre-cleaned canonical parquet inputs in favour of the standard cleaning pipeline.
- Suppressed a noisy Splink warning emitted during normal use.
- Cast `match_reason` to a stable string for display and switched internal usage to the `MatchReason` enum.

### Fixed

- Distinguishability calculations now behave consistently when `best_match_only=False`.
- Jaccard comparison no longer errors on empty strings.
- Canonical folders can now be read from `https://` locations.
- Corrected the project licence metadata.

## [1.0.1] - 2026-03-13

### Added

- Documentation for choosing a matching threshold and optimising accuracy.
- Updated examples to use downloadable `ukam` datasets.
- CI concurrency rules to cancel stale workflow runs.

### Changed

- `ukam_address_id` is now `int32` rather than a UUID.
- Renamed materialised/transient table conventions, using a `__ukam__tmp` prefix for transient tables.
- Refactored and simplified the benchmarking modules (moved analysis into `insights`, simplified data loading).
- Bumped minimum Splink to `4.0.16`.

### Fixed

- Exploding blocking rules no longer fail on certain inputs.
- Fixed a materialisation bug surfaced by issue #301.
- Benchmark fixes including UPRNs no longer being cast as floats.
- Typos in the README.

## [1.0.0] - 2026-03-04

Initial stable release.

[Unreleased]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.1.2...HEAD
[1.1.2]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/moj-analytical-services/uk_address_matcher/releases/tag/v1.0.0
