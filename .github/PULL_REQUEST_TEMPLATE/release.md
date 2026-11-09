## Release

Release version: `vX.Y.Z`
Previous release or comparison baseline:

Briefly summarise the changes in this release.

## Version Checklist

- [ ] Chosen the version according to [Semantic Versioning](https://semver.org/).
- [ ] Updated `uk_address_matcher/__init__.py`.
- [ ] Updated `pyproject.toml`.
- [ ] Updated `uv.lock` and confirmed all three version declarations agree.

## Validation Checklist

- [ ] Run the [release E2E validation prompt](https://github.com/moj-analytical-services/uk_address_matcher/blob/main/.github/prompts/release-e2e.prompt.md) for the target version and reviewed its results.
- [ ] Included `docs/runtime/<target-release>/manifest.json` in this PR.
- [ ] Included `docs/runtime/<target-release>/report.md` in this PR.
- [ ] Confirmed preparation and matching runtimes look sensible against the comparison baseline.
- [ ] Reviewed preparation and pooled-council peak RSS, spill usage and cleanup; the figures look sensible.
- [ ] Confirmed matching precision, recall and F1 at MW 8, 10 and 12 are comparable to the baseline for both supplied-postcode and postcode-less runs.
- [ ] Explained any regressions, missing measurements or incomplete validation below for release review.

## Comparison Notes

Link the release reports and explain any material runtime, resource or matching
differences from the baseline. State explicitly if there are no material differences.