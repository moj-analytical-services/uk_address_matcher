---
applyTo:
  - "benchmarking/**/*.py"
  - "docs/findings/**/*.md"
  - "example_data/**/*"
  - "tests/**/*"
  - "dev_scripts/reduced_canonical.py"
---

# Data handling and safety

- Never commit real personal or sensitive address data.
- Use sanitised fixtures and the parquet files in `example_data/` for reproduction/tests.
- Keep example/debug outputs free of personal data.
- When adding fixtures, keep them minimal but representative of UK address edge cases.
- Document any new fixture purpose in nearby tests or comments.
- Preserve expected input schemas for matching pipelines (for example `unique_id`, `address_concat`, and `postcode` where required).
- If schema assumptions change, update dependent tests and examples in the same change.
