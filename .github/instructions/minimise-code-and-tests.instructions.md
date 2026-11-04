---
applyTo:
  - "**/*.py"
  - "**/*.sql"
  - "tests/**/*"
  - "frontend/**/*"
  - "shell/**/*.sh"
  - ".github/**/*.md"
  - ".github/workflows/**/*.yml"
  - ".github/workflows/**/*.yaml"
---

# Minimise Code and Tests

## Implementation

- Inspect existing code before editing and reuse or modify existing functionality wherever possible.
- Aim for the smallest correct, maintainable implementation. Prefer targeted changes; avoid
  unnecessary abstractions, helpers, configuration, defensive logic, or new files.
- Do not add speculative features or refactor unrelated code.
- Preserve correctness, existing behaviour, and performance.

## Testing

- Prioritise test quality over quantity. Review existing coverage before adding tests; extend existing
  tests where practical.
- Prefer meaningful unit and integration tests that protect important behaviour or realistic
  regressions.
- Avoid redundant tests, trivial assertions, excessive mocking, and tests of implementation details.
- Do not add tests solely to increase coverage; every permanent test should justify its maintenance
  cost.
- Keep exploratory tests, debugging scripts, and temporary experiments in `.ai_scratch/`. This
  directory is ignored by git; do not commit its contents unless explicitly requested.

## Mandatory self-review

Before finishing, inspect the complete diff and actively simplify it:

1. Remove or consolidate new code where existing functionality suffices; check for unnecessary
   abstractions, complexity, or repetition.
2. Confirm each new test protects meaningful behaviour and justifies its maintenance cost.
3. Check that the diff is proportionate to the request.
4. Apply simplifications, rerun relevant tests, and briefly report changes, tests, and any reductions
   or test consolidation.