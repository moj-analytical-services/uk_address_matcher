---
name: sql-stage
description: "Add or change a SQL pipeline stage with explicit stage contracts and targeted stage validation."
disable-model-invocation: true
---
Add or modify the SQL pipeline stage without breaking downstream contracts.

Success means:

- the stage intent, inputs, outputs, and dependencies are explicit
- the implementation stays inside the pipeline framework with DuckDB-compatible SQL
- downstream-required columns and enum registrations remain correct
- targeted stage tests pass before any broader checks

Working rules:

- review the relevant `.github/instructions/*.instructions.md` files and the owning stage chain first
- use `@pipeline_stage`, `CTEStep`, and dependency placeholders such as `{annotated_exact_matches}`
- treat missing columns, enum updates, and downstream compatibility as first-class constraints

Report:

- stage intent
- affected stages
- SQL or contract changes
- validation evidence

Constraints:
- Minimal diff, no unrelated refactors.
- Keep required columns for downstream stages.
- Use only safe fixture data from `example_data/`.
