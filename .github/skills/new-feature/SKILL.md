---
name: new-feature
description: "Implement a focused feature in uk_address_matcher with clear success criteria and targeted validation."
disable-model-invocation: true
---
Implement the feature with the smallest design that satisfies the request.

Success means:

- scope and acceptance criteria are clear before the main edit
- the implementation follows existing `cleaning`, `linking_model`, `sql_pipeline`, and test patterns
- only the necessary files are changed
- targeted `uv run pytest ...` validation is run for the touched behaviour

Working rules:

- read the relevant `.github/instructions/*.instructions.md` files first
- prefer the existing abstraction that already owns the behaviour
- reuse helpers and conventions before adding new ones
- keep follow-up risks and open questions explicit if they materially affect the design

Report:

- feature scope
- main design choice
- changed behaviour
- validation evidence

Constraints:
- British English.
- PEP8 + practical typing.
- DuckDB/Splink pipeline conventions must be preserved.
- No real personal data in code, tests, or fixtures.
