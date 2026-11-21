---
name: bugfix
description: "Resolve a bug in uk_address_matcher with a root-cause-first fix and targeted validation."
disable-model-invocation: true
---
Resolve the bug end to end with a root-cause-first approach.

Success means:

- the failing behaviour is reproduced or anchored to an existing failing test
- the owning defect is fixed with the smallest safe code change
- regression coverage is added or updated where applicable
- targeted `uv run pytest ...` validation passes before any broader check

Working rules:

- start from the failing command, test, file, symbol, or nearby implementation surface
- fix the root cause rather than layering on defensive patches
- keep the diff minimal and focused
- if SQL stages are involved, follow the matching `.github/instructions/*.instructions.md` guidance

Report:

- bug summary
- root cause
- fix
- validation evidence

Constraints:
- Keep changes minimal and focused.
- Follow `.github/instructions/*.instructions.md` guidance and pipeline conventions if SQL stages are involved.
- Use British English and repository commit/PR norms.
