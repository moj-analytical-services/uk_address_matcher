---
name: rebuild-reduced-canonical
description: "Use the benchmark-experiment skill to rebuild the reduced canonical dataset with the standard workflow and report readiness for benchmarking."
disable-model-invocation: true
---
Use the `benchmark-experiment` skill for this workflow.

Goal:

- rebuild the reduced canonical dataset with the standard repository path and report whether it is ready for the next benchmark run

Success means:

- `.github/instructions/benchmark-experiments.instructions.md` is used first
- the standard rebuild path is used unless it is broken
- the source path, output path, and filtered row count are reported when available
- readiness for the next benchmark run is stated clearly

Stop rules:

- do not invent a bespoke canonical-preparation script unless the standard rebuild path is broken
- do not treat the rebuild as successful until the output path and benchmark-readiness check are both reported

Constraints:
- Use `uv`.
- Keep the workflow repeatable and minimal.
- Report blockers precisely if environment variables or source artefacts are missing.