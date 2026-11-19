# Contributing to UK Address Matcher

UK Address Matcher is maintained by the same team as
[Splink](https://github.com/moj-analytical-services/splink). We follow the same general
[contribution principles](https://github.com/moj-analytical-services/splink/blob/master/CONTRIBUTING.md). We welcome contributions, but ask that they are practical improvements, an agreed scope and small, focused changes that the team can review and maintain.

We are also aware that contributing to open-source can take many forms! Below are some ways you can get involved and guidance to help you contribute effectively.

## Asking questions

Please ask questions on our
[GitHub discussion board](https://github.com/moj-analytical-services/uk_address_matcher/discussions)
so that other users can benefit from the answers. Check the
[documentation](https://moj-analytical-services.github.io/uk_address_matcher/)
and existing discussions first in case your question has already been answered.

## Reporting issues

If something is broken, behaves unexpectedly or is missing a useful feature,
please [raise an issue](https://github.com/moj-analytical-services/uk_address_matcher/issues).
Search existing issues first and link to any related reports.

For bugs, include your operating system, Python, UK Address Matcher and DuckDB
versions, what you expected to happen and what actually happened. Where possible,
include a small, self-contained example using synthetic addresses that reproduces
the problem. For feature requests, explain the practical problem you want to solve.
Do not include private address data, confidential benchmark results or credentials.
Report suspected vulnerabilities privately using our [security policy](SECURITY.md).

## Contributing to documentation

Documentation contributions are welcome, including typo fixes, clearer examples
and accessibility improvements. For a small correction, please open a small PR
with the proposed change rather than only describing the edit in an issue.
You can edit the relevant file on GitHub or work in a fork locally.

Keep documentation concise and within the scope of your contribution. A brief
acknowledgement of a minor behaviour change can be useful; it does not normally
need a new guide or extensive explanation. We aim to keep the documentation
lightweight and focused on what users need.

Do not make sweeping documentation rewrites or reorganise unrelated pages as part
of a feature PR. Discuss substantial changes with the team before starting.
Take particular care with automated AI edits: review the full diff and remove
changes beyond the agreed scope. A small feature should not cause unrelated
documentation to be rewritten around that feature or imply that it is the default
approach throughout the project.

## Contributing code

### Before starting work

If this is your first code contribution, comment on the relevant
[GitHub issue](https://github.com/moj-analytical-services/uk_address_matcher/issues)
before starting. If there is no issue, open one. Briefly explain why you want to
work on it, the problem you have encountered and your proposed approach.

Wait for a maintainer to confirm that a PR would be welcome and agree its scope.
An open issue is not, by itself, an invitation to implement a solution. Our main
constraint is often the time needed to review, test and maintain a change, rather
than the time needed to write it. We particularly welcome contributions from
people using UK Address Matcher to solve practical address-matching problems or with improvements to the performance of the library.

Established contributors do not normally need explicit agreement for small,
focused changes. Substantial changes, new features and changes to public behaviour
should still be discussed with the team first.

Once the work is agreed, comment on the issue to say you are working on it, open a
draft PR early and keep the change as small and focused as reasonably possible.
Avoid unrelated refactoring, formatting, dependency updates or documentation edits.
Add focused tests for behaviour changes and explain how you verified the result.
Use `uv` for local development commands, such as `uv sync` and
`uv run pytest tests/<relevant_test_file>.py`. See the
[development notes](readme.md#development) and
[security checks](security/README.md) for further setup guidance.

PR workflows from external forks require approval before they run. This approval
allows CI to execute; it is not approval to merge or permission to change
maintainer-owned security controls. Do not disable checks or change security
baselines to make a PR pass. Ask a maintainer about any required control changes.

PRs that do not follow the contribution process may be closed without detailed review.

### Use of AI

AI assistance with a contribution is permitted, but you remain responsible for
understanding and testing everything you submit.

- External contributors should declare significant use of AI in their PR. We openly use AI ourselves, but encourage responsible and transparent use.
- Write PR descriptions, comments, issue reports and other interactions with the
  team yourself, not with AI. Explain the problem, your decisions and the checks
  you performed in your own words. Please follow our PR templates when submitting contributions.
- Do not submit unreviewed AI output or use AI to expand a change beyond its
  agreed scope. The documentation guidance above applies equally to AI-assisted work.

Obvious AI-generated replies or PRs produced entirely by AI without meaningful
contributor understanding and review may be rejected or closed without detailed
review. AI assistance does not replace the requirement to agree scope with the
team before starting work.