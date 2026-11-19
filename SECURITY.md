# Security policy

## Reporting a vulnerability

If you find a security vulnerability or a security-related issue with the software,
please do not open a public issue containing exploit details, credentials, private
address data, or confidential benchmark results. Report suspected vulnerabilities
privately through
[GitHub security advisories](https://github.com/moj-analytical-services/uk_address_matcher/security/advisories/new).
If private reporting is unavailable, ask the maintainers for a private reporting
channel without disclosing the vulnerability.

Use the latest published release. Security fixes target the current release; older
releases do not have a separate backport commitment.

## Contributors

Run security checks, but do not modify `security/`, scanner configuration, dependency
audit scripts, security workflows, or their ownership rules. Do not introduce inline
security suppressions or audit exclusions to make a check pass. Remove genuine secrets
and rotate exposed credentials; deleting a committed secret does not make it safe.

When a check reports a false positive or a dependency change needs approval, ask a
maintainer. Maintainers handle justified exceptions and dependency updates in separate
maintainer-owned PRs. Dependabot proposals receive the same protection, not a bypass.

Never commit personal or sensitive address data. Sanitise fixtures and keep private
records out of logs, screenshots, job summaries, comments, and uploaded artefacts.
Secret scanners are not personal-data detectors.

## Maintainers

Security controls are reviewed by `moj-analytical-services/data_linking`. Only repository
administrators and users with the `maintain` role may author or trigger protected-file
updates. Ordinary write access is not sufficient. Code-owner review is still required.

See [developer security administration](security/README.md) for setup, exception review,
GitHub protection requirements, and release prerequisites. These controls are not
runtime configuration and are excluded from distribution packages.