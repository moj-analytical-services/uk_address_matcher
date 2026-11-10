from __future__ import annotations

import fnmatch
import http.client
import json
import os
import re
import sys

PROTECTED_PATHS = (
    "security/*",
    ".github/CODEOWNERS",
    ".github/workflows/*",
    ".github/dependabot.*",
    ".pre-commit-config.*",
    ".gitignore",
    ".bandit",
    "bandit.*",
    "zizmor.*",
    "pyproject.toml",
    "uv.lock",
    "uv.toml",
    "package.json",
    "package-lock.json",
    ".npmrc",
    "frontend/labelling/*",
    "dev_scripts/check_secrets.py",
    "dev_scripts/audit_dependencies.py",
    "dev_scripts/*security*.py",
    "tests/test_security_checks.py",
    "scripts/release.py",
    "scripts/prepare-release.sh",
    "tests/test_release_process.py",
    ".github/release-*.json",
    "SECURITY.md",
    "MANIFEST.in",
)
SUPPRESSION = re.compile(
    r"(?i)(?:#\s*nosec\b|pragma:\s*allowlist\s+(?:secret|nextline\s+secret)"
    r"|zizmor:\s*ignore|--ignore-vuln\b|(?:^|\s)(?:SKIP|BANDIT_CONFIG)\s*=)"
)
REVIEWABLE_SUFFIXES = (".py", ".js", ".sh", ".yaml", ".yml", ".toml", ".json")
ACTION_PIN = re.compile(
    r"(?P<target> +(?:- +)?uses: +[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+)"
    r"@[0-9a-fA-F]{40}(?: +# v[0-9][0-9A-Za-z.+-]*)?"
)


def is_dependabot_action_update(
    pull_request: dict[str, object],
    files: list[dict[str, object]],
    repository: str,
) -> bool:
    user = pull_request.get("user")
    head = pull_request.get("head")
    if not isinstance(user, dict) or not isinstance(head, dict):
        return False
    if (
        user.get("login") != "dependabot[bot]"
        or user.get("id") != 49699333
        or user.get("type") != "Bot"
    ):
        return False
    head_repository = head.get("repo")
    if (
        not isinstance(head_repository, dict)
        or head_repository.get("full_name") != repository
        or not str(head.get("ref", "")).startswith("dependabot/github_actions/")
        or not files
    ):
        return False

    for file in files:
        if (
            not re.fullmatch(r"\.github/workflows/[^/]+\.ya?ml", str(file["filename"]))
            or file.get("status") != "modified"
            or file.get("previous_filename")
        ):
            return False
        patch = file.get("patch")
        if not isinstance(patch, str):
            return False
        before: list[tuple[str, str]] = []
        after: list[tuple[str, str]] = []
        additions = 0
        deletions = 0
        for line in patch.splitlines():
            if line.startswith(("@@ ", " ")):
                before.append(("context", line))
                after.append(("context", line))
            elif line.startswith(("+", "-")):
                match = ACTION_PIN.fullmatch(line[1:])
                if match is None:
                    return False
                if line.startswith("+"):
                    after.append(("pin", match["target"]))
                    additions += 1
                else:
                    before.append(("pin", match["target"]))
                    deletions += 1
            else:
                return False
        if (
            not additions
            or before != after
            or additions != file.get("additions")
            or deletions != file.get("deletions")
        ):
            return False
    return True


def protected_changes(files: list[dict[str, object]]) -> list[str]:
    changes: list[str] = []
    for file in files:
        filename = str(file["filename"])
        paths = [filename]
        if file.get("previous_filename"):
            paths.append(str(file["previous_filename"]))
        if any(
            fnmatch.fnmatchcase(path, pattern)
            for path in paths
            for pattern in PROTECTED_PATHS
        ):
            changes.append(filename)
            continue
        patch = file.get("patch")
        if not isinstance(patch, str):
            if (
                filename.endswith(REVIEWABLE_SUFFIXES)
                and file.get("status") != "removed"
            ):
                changes.append(filename)
            continue
        additions = (
            line[1:]
            for line in patch.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        )
        if any(SUPPRESSION.search(line) for line in additions):
            changes.append(filename)
    return sorted(set(changes))


def is_security_maintainer(permission: dict[str, object]) -> bool:
    return permission.get("permission") == "admin" or permission.get("role_name") in {
        "admin",
        "maintain",
    }


def github_request(path: str) -> object:
    connection = http.client.HTTPSConnection("api.github.com", timeout=30)
    try:
        connection.request(
            "GET",
            path,
            headers={
                "Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "ukam-security-policy",
            },
        )
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"GitHub policy request failed ({response.status}).")
        return json.load(response)
    finally:
        connection.close()


def main() -> int:
    repository = os.environ["GITHUB_REPOSITORY"]
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as event_file:
        event = json.load(event_file)
    pull_request = event["pull_request"]
    number = pull_request["number"]
    prefix = f"/repos/{repository}"
    current = github_request(f"{prefix}/pulls/{number}")
    if not isinstance(current, dict):
        raise ValueError("Unexpected pull request response.")
    if current["head"]["sha"] != pull_request["head"]["sha"]:
        raise ValueError(
            "The PR changed during validation; rerun for the current commit."
        )
    if current["changed_files"] > 3000:
        raise ValueError("Split this PR: GitHub cannot enumerate more than 3000 files.")

    files: list[dict[str, object]] = []
    for page in range(1, 31):
        response = github_request(
            f"{prefix}/pulls/{number}/files?per_page=100&page={page}"
        )
        if not isinstance(response, list):
            raise ValueError("Unexpected changed-files response.")
        files.extend(response)
        if len(response) < 100:
            break
    if len(files) != current["changed_files"]:
        raise ValueError("The complete PR diff could not be verified.")

    changes = protected_changes(files)
    if changes and not is_dependabot_action_update(current, files, repository):
        author = current["user"]["login"]
        for login in {author, event["sender"]["login"]}:
            permission = github_request(f"{prefix}/collaborators/{login}/permission")
            if not isinstance(permission, dict) or not is_security_maintainer(
                permission
            ):
                sys.stderr.write(
                    "Security controls are maintained exclusively by maintainers.\n"
                    "Remove these changes and ask a maintainer for a separate "
                    "control-update PR:\n" + "\n".join(changes) + "\n"
                )
                return 1
    latest = github_request(f"{prefix}/pulls/{number}")
    if not isinstance(latest, dict) or latest["head"]["sha"] != current["head"]["sha"]:
        raise ValueError(
            "The PR changed during validation; rerun for the current commit."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
