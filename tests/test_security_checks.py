from __future__ import annotations

import json
import re
import secrets
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from dev_scripts.check_secrets import main
from security import check_security_policy
from security.check_security_policy import is_security_maintainer, protected_changes


@pytest.mark.parametrize("returncode", [0, 1, 3])
def test_secrets_check_does_not_modify_maintained_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, returncode: int
) -> None:
    baseline = tmp_path / "secrets.baseline"
    original_contents = '{"results": {}}\n'
    baseline.write_text(original_contents, encoding="utf-8")
    temporary_baselines: list[Path] = []

    def run_hook(
        command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        temporary_baseline = Path(command[command.index("--baseline") + 1])
        temporary_baselines.append(temporary_baseline)
        assert temporary_baseline != baseline
        assert temporary_baseline.read_text(encoding="utf-8") == original_contents
        assert command[-1] == "example.py"
        temporary_baseline.write_text("updated metadata", encoding="utf-8")
        return subprocess.CompletedProcess(command, returncode, stdout="", stderr="")

    monkeypatch.setattr("dev_scripts.check_secrets.subprocess.run", run_hook)

    assert main(["--baseline", str(baseline), "example.py"]) == (
        0 if returncode == 3 else returncode
    )
    assert baseline.read_text(encoding="utf-8") == original_contents
    assert not temporary_baselines[0].exists()


def test_secrets_check_requires_maintained_baseline(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        main(["--baseline", str(tmp_path / "missing.baseline"), "example.py"])
    assert error.value.code == 2


@pytest.mark.parametrize(
    "filename",
    [
        "security/secrets.baseline",
        "security/pre-commit-config.yaml",
        "security/nested/exception.json",
        ".github/workflows/security.yml",
        ".github/CODEOWNERS",
        "security/check_security_policy.py",
        "dev_scripts/check_secrets.py",
        "pyproject.toml",
        "uv.lock",
        "frontend/labelling/package.json",
        "frontend/labelling/package-lock.json",
        "frontend/labelling/vite.config.js",
        "frontend/labelling/.npmrc",
    ],
)
def test_policy_protects_security_controls(filename: str) -> None:
    assert protected_changes([{"filename": filename}]) == [filename]


def test_policy_protects_renamed_controls() -> None:
    assert protected_changes(
        [{"filename": "elsewhere.json", "previous_filename": "security/secrets.baseline"}]
    ) == ["elsewhere.json"]


@pytest.mark.parametrize(
    "filename",
    [
        "labelling.yml",
        "docs.yml",
        "test-build-package.yaml",
        "security.yml",
        "run_pytest.yml",
    ],
)
def test_frontend_workflows_use_relocated_project(filename: str) -> None:
    workflow_path = Path(__file__).parents[1] / ".github" / "workflows" / filename
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            if "npm " in step.get("run", ""):
                assert step["working-directory"] == "frontend/labelling"
    for event in workflow["on"].values():
        if isinstance(event, dict) and "paths" in event:
            assert "frontend/labelling/**" in event["paths"]


@pytest.mark.parametrize(
    ("ecosystem", "directory"),
    [("uv", "/"), ("npm", "/frontend/labelling"), ("github-actions", "/")],
)
def test_dependabot_tracks_dependencies_with_cooldowns(
    ecosystem: str, directory: str
) -> None:
    config_path = Path(__file__).parents[1] / ".github" / "dependabot.yml"
    config = yaml.load(config_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    update = next(
        update for update in config["updates"] if update["package-ecosystem"] == ecosystem
    )
    assert update["directory"] == directory
    assert update["cooldown"] == {"default-days": "7"}
    assert update["schedule"] == {"interval": "cron", "cronjob": "0 9 1,15 * *"}
    assert "reviewers" not in update
    assert int(update["open-pull-requests-limit"]) > 0
    if ecosystem == "uv":
        assert update["allow"] == [{"dependency-type": "all"}]


@pytest.mark.parametrize(
    "addition",
    [
        "+dangerous_call()  # nosec B608",
        "+value = 'placeholder'  # pragma: allowlist secret",
        "+# pragma: allowlist nextline secret",
        "+# zizmor: ignore[dangerous-triggers]",
        "+pip-audit --ignore-vuln EXAMPLE-ADVISORY",
        "+SKIP=detect-secrets pre-commit run",
    ],
)
def test_policy_requires_review_for_new_suppressions(addition: str) -> None:
    assert protected_changes([{"filename": "example.py", "patch": addition}]) == [
        "example.py"
    ]


def test_policy_allows_ordinary_code_and_removed_suppressions() -> None:
    assert (
        protected_changes(
            [
                {
                    "filename": "uk_address_matcher/example.py",
                    "patch": "-old_call()  # nosec\n+safe_call()",
                }
            ]
        )
        == []
    )


def test_policy_requires_review_when_source_patch_is_unavailable() -> None:
    assert protected_changes([{"filename": "example.py", "status": "modified"}]) == [
        "example.py"
    ]


@pytest.mark.parametrize(
    ("permission", "expected"),
    [
        ({"permission": "admin", "role_name": "admin"}, True),
        ({"permission": "write", "role_name": "maintain"}, True),
        ({"permission": "write", "role_name": "write"}, False),
        ({"permission": "read", "role_name": "read"}, False),
        ({}, False),
    ],
)
def test_only_administrators_and_maintainers_can_change_controls(
    permission: dict[str, object], expected: bool
) -> None:
    assert is_security_maintainer(permission) is expected


@pytest.fixture
def dependabot_action_pr() -> dict[str, object]:
    return {
        "user": {"login": "dependabot[bot]", "id": 49699333, "type": "Bot"},
        "head": {
            "ref": "dependabot/github_actions/actions/checkout-7.0.1",
            "repo": {"full_name": "example/repository"},
            "sha": "current",
        },
        "changed_files": 1,
    }


@pytest.fixture
def action_pin_file() -> dict[str, object]:
    return {
        "filename": ".github/workflows/security-policy.yml",
        "status": "modified",
        "additions": 1,
        "deletions": 1,
        "patch": (
            "@@ -1,3 +1,3 @@\n"
            " steps:\n"
            f"-  - uses: actions/checkout@{'a' * 40} # v6.0.2\n"
            f"+  - uses: actions/checkout@{'b' * 40} # v7.0.1\n"
            "     with:\n"
        ),
    }


def test_policy_allows_only_dependabot_action_pins(
    dependabot_action_pr: dict[str, object], action_pin_file: dict[str, object]
) -> None:
    assert check_security_policy.is_dependabot_action_update(
        dependabot_action_pr, [action_pin_file], "example/repository"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filename", "security/check_security_policy.py"),
        ("status", "added"),
        ("previous_filename", ".github/workflows/other.yml"),
        ("patch", None),
        ("additions", 2),
        ("deletions", 2),
    ],
)
def test_dependabot_exception_rejects_other_or_incomplete_changes(
    dependabot_action_pr: dict[str, object],
    action_pin_file: dict[str, object],
    field: str,
    value: object,
) -> None:
    action_pin_file[field] = value
    assert not check_security_policy.is_dependabot_action_update(
        dependabot_action_pr, [action_pin_file], "example/repository"
    )


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        ("+  - uses: actions/checkout@", "+  - uses: other/checkout@"),
        ("+  - uses:", "+    - uses:"),
        ("b" * 40, "v7.0.1"),
        ("# v7.0.1", "# zizmor: ignore[dangerous-triggers]"),
        ("+  - uses: actions/checkout@", "+  - run: echo "),
        ("+  - uses: actions/checkout@", "+    permissions: "),
        ("     with:", f"+  - uses: actions/checkout@{'c' * 40}"),
    ],
)
def test_dependabot_exception_rejects_action_or_structure_changes(
    dependabot_action_pr: dict[str, object],
    action_pin_file: dict[str, object],
    original: str,
    replacement: str,
) -> None:
    action_pin_file["patch"] = str(action_pin_file["patch"]).replace(
        original, replacement
    )
    assert not check_security_policy.is_dependabot_action_update(
        dependabot_action_pr, [action_pin_file], "example/repository"
    )


@pytest.mark.parametrize(
    "user",
    [
        {"login": "dependabot[bot]", "id": 123, "type": "Bot"},
        {"login": "other[bot]", "id": 49699333, "type": "Bot"},
        {"login": "dependabot[bot]", "id": 49699333, "type": "User"},
    ],
)
def test_dependabot_exception_verifies_bot_identity(
    dependabot_action_pr: dict[str, object],
    action_pin_file: dict[str, object],
    user: dict[str, object],
) -> None:
    dependabot_action_pr["user"] = user
    assert not check_security_policy.is_dependabot_action_update(
        dependabot_action_pr, [action_pin_file], "example/repository"
    )


def test_dependabot_exception_rejects_forks_and_mixed_changes(
    dependabot_action_pr: dict[str, object], action_pin_file: dict[str, object]
) -> None:
    assert not check_security_policy.is_dependabot_action_update(
        dependabot_action_pr, [action_pin_file], "other/repository"
    )
    assert not check_security_policy.is_dependabot_action_update(
        dependabot_action_pr,
        [action_pin_file, {"filename": "security/secrets.baseline"}],
        "example/repository",
    )


def test_policy_accepts_dependabot_pin_updates_without_granting_a_role(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dependabot_action_pr: dict[str, object],
    action_pin_file: dict[str, object],
) -> None:
    event_file = tmp_path / "event.json"
    event_file.write_text(
        json.dumps({"pull_request": {"number": 12, "head": {"sha": "current"}}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repository")

    def request(path: str) -> object:
        assert "/collaborators/" not in path
        if "/files?" in path:
            return [action_pin_file]
        return dependabot_action_pr

    monkeypatch.setattr(check_security_policy, "github_request", request)
    assert check_security_policy.main() == 0


def test_real_secret_hook_rejects_new_secret_without_updating_baseline(
    tmp_path: Path,
) -> None:
    pytest.importorskip("detect_secrets")
    maintained_baseline = Path(__file__).parents[1] / "security" / "secrets.baseline"
    baseline_contents = maintained_baseline.read_bytes()
    candidate = tmp_path / "new_secret.py"
    candidate.write_text(f'password = "{secrets.token_hex(32)}"\n', encoding="utf-8")

    assert main(["--baseline", str(maintained_baseline), str(candidate)]) == 1
    assert maintained_baseline.read_bytes() == baseline_contents


@pytest.mark.parametrize(
    ("author_role", "actor_role", "expected"),
    [("maintain", "maintain", 0), ("write", "maintain", 1), ("admin", "write", 1)],
)
def test_policy_checks_author_and_triggering_actor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    author_role: str,
    actor_role: str,
    expected: int,
) -> None:
    event_file = tmp_path / "event.json"
    event_file.write_text(
        json.dumps(
            {
                "pull_request": {"number": 12, "head": {"sha": "current"}},
                "sender": {"login": "pusher"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repository")

    def request(path: str) -> object:
        if "/files?" in path:
            return [{"filename": "security/secrets.baseline", "patch": "+change"}]
        if "/collaborators/" in path:
            role = actor_role if "/pusher/" in path else author_role
            return {"permission": "write", "role_name": role}
        return {
            "head": {"sha": "current"},
            "changed_files": 1,
            "user": {"login": "author"},
        }

    monkeypatch.setattr(check_security_policy, "github_request", request)
    assert check_security_policy.main() == expected


@pytest.mark.parametrize("changed_files", [1, 3001])
def test_policy_fails_closed_on_incomplete_or_oversized_diffs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed_files: int
) -> None:
    event_file = tmp_path / "event.json"
    event_file.write_text(
        json.dumps({"pull_request": {"number": 12, "head": {"sha": "current"}}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repository")

    def request(path: str) -> object:
        if "/files?" in path:
            return []
        return {"head": {"sha": "current"}, "changed_files": changed_files}

    monkeypatch.setattr(check_security_policy, "github_request", request)
    with pytest.raises(ValueError):
        check_security_policy.main()


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("uk_address_matcher/cleaning/example.py", True),
        ("benchmarking/runner.py", True),
        ("tests/test_security_checks.py", True),
        ("dev_scripts/check_secrets.py", False),
        ("security/check_security_policy.py", False),
        (".github/workflows/run_labels_accuracy_test.py", False),
        ("examples/example_matching.py", False),
        ("uk_address_matcher_extra/example.py", False),
    ],
)
def test_bandit_scans_only_core_python_directories(filename: str, expected: bool) -> None:
    config_path = Path(__file__).parents[1] / "security" / "pre-commit-config.yaml"
    config = yaml.load(config_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    hook = next(hook for hook in config["repos"][0]["hooks"] if hook["id"] == "bandit")
    assert bool(re.search(hook["files"], filename)) is expected
    assert hook["types"] == ["python"]


def test_ci_temporarily_skips_only_bandit() -> None:
    workflow_path = Path(__file__).parents[1] / ".github" / "workflows" / "security.yml"
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    steps = workflow["jobs"]["security"]["steps"]
    scan = next(
        step for step in steps if "pre-commit run --all-files" in step.get("run", "")
    )
    assert scan["run"] == (
        "uv run --no-sync pre-commit run --all-files "
        "--config security/pre-commit-config.yaml"
    )
    assert scan["env"] == {"SKIP": "bandit"}


@pytest.mark.parametrize(
    "workflow_path",
    sorted((Path(__file__).parents[1] / ".github" / "workflows").glob("*.y*ml")),
    ids=lambda path: path.name,
)
def test_workflows_enforce_actions_security_policy(workflow_path: Path) -> None:
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    assert workflow["permissions"] == {}
    assert not {"pull_request_target", "workflow_run"}.intersection(workflow["on"])
    allowed_writes = {
        ("docs.yml", "deploy"): {"pages", "id-token"},
        ("pypi-release.yaml", "publish"): {"id-token"},
        ("edge_cases_publish_comment.yml", "comment"): {"pull-requests"},
    }
    for name, job in workflow["jobs"].items():
        assert isinstance(job.get("permissions"), dict)
        for scope, permission in job["permissions"].items():
            assert permission == "read" or (
                permission == "write"
                and scope in allowed_writes.get((workflow_path.name, name), set())
            )
        for action in [job, *job.get("steps", [])]:
            reference = action.get("uses", "")
            if reference and not reference.startswith("./"):
                assert re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference)
            assert not reference.startswith("actions/cache@")
            settings = action.get("with", {})
            if reference.startswith("astral-sh/setup-uv@"):
                assert settings.get("enable-cache") == "false"
            if reference.startswith("actions/setup-node@"):
                assert settings.get("package-manager-cache") == "false"
                assert not settings.get("cache")
            if reference.startswith("actions/setup-python@"):
                assert not settings.get("cache")


def test_workflow_scanner_has_no_control_exemptions() -> None:
    config_path = Path(__file__).parents[1] / "security" / "zizmor.yml"
    config = yaml.load(config_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    for rule in ["dangerous-triggers", "unpinned-uses", "excessive-permissions"]:
        assert not config.get("rules", {}).get(rule, {}).get("ignore")


def test_edge_case_comment_publication_requires_manual_main_dispatch() -> None:
    workflow_path = (
        Path(__file__).parents[1]
        / ".github"
        / "workflows"
        / "edge_cases_publish_comment.yml"
    )
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["on"]["workflow_dispatch"]["inputs"]["run_id"]["required"] == "true"
    job = workflow["jobs"]["comment"]
    assert job["if"] == "github.ref == 'refs/heads/main'"
    assert all("actions/checkout@" not in step.get("uses", "") for step in job["steps"])
    validation = job["steps"][0]["with"]["script"]
    assert "Number.isSafeInteger(runId)" in validation
    assert "github.rest.actions.getWorkflowRun" in validation
    assert "run.path !== '.github/workflows/edge_cases_github_comment.yml'" in validation
    assert "pr.head.sha !== run.head_sha" in validation
    assert job["steps"][1]["with"]["run-id"] == "${{ steps.validate.outputs.run_id }}"


@pytest.mark.skipif(
    shutil.which("node") is None, reason="Node is required for workflow JS"
)
@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        ({}, "valid"),
        ({"run_id": "1; process.exit(0)"}, "failed"),
        ({"path": ".github/workflows/other.yml"}, "failed"),
        ({"status": "in_progress"}, "failed"),
        ({"event": "push"}, "failed"),
        ({"pr_sha": "stale"}, "skipped"),
        ({"size": 100001}, "failed"),
        ({"repository": "outside/repository"}, "skipped"),
    ],
)
def test_manual_comment_validator_rejects_untrusted_runs(
    scenario: dict[str, object], expected: str
) -> None:
    workflow_path = (
        Path(__file__).parents[1]
        / ".github"
        / "workflows"
        / "edge_cases_publish_comment.yml"
    )
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    script = workflow["jobs"]["comment"]["steps"][0]["with"]["script"]
    harness = """
        const { script, scenario } = JSON.parse(process.argv[1]);
        process.env.RUN_ID = scenario.run_id ?? '123';
        const outputs = {};
        const failures = [];
        const context = {repo: {owner: 'example', repo: 'repository'}};
        const core = {
            info: () => {},
            setFailed: message => failures.push(message),
            setOutput: (name, value) => { outputs[name] = value; },
        };
        const github = {rest: {
            actions: {
                getWorkflowRun: async () => ({data: {
                    event: scenario.event ?? 'pull_request',
                    status: scenario.status ?? 'completed',
                    path: scenario.path ??
                        '.github/workflows/edge_cases_github_comment.yml',
                    repository: {full_name: scenario.repository ?? 'example/repository'},
                    pull_requests: [{number: 17}],
                    head_sha: 'current',
                }}),
                listWorkflowRunArtifacts: async () => ({data: {artifacts: [{
                    name: 'edge-cases-comment', id: 23, expired: false,
                    size_in_bytes: scenario.size ?? 500,
                }]}}),
            },
            pulls: {get: async () => ({data: {
                state: 'open', head: {sha: scenario.pr_sha ?? 'current'},
            }})},
        }};
        const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
        new AsyncFunction('github', 'context', 'core', script)(github, context, core)
            .then(() => console.log(JSON.stringify({outputs, failures})))
            .catch(error => { console.error(error); process.exitCode = 1; });
    """
    result = subprocess.run(
        ["node", "-e", harness, json.dumps({"script": script, "scenario": scenario})],
        capture_output=True,
        text=True,
        check=True,
    )
    outcome = json.loads(result.stdout)
    if expected == "valid":
        assert outcome == {
            "outputs": {
                "number": "17",
                "artifact_id": "23",
                "run_id": "123",
                "head_sha": "current",
            },
            "failures": [],
        }
    else:
        assert outcome["outputs"] == {}
        assert bool(outcome["failures"]) is (expected == "failed")


def test_trusted_policy_workflow_never_checks_out_pr_head() -> None:
    workflow_path = (
        Path(__file__).parents[1] / ".github" / "workflows" / "security-policy.yml"
    )
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    assert set(workflow["on"]) == {"pull_request"}
    assert workflow["permissions"] == {}
    assert workflow["jobs"]["policy"]["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
    }
    steps = workflow["jobs"]["policy"]["steps"]
    assert steps[0]["with"]["ref"] == "${{ github.event.pull_request.base.sha }}"
    assert steps[0]["with"]["persist-credentials"] == "false"
    assert steps[-1]["run"] == "python -I security/check_security_policy.py"


def test_private_labels_workflow_does_not_run_pr_code() -> None:
    workflow_path = (
        Path(__file__).parents[1]
        / ".github"
        / "workflows"
        / "accuracy_from_labels_dataset.yml"
    )
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    assert set(workflow["on"]) == {"push", "workflow_dispatch"}
    job = workflow["jobs"]["run-address-matcher"]
    assert job["if"] == "github.ref == 'refs/heads/main'"
    assert job["environment"] == "private-labels"


def test_publisher_only_publishes_verified_artefacts() -> None:
    workflow_path = (
        Path(__file__).parents[1] / ".github" / "workflows" / "pypi-release.yaml"
    )
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )
    assert workflow["on"] == {"release": {"types": ["published"]}}
    job = workflow["jobs"]["publish"]
    assert job["permissions"]["id-token"] == "write"
    steps = job["steps"]
    assert all("actions/checkout@" not in step.get("uses", "") for step in steps)
    assert [step["run"] for step in steps if "run" in step] == [
        "uv publish --trusted-publishing always "
        "--check-url https://pypi.org/simple/ dist/*"
    ]
    assert "head_sha: context.sha" in steps[0]["with"]["script"]
    assert any(
        "actions/download-artifact@" in step.get("uses", "")
        and step["with"]["name"] == "${{ steps.candidate.outputs.artifact_name }}"
        for step in steps
    )
