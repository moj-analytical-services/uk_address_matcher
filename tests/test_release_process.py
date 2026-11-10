from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def workflow_scripts() -> dict[str, str]:
    directory = Path(__file__).parents[1] / ".github" / "workflows"
    candidate = yaml.load(
        (directory / "release-candidate.yml").read_text(), Loader=yaml.BaseLoader
    )
    publisher = yaml.load(
        (directory / "pypi-release.yaml").read_text(), Loader=yaml.BaseLoader
    )
    return {
        "proposal": candidate["jobs"]["proposal"]["steps"][0]["with"]["script"],
        "verified": candidate["jobs"]["verified"]["steps"][0]["with"]["script"],
        "publish": publisher["jobs"]["publish"]["steps"][0]["with"]["script"],
    }


def run_workflow_script(script: str, scenario: dict[str, object]) -> dict:
    if shutil.which("node") is None:
        pytest.skip("Node is required for workflow JavaScript")
    harness = """
        const {script, scenario} = JSON.parse(process.argv[1]);
        const outputs = {};
        const context = {
            repo: {owner: 'example', repo: 'repository'}, sha: 'tested',
            payload: {repository: {full_name: 'example/repository'}}
        };
        process.env.RELEASE_TAG = 'v9.1.0rc1';
        process.env.VALIDATION_RESULTS = JSON.stringify(scenario.gates ?? {
            proposal: {result: 'success'}, pytest: {result: 'success'},
            build: {result: 'success'}, labelling: {result: 'success'},
            security: {result: 'success'}
        });
        const core = {setOutput: (name, value) => { outputs[name] = value; }};
        const github = {rest: {
            repos: {
                getReleaseByTag: async () => ({data: {
                    draft: scenario.draft ?? true, target_commitish: 'tested'
                }}),
                getCommit: async () => ({data: {sha: scenario.sha ?? 'tested'}})
            },
            actions: {listWorkflowRuns: async query => {
                if (query.head_sha !== 'tested' ||
                    query.workflow_id !== 'release-candidate.yml' ||
                    query.event !== 'workflow_dispatch') throw Error('Wrong query');
                return {data: {workflow_runs: scenario.missing ? [] : [{
                    id: 50, run_attempt: 2, status: 'completed',
                    conclusion: 'success', head_sha: 'tested',
                    head_branch: 'build/example',
                    head_repository: {full_name: 'example/repository'},
                    ...scenario.run
                }]}};
            }}
        }};
        const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
        new AsyncFunction('github', 'context', 'core', script)(github, context, core)
            .then(() => console.log(JSON.stringify({outputs})))
            .catch(error => console.log(JSON.stringify({error: error.message})));
    """
    result = subprocess.run(
        ["node", "-e", harness, json.dumps({"script": script, "scenario": scenario})],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_draft_validation_and_selection_of_tested_distributions(
    workflow_scripts: dict[str, str],
) -> None:
    assert run_workflow_script(workflow_scripts["proposal"], {}) == {
        "outputs": {"target_sha": "tested"}
    }
    assert run_workflow_script(workflow_scripts["verified"], {}) == {"outputs": {}}
    assert run_workflow_script(workflow_scripts["publish"], {}) == {
        "outputs": {"run_id": 50, "artifact_name": "dist-2"}
    }


@pytest.mark.parametrize("scenario", [{"draft": False}, {"sha": "changed"}])
def test_candidate_rejects_published_or_changed_draft(
    workflow_scripts: dict[str, str], scenario: dict[str, object]
) -> None:
    assert "error" in run_workflow_script(workflow_scripts["proposal"], scenario)


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_candidate_requires_every_gate_to_succeed(
    workflow_scripts: dict[str, str], result: str
) -> None:
    assert "error" in run_workflow_script(
        workflow_scripts["verified"], {"gates": {"security": {"result": result}}}
    )


@pytest.mark.parametrize(
    "scenario",
    [
        {"missing": True},
        {"run": {"conclusion": "failure"}},
        {"run": {"status": "in_progress"}},
        {"run": {"head_sha": "changed"}},
        {"run": {"head_branch": "untrusted"}},
        {"run": {"head_repository": {"full_name": "outside/repository"}}},
    ],
)
def test_publisher_requires_successful_validation_of_the_published_commit(
    workflow_scripts: dict[str, str], scenario: dict[str, object]
) -> None:
    assert "error" in run_workflow_script(workflow_scripts["publish"], scenario)
