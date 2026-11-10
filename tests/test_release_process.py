from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest


@pytest.fixture
def release():
    path = Path(__file__).parents[1] / "scripts" / "release.py"
    spec = importlib.util.spec_from_file_location("release_process", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def distributions(tmp_path):
    directory = tmp_path / "dist"
    directory.mkdir()
    metadata = b"Metadata-Version: 2.1\nName: uk-address-matcher\nVersion: 2.0.0.dev2\n"
    with zipfile.ZipFile(
        directory / "uk_address_matcher-2.0.0.dev2-py3-none-any.whl", "w"
    ) as archive:
        archive.writestr("uk_address_matcher-2.0.0.dev2.dist-info/METADATA", metadata)
    with tarfile.open(
        directory / "uk_address_matcher-2.0.0.dev2.tar.gz", "w:gz"
    ) as archive:
        member = tarfile.TarInfo("uk_address_matcher-2.0.0.dev2/PKG-INFO")
        member.size = len(metadata)
        archive.addfile(member, io.BytesIO(metadata))
    return directory


def test_distribution_versions_and_content_are_bound(release, distributions):
    files = release.distribution_files(distributions, "v2.0.0.dev2")
    assert len(files) == 2
    assert all(len(digest) == 64 for digest in files.values())
    with pytest.raises(ValueError):
        release.distribution_files(distributions, "v2.0.0.dev3")
    (distributions / "unexpected.txt").touch()
    with pytest.raises(ValueError):
        release.distribution_files(distributions, "v2.0.0.dev2")


def test_pypi_recovery_accepts_only_original_files(release, monkeypatch):
    monkeypatch.setattr(
        release, "pypi_files", lambda version: {"wheel.whl": "original"}
    )
    version = release.Version("2.0.0.dev2")
    files = {"wheel.whl": "original", "source.tar.gz": "source"}
    release.check_pypi(version, files)
    with pytest.raises(ValueError, match="both verified"):
        release.check_pypi(version, files, complete=True)
    with pytest.raises(ValueError, match="different files"):
        release.check_pypi(version, {"wheel.whl": "changed"})


def test_draft_binding_and_published_release_rejection(release, monkeypatch):
    sha = "a" * 40
    draft = {
        "tag_name": "v2.0.0.dev2",
        "target_commitish": sha,
        "prerelease": True,
        "draft": True,
    }
    monkeypatch.setattr(release, "tag_commit", lambda tag: None)
    release.check_release(draft, sha, True)
    with pytest.raises(ValueError, match="target SHA"):
        release.check_release(draft, "b" * 40, True)
    with pytest.raises(ValueError, match="still be a draft"):
        release.check_release(dict(draft, draft=False), sha, True)
    monkeypatch.setattr(release, "tag_commit", lambda tag: "b" * 40)
    with pytest.raises(ValueError, match="different commit"):
        release.check_release(draft, sha, True)


@pytest.fixture
def candidate(release, distributions, monkeypatch):
    monkeypatch.chdir(distributions.parent)
    sha = "a" * 40
    gates = {
        name: {"result": "success"}
        for name in ("run-pytest", "test-build-package", "test-labelling", "security")
    }
    for name, value in {
        "RELEASE_TAG": "v2.0.0.dev2",
        "TARGET_SHA": sha,
        "PRERELEASE": "true",
        "SOURCE_BRANCH": "build/ukam_2.0",
        "VALIDATION_RESULTS": json.dumps(gates),
        "GITHUB_RUN_ID": "50",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_SHA": "b" * 40,
        "DIST_ARTIFACT_ID": "20",
    }.items():
        monkeypatch.setenv(name, value)
    draft = {
        "id": 1,
        "tag_name": "v2.0.0.dev2",
        "target_commitish": sha,
        "prerelease": True,
        "draft": True,
    }
    state = {
        "run": {
            "path": ".github/workflows/release-candidate.yml",
            "head_branch": "build/ukam_2.0",
            "head_sha": "b" * 40,
            "event": "push",
            "run_attempt": 1,
        },
        "draft": draft,
        "artifacts": [
            {"id": 10, "name": "release-manifest-1", "expired": False},
            {"id": 20, "name": "dist-1", "expired": False},
        ],
        "attempt": {"conclusion": "success"},
    }

    def api(path, **kwargs):
        if path == "releases?per_page=100":
            return [draft]
        if path.startswith("git/matching-refs/"):
            return []
        if path == "actions/artifacts/20":
            return {"name": "dist-1", "expired": False, "workflow_run": {"id": 50}}
        if path == "actions/runs/50":
            return state["run"]
        if "/artifacts?" in path:
            return state["artifacts"]
        if "/attempts/" in path:
            return state["attempt"]
        raise AssertionError(path)

    monkeypatch.setattr(release, "api", api)
    release.candidate()
    state["manifest"] = json.loads(Path("release-manifest.json").read_text())
    state["files"] = {path.name: path.read_bytes() for path in distributions.iterdir()}
    for path in distributions.iterdir():
        path.unlink()

    def download(artifact_id):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            if artifact_id == 10:
                archive.writestr("release-manifest.json", json.dumps(state["manifest"]))
            else:
                assert artifact_id == 20
                for name, content in state["files"].items():
                    archive.writestr(name, content)
        buffer.seek(0)
        return zipfile.ZipFile(buffer)

    monkeypatch.setattr(release, "download", download)
    return state


@pytest.mark.parametrize(
    "problem",
    [
        "none", "workflow", "branch", "event", "sha", "gate", "run",
        "expired", "published", "tampered",
    ],
)
def test_publisher_binds_the_run_draft_and_original_distributions(
    release, candidate, problem
):
    if problem == "workflow":
        candidate["run"]["path"] = ".github/workflows/unrelated.yml"
    elif problem == "branch":
        candidate["run"]["head_branch"] = "feature/untrusted"
    elif problem == "event":
        candidate["run"]["event"] = "pull_request"
    elif problem == "sha":
        candidate["draft"]["target_commitish"] = "c" * 40
    elif problem == "gate":
        candidate["manifest"]["gates"]["security"] = "skipped"
    elif problem == "run":
        candidate["manifest"]["run_id"] = 99
    elif problem == "expired":
        candidate["artifacts"][1]["expired"] = True
    elif problem == "published":
        candidate["draft"]["draft"] = False
    elif problem == "tampered":
        name = next(name for name in candidate["files"] if name.endswith(".whl"))
        candidate["files"][name] += b"changed"
    if problem == "none":
        manifest = release.verify("v2.0.0.dev2", 50, None)
        assert manifest["files"] == release.distribution_files(
            Path("dist"), manifest["tag"]
        )
    else:
        with pytest.raises(ValueError):
            release.verify("v2.0.0.dev2", 50, None)


def test_recovery_can_select_the_original_verified_attempt(release, candidate):
    candidate["run"]["head_branch"] = "main"
    candidate["run"]["run_attempt"] = 2
    manifest = release.verify("v2.0.0.dev2", 50, 10)
    assert manifest["run_attempt"] == 1
    assert manifest["artifact_id"] == 20


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_candidate_rejects_non_successful_gates(
    release, candidate, monkeypatch, result
):
    gates = {name: {"result": "success"} for name in candidate["manifest"]["gates"]}
    gates["security"]["result"] = result
    monkeypatch.setenv("VALIDATION_RESULTS", json.dumps(gates))
    with pytest.raises(ValueError, match="must succeed"):
        release.candidate()


def test_prepare_pins_branch_and_retains_draft_on_request_failure(
    release, monkeypatch
):
    calls = []
    monkeypatch.setattr(release, "tag_commit", lambda tag: None)
    monkeypatch.setattr(release, "pypi_files", lambda version: {})
    monkeypatch.setattr(
        release,
        "api",
        lambda path, **kwargs: (
            {"commit": {"sha": "a" * 40}} if path.startswith("branches/")
            else {} if path.startswith("contents/") else []
        ),
    )

    def gh(*args, **kwargs):
        calls.append(args)
        if args[:3] == ("api", "--method", "PUT"):
            raise subprocess.CalledProcessError(1, args)
        return b"draft-url"

    monkeypatch.setattr(release, "gh", gh)
    with pytest.raises(subprocess.CalledProcessError):
        release.prepare("2.0.0.dev2", "build/ukam_2.0", True)
    create = next(args for args in calls if args[:2] == ("release", "create"))
    assert create[create.index("--target") + 1] == "a" * 40
    assert "--draft" in create and "--prerelease" in create
    assert calls[-1][:3] == ("api", "--method", "PUT")
    assert not any("delete" in args for args in calls)


def test_branch_requests_pin_source_and_keep_publishing_separate(
    release, monkeypatch, tmp_path
):
    branch = "build/ukam_2.0"
    sha = "a" * 40
    requests = []
    draft = []
    monkeypatch.setattr(release, "pypi_files", lambda version: {})

    def gh(*args, **kwargs):
        if args[:2] == ("release", "create"):
            draft.append({
                "tag_name": "v2.0.0.dev2", "target_commitish": sha,
                "prerelease": True, "draft": True,
            })
            return b"draft-url"
        if args[:3] == ("api", "--method", "PUT"):
            payload = json.loads(kwargs["input"])
            requests.append((args[3], payload))
            return b"{}"
        if args[0] == "api":
            path = args[1].removeprefix(f"repos/{release.REPOSITORY}/")
            if path.startswith("branches/"):
                return json.dumps({"commit": {"sha": sha}}).encode()
            if path.startswith("releases?"):
                return json.dumps([draft] if "--slurp" in args else draft).encode()
            if path.startswith("git/matching-refs/"):
                return b"[]"
            if path.startswith("contents/"):
                if "release-candidate.json" in path:
                    raise subprocess.CalledProcessError(
                        1, args, output=b'{"status":"404"}'
                    )
                return b'{"sha":"previous-publish-request"}'
            if path.startswith("compare/"):
                return b'{"status":"ahead"}'
            raise AssertionError(path)
        assert args == ("auth", "status")
        return b""

    monkeypatch.setattr(release, "gh", gh)
    release.prepare("2.0.0.dev2", branch, True)
    assert len(requests) == 1
    path, payload = requests[0]
    assert path.endswith("/.github/release-candidate.json")
    assert payload["branch"] == branch and "sha" not in payload
    request = json.loads(base64.b64decode(payload["content"]))
    assert request["target_sha"] == sha
    assert request["source_branch"] == branch
    assert request["prerelease"] is True

    monkeypatch.chdir(tmp_path)
    for name in (
        "RELEASE_TAG", "TARGET_SHA", "SOURCE_BRANCH", "PRERELEASE", "VALIDATION_RUN_ID"
    ):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github/release-candidate.json").write_text(json.dumps(request))
    monkeypatch.setenv("GITHUB_REF_NAME", branch)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))
    monkeypatch.setattr(release.sys, "argv", ["release.py", "proposal"])
    release.main()
    assert (tmp_path / "outputs").read_text() == f"target_sha={sha}\n"

    monkeypatch.setattr(release.sys, "argv", [
        "release.py", "request-publish", "--branch", branch,
        "--tag", "v2.0.0.dev2", "--run-id", "50",
    ])
    release.main()
    path, payload = requests[1]
    assert path.endswith("/.github/release-publish.json")
    assert payload["branch"] == branch
    assert payload["sha"] == "previous-publish-request"
    request = json.loads(base64.b64decode(payload["content"]))
    (tmp_path / ".github/release-publish.json").write_text(json.dumps(request))
    release.load_request("publish")
    assert os.environ["VALIDATION_RUN_ID"] == "50"
    assert os.environ["RELEASE_TAG"] == "v2.0.0.dev2"
    with pytest.raises(ValueError, match="trusted build branch"):
        release.push_request("publish", "feature/untrusted", request)
    assert len(requests) == 2


def test_finalisation_retains_draft_on_failure_and_reuses_original_files(
    release, candidate, monkeypatch
):
    manifest = release.verify("v2.0.0.dev2", 50, 10)
    calls = []
    fail_upload = [True]
    original_api = release.api
    draft = candidate["draft"]
    draft.update(assets=[], immutable=False)
    monkeypatch.setattr(release, "pypi_files", lambda version: manifest["files"])
    monkeypatch.setattr(
        release, "tag_commit", lambda tag: None if draft["draft"] else manifest["sha"]
    )
    monkeypatch.setattr(
        release,
        "api",
        lambda path, **kwargs: (
            draft if path == "releases/1" else original_api(path, **kwargs)
        ),
    )

    def gh(*args):
        calls.append(args)
        if args[:2] == ("release", "upload"):
            if fail_upload.pop() if fail_upload else False:
                raise subprocess.CalledProcessError(1, args)
            draft["assets"] = [
                {"id": index, "name": name}
                for index, name in enumerate(candidate["files"])
            ]
            return b""
        if args[:2] == ("release", "edit"):
            draft.update(draft=False, immutable=True)
            return b""
        asset_id = int(args[1].split("/")[-1])
        return candidate["files"][draft["assets"][asset_id]["name"]]

    monkeypatch.setattr(release, "gh", gh)
    with pytest.raises(subprocess.CalledProcessError):
        release.finalise(manifest)
    assert draft["draft"]
    assert not any(args[:2] == ("release", "edit") for args in calls)
    release.finalise(manifest)
    release.finalise(manifest)
    assert not draft["draft"] and draft["immutable"]
    assert sum(args[:2] == ("release", "upload") for args in calls) == 2
    assert sum(args[:2] == ("release", "edit") for args in calls) == 1
    assert not any("delete" in args for args in calls)


def test_prepare_script_defaults_and_explicit_prerelease(tmp_path):
    script = Path(__file__).parents[1] / "scripts" / "prepare-release.sh"
    tools = tmp_path / "tools"
    tools.mkdir()
    for tool in ("gh", "uv"):
        executable = tools / tool
        executable.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\"\n")
        executable.chmod(0o755)
    git = tools / "git"
    git.write_text("#!/usr/bin/env bash\nprintf '%s\\n' 'build/ukam_2.0'\n")
    git.chmod(0o755)
    result = subprocess.run(
        ["bash", str(script)],
        input="2.0.0\n\n\n",
        text=True,
        capture_output=True,
        env=dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}"),
        check=True,
    )
    assert "--branch\nbuild/ukam_2.0\n--prerelease\nno\n" in result.stdout


@pytest.mark.parametrize("collision", ["tag", "release", "pypi", "prerelease"])
def test_preparation_does_not_overwrite_existing_versions(
    release, monkeypatch, collision
):
    calls = []
    monkeypatch.setattr(release, "gh", lambda *args: calls.append(args) or b"")
    monkeypatch.setattr(
        release, "tag_commit", lambda tag: "a" * 40 if collision == "tag" else None
    )
    monkeypatch.setattr(
        release,
        "pypi_files",
        lambda version: {"existing.whl": "digest"} if collision == "pypi" else {},
    )
    monkeypatch.setattr(
        release,
        "api",
        lambda path, **kwargs: (
            {"commit": {"sha": "a" * 40}}
            if path.startswith("branches/")
            else [{"tag_name": "v2.0.0.dev2"}]
            if collision == "release"
            else []
        ),
    )
    with pytest.raises(ValueError):
        release.prepare("2.0.0.dev2", "main", collision != "prerelease")
    assert not any(
        args[:2] in (("release", "create"), ("workflow", "run")) for args in calls
    )
