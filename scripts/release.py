from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import os
import re
import shlex
import subprocess
import sys
import tarfile
import uuid
import zipfile
from email.parser import BytesParser
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

from packaging.utils import (
    canonicalize_name,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import Version

REPOSITORY = "moj-analytical-services/uk_address_matcher"


def gh(*arguments: str, **kwargs) -> bytes:
    return subprocess.check_output(["gh", *arguments], **kwargs)


def api(path: str, *, paginate: bool = False, collection: str | None = None):
    arguments = ["api", f"repos/{REPOSITORY}/{path}"]
    if paginate:
        arguments.extend(["--paginate", "--slurp"])
    result = json.loads(gh(*arguments))
    return (
        [item for page in result for item in (page[collection] if collection else page)]
        if paginate
        else result
    )


def normalise_version(value: str) -> tuple[str, Version]:
    version = Version(value.removeprefix("v"))
    if version.local:
        raise ValueError("Local versions cannot be published to PyPI.")
    return f"v{version}", version


def pypi_files(version: Version) -> dict[str, str]:
    try:
        with urlopen(
            f"https://pypi.org/pypi/uk-address-matcher/{version}/json", timeout=30
        ) as response:
            release = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return {}
        raise
    return {entry["filename"]: entry["digests"]["sha256"] for entry in release["urls"]}


def check_pypi(version: Version, files: dict[str, str], *, complete=False) -> None:
    published = pypi_files(version)
    if any(files.get(name) != digest for name, digest in published.items()):
        raise ValueError("PyPI already contains different files for this version.")
    if complete and published != files:
        raise ValueError("PyPI does not yet contain both verified distributions.")


def tag_commit(tag: str) -> str | None:
    matches = api(f"git/matching-refs/tags/{quote(tag, safe='')}")
    if not any(item["ref"] == f"refs/tags/{tag}" for item in matches):
        return None
    return api(f"commits/refs/tags/{quote(tag, safe='')}")["sha"]


def release_for(tag: str) -> dict:
    matches = [
        release
        for release in api("releases?per_page=100", paginate=True)
        if release["tag_name"] == tag
    ]
    if len(matches) != 1:
        raise ValueError("Expected exactly one release with the requested tag.")
    return matches[0]


def check_release(
    release: dict, sha: str, prerelease: bool, *, published=False
) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("The target must be an exact 40-character commit SHA.")
    if not release["draft"] and not published:
        raise ValueError(
            "The release must still be a draft; do not publish it in the UI."
        )
    if release["target_commitish"] != sha or release["prerelease"] != prerelease:
        raise ValueError("Draft target SHA or prerelease designation does not match.")
    if tag_commit(release["tag_name"]) not in (None, sha):
        raise ValueError("The release tag points to a different commit.")


def distribution_files(directory: Path, tag: str) -> dict[str, str]:
    canonical_tag, version = normalise_version(tag)
    if tag != canonical_tag:
        raise ValueError("Use the normalised v<version> release tag.")
    paths = sorted(directory.iterdir())
    wheels = [path for path in paths if path.name.endswith(".whl")]
    sources = [path for path in paths if path.name.endswith(".tar.gz")]
    if len(paths) != 2 or len(wheels) != 1 or len(sources) != 1:
        raise ValueError("Expected exactly one wheel and one source distribution.")
    for path in paths:
        if path in wheels:
            name, parsed_version, *_ = parse_wheel_filename(path.name)
            with zipfile.ZipFile(path) as archive:
                (metadata_name,) = [
                    name
                    for name in archive.namelist()
                    if name.endswith(".dist-info/METADATA")
                ]
                metadata = archive.read(metadata_name)
        else:
            name, parsed_version = parse_sdist_filename(path.name)
            with tarfile.open(path) as archive:
                (member,) = [
                    member
                    for member in archive.getmembers()
                    if len(Path(member.name).parts) == 2
                    and member.name.endswith("/PKG-INFO")
                    and member.isfile()
                ]
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError("Missing source distribution metadata.")
                metadata = stream.read()
        headers = BytesParser().parsebytes(metadata)
        if (
            canonicalize_name(name) != "uk-address-matcher"
            or parsed_version != version
            or canonicalize_name(headers["Name"]) != "uk-address-matcher"
            or Version(headers["Version"]) != version
        ):
            raise ValueError("Distribution name/version does not match the release.")
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def download(artifact_id: int) -> zipfile.ZipFile:
    return zipfile.ZipFile(
        io.BytesIO(gh("api", f"repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"))
    )


def write_outputs(**values) -> None:
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.writelines(f"{name}={value}\n" for name, value in values.items())


def trusted_branch(branch: str) -> bool:
    return branch == "main" or branch.startswith("build/")


def push_request(kind: str, branch: str, request: dict) -> None:
    if not trusted_branch(branch):
        raise ValueError("Release requests require main or a trusted build branch.")
    path = f"contents/.github/release-{kind}.json"
    try:
        existing = api(f"{path}?ref={quote(branch, safe='')}")
    except subprocess.CalledProcessError as error:
        if json.loads(error.output or b"{}").get("status") != "404":
            raise
        existing = {}
    request = dict(request, request_id=uuid.uuid4().hex)
    payload = {
        "branch": branch,
        "message": f"chore(release): request {kind} for {request['release_tag']}",
        "content": base64.b64encode(
            (json.dumps(request, indent=2) + "\n").encode()
        ).decode(),
    }
    if existing.get("sha"):
        payload["sha"] = existing["sha"]
    gh(
        "api", "--method", "PUT", f"repos/{REPOSITORY}/{path}", "--input", "-",
        input=json.dumps(payload).encode(),
    )
    logging.info("Committed %s request to remote branch %s.", kind, branch)


def load_request(kind: str) -> None:
    if not trusted_branch(os.environ["GITHUB_REF_NAME"]):
        raise ValueError("Release requests require main or a trusted build branch.")
    request = json.loads(Path(f".github/release-{kind}.json").read_text())
    tag, _ = normalise_version(request["release_tag"])
    if tag != request["release_tag"]:
        raise ValueError("Use the normalised v<version> release tag.")
    os.environ["RELEASE_TAG"] = tag
    if kind == "candidate":
        if (
            not isinstance(request["prerelease"], bool)
            or request["source_branch"] != os.environ["GITHUB_REF_NAME"]
        ):
            raise ValueError("Candidate branch or prerelease designation is invalid.")
        os.environ["TARGET_SHA"] = request["target_sha"]
        os.environ["SOURCE_BRANCH"] = request["source_branch"]
        os.environ["PRERELEASE"] = str(request["prerelease"]).lower()
    else:
        run_id = request["validation_run_id"]
        if type(run_id) is not int or run_id < 1:
            raise ValueError("A positive candidate run ID is required.")
        os.environ["VALIDATION_RUN_ID"] = str(run_id)


def prepare(version_text: str, branch: str, prerelease: bool) -> None:
    if not trusted_branch(branch):
        raise ValueError("Release requests require main or a trusted build branch.")
    tag, version = normalise_version(version_text)
    if version.is_prerelease != prerelease:
        raise ValueError("Development/prerelease versions require Pre-release: yes.")
    gh("auth", "status")
    sha = api(f"branches/{quote(branch, safe='')}")["commit"]["sha"]
    if (
        tag_commit(tag)
        or any(
            release["tag_name"] == tag
            for release in api("releases?per_page=100", paginate=True)
        )
        or pypi_files(version)
    ):
        raise ValueError(
            "Tag, release or PyPI version already exists; nothing was changed."
        )
    arguments = [
        "release",
        "create",
        tag,
        "--repo",
        REPOSITORY,
        "--draft",
        "--target",
        sha,
        "--title",
        tag,
        "--generate-notes",
    ]
    if prerelease:
        arguments.append("--prerelease")
    logging.info(gh(*arguments).decode().strip())
    retry = [
        "uv", "run", "--no-project", "--python", "3.12", "--with",
        "packaging==25.0", "python", "-I", "scripts/release.py",
        "request-candidate", "--branch", branch, "--tag", tag,
        "--target-sha", sha, "--prerelease", "yes" if prerelease else "no",
    ]
    logging.info("Source: %s\nTarget SHA: %s", branch, sha)
    try:
        push_request("candidate", branch, {
            "release_tag": tag, "target_sha": sha, "source_branch": branch,
            "prerelease": prerelease,
        })
    except subprocess.CalledProcessError:
        logging.error(
            "Draft retained. Retry the request with:\n%s", shlex.join(retry)
        )
        raise
    logging.info(
        f"Validation runs: https://github.com/{REPOSITORY}/actions/workflows/release-candidate.yml"
    )
    logging.info(
        "After validation succeeds, use request-publish with its run ID."
    )


def candidate() -> None:
    tag, version = normalise_version(os.environ["RELEASE_TAG"])
    sha = os.environ["TARGET_SHA"]
    prerelease = os.environ["PRERELEASE"] == "true"
    results = json.loads(os.environ["VALIDATION_RESULTS"])
    if set(results) != {
        "run-pytest",
        "test-build-package",
        "test-labelling",
        "security",
    }:
        raise ValueError("Missing validation gates.")
    if any(job["result"] != "success" for job in results.values()):
        raise ValueError("All four validation gates must succeed.")
    release = release_for(tag)
    check_release(release, sha, prerelease)
    if version.is_prerelease != prerelease:
        raise ValueError("Version and prerelease designation disagree.")
    artifact_id = int(os.environ["DIST_ARTIFACT_ID"])
    artifact = api(f"actions/artifacts/{artifact_id}")
    if artifact["expired"] or artifact["workflow_run"]["id"] != int(
        os.environ["GITHUB_RUN_ID"]
    ):
        raise ValueError("Build artefact does not belong to this candidate run.")
    manifest = {
        "tag": tag,
        "sha": sha,
        "source_branch": os.environ["SOURCE_BRANCH"],
        "prerelease": prerelease,
        "release_id": release["id"],
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "workflow_sha": os.environ["GITHUB_SHA"],
        "artifact_id": artifact_id,
        "artifact_name": artifact["name"],
        "gates": {name: job["result"] for name, job in results.items()},
        "files": distribution_files(Path("dist"), tag),
    }
    Path("release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def proposal() -> None:
    tag, version = normalise_version(os.environ["RELEASE_TAG"])
    sha = os.environ["TARGET_SHA"]
    prerelease = os.environ["PRERELEASE"] == "true"
    if tag != os.environ["RELEASE_TAG"] or version.is_prerelease != prerelease:
        raise ValueError("Release tag or prerelease designation is not canonical.")
    check_release(release_for(tag), sha, prerelease)
    branch = quote(os.environ["SOURCE_BRANCH"], safe="")
    api(f"branches/{branch}")
    if api(f"compare/{sha}...{branch}")["status"] not in ("ahead", "identical"):
        raise ValueError("The target SHA is not on the selected source branch.")
    write_outputs(target_sha=sha)


def verify(tag: str, run_id: int, manifest_id: int | None, *, published=False) -> dict:
    run = api(f"actions/runs/{run_id}")
    if (
        run["path"] != ".github/workflows/release-candidate.yml"
        or run["event"] != "push"
        or not trusted_branch(run["head_branch"])
    ):
        raise ValueError("Expected a candidate push on main or a trusted build branch.")
    artifacts = api(
        f"actions/runs/{run_id}/artifacts?per_page=100",
        paginate=True,
        collection="artifacts",
    )
    manifest_name = f"release-manifest-{run['run_attempt']}"
    (selected,) = [
        artifact
        for artifact in artifacts
        if (
            artifact["id"] == manifest_id
            if manifest_id
            else artifact["name"] == manifest_name
        )
    ]
    if selected["expired"]:
        raise ValueError("The release manifest has expired.")
    with download(selected["id"]) as archive:
        manifest = json.loads(archive.read("release-manifest.json"))
    attempt = api(f"actions/runs/{run_id}/attempts/{manifest['run_attempt']}")
    if (
        attempt["conclusion"] != "success"
        or manifest["run_id"] != run_id
        or manifest["tag"] != tag
        or manifest["workflow_sha"] != run["head_sha"]
        or selected["name"] != f"release-manifest-{manifest['run_attempt']}"
        or manifest["gates"]
        != dict.fromkeys(
            ("run-pytest", "test-build-package", "test-labelling", "security"),
            "success",
        )
    ):
        raise ValueError("Candidate run, manifest or validation results do not match.")
    release = release_for(tag)
    check_release(release, manifest["sha"], manifest["prerelease"], published=published)
    if release["id"] != manifest["release_id"]:
        raise ValueError("This is not the draft verified by the candidate run.")
    (distribution,) = [
        item for item in artifacts if item["id"] == manifest["artifact_id"]
    ]
    if distribution["expired"] or distribution["name"] != manifest["artifact_name"]:
        raise ValueError("Verified distributions are missing or expired.")
    directory = Path("dist")
    directory.mkdir(exist_ok=True)
    if list(directory.iterdir()):
        raise ValueError("Distribution download directory must be empty.")
    with download(distribution["id"]) as archive:
        names = archive.namelist()
        if len(names) != 2 or set(names) != set(manifest["files"]):
            raise ValueError("Distribution archive differs from the release manifest.")
        for name in names:
            if Path(name).name != name:
                raise ValueError("Unexpected distribution archive path.")
            (directory / name).write_bytes(archive.read(name))
    if distribution_files(directory, tag) != manifest["files"]:
        raise ValueError("Distribution hashes do not match the verified manifest.")
    if normalise_version(tag)[1].is_prerelease != manifest["prerelease"]:
        raise ValueError("Version and prerelease designation disagree.")
    write_outputs(manifest_id=selected["id"])
    return manifest


def finalise(manifest: dict) -> None:
    tag = manifest["tag"]
    check_pypi(normalise_version(tag)[1], manifest["files"], complete=True)
    release = release_for(tag)
    if release["draft"]:
        gh(
            "release",
            "upload",
            tag,
            *[str(path) for path in sorted(Path("dist").iterdir())],
            "--repo",
            REPOSITORY,
            "--clobber",
        )
    release = api(f"releases/{release['id']}")
    assets = {asset["name"]: asset for asset in release["assets"]}
    if set(assets) != set(manifest["files"]):
        raise ValueError(
            "GitHub release assets do not match the verified distributions."
        )
    for name, expected in manifest["files"].items():
        content = gh(
            "api",
            f"repos/{REPOSITORY}/releases/assets/{assets[name]['id']}",
            "-H",
            "Accept: application/octet-stream",
        )
        if hashlib.sha256(content).hexdigest() != expected:
            raise ValueError(
                "A GitHub release asset differs from the verified distribution."
            )
    check_release(release, manifest["sha"], manifest["prerelease"], published=True)
    if release["draft"]:
        gh("release", "edit", tag, "--repo", REPOSITORY, "--draft=false")
    if not api(f"releases/{release['id']}").get("immutable"):
        raise ValueError("The published GitHub release is not immutable.")
    if tag_commit(tag) != manifest["sha"]:
        raise ValueError("The published tag does not identify the tested commit.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare and verify UKAM draft releases."
    )
    parser.add_argument(
        "command",
        choices=(
            "prepare",
            "request-candidate",
            "request-publish",
            "proposal",
            "candidate",
            "verify",
            "publish-check",
            "finalise",
        ),
    )
    parser.add_argument("--version")
    parser.add_argument("--target-sha")
    parser.add_argument("--branch")
    parser.add_argument("--prerelease", choices=("yes", "no"), default="no")
    parser.add_argument("--tag", default=os.environ.get("RELEASE_TAG"))
    parser.add_argument(
        "--run-id", type=int, default=os.environ.get("VALIDATION_RUN_ID")
    )
    parser.add_argument(
        "--manifest-id", type=int, default=os.environ.get("MANIFEST_ID") or None
    )
    arguments = parser.parse_args()
    if arguments.command in {"prepare", "request-candidate", "request-publish"}:
        if not arguments.branch:
            parser.error("--branch is required; release requests create remote commits")
    if arguments.command in {"proposal", "candidate", "publish-check", "finalise"}:
        kind = (
            "candidate" if arguments.command in {"proposal", "candidate"} else "publish"
        )
        load_request(kind)
        arguments.tag = os.environ["RELEASE_TAG"]
        if kind == "publish":
            arguments.run_id = int(os.environ["VALIDATION_RUN_ID"])
    if arguments.command == "prepare":
        if not arguments.version:
            parser.error("--version is required")
        prepare(arguments.version, arguments.branch, arguments.prerelease == "yes")
    elif arguments.command == "request-candidate":
        if not arguments.tag or not arguments.target_sha:
            parser.error("A release tag and target SHA are required")
        tag, version = normalise_version(arguments.tag)
        prerelease = arguments.prerelease == "yes"
        if version.is_prerelease != prerelease:
            parser.error("Version and prerelease designation disagree")
        check_release(release_for(tag), arguments.target_sha, prerelease)
        push_request("candidate", arguments.branch, {
            "release_tag": tag, "target_sha": arguments.target_sha,
            "source_branch": arguments.branch, "prerelease": prerelease,
        })
    elif arguments.command == "request-publish":
        if not arguments.tag or not arguments.run_id or arguments.run_id < 1:
            parser.error("A release tag and positive validation run ID are required")
        push_request("publish", arguments.branch, {
            "release_tag": normalise_version(arguments.tag)[0],
            "validation_run_id": arguments.run_id,
        })
    elif arguments.command == "candidate":
        candidate()
    elif arguments.command == "proposal":
        proposal()
    else:
        if not arguments.tag or not arguments.run_id:
            parser.error("A release tag and validation run ID are required")
        manifest = verify(
            arguments.tag,
            arguments.run_id,
            arguments.manifest_id,
            published=arguments.command == "finalise",
        )
        if arguments.command == "publish-check":
            check_pypi(normalise_version(arguments.tag)[1], manifest["files"])
        elif arguments.command == "finalise":
            finalise(manifest)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        main()
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(f"Release stopped; no automatic cleanup: {error}")
