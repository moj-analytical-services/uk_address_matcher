from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    exported = subprocess.run(
        [
            "uv",
            "export",
            "--locked",
            "--all-extras",
            "--all-groups",
            "--no-emit-project",
            "--format",
            "requirements-txt",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if exported.returncode:
        sys.stderr.write(exported.stderr)
        return exported.returncode

    with tempfile.TemporaryDirectory(prefix="ukam-audit-") as temporary_directory:
        requirements_file = Path(temporary_directory) / "requirements.txt"
        requirements_file.write_text(exported.stdout, encoding="utf-8")
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "pip_audit",
                "--disable-pip",
                "--no-deps",
                "--requirement",
                str(requirements_file),
            ],
            check=False,
        ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
