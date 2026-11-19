from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scan secrets without updating baselines."
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=REPOSITORY_ROOT / "security" / "secrets.baseline",
    )
    parser.add_argument("filenames", nargs="+")
    arguments = parser.parse_args(argv)

    if not arguments.baseline.is_file():
        parser.error("The maintained secrets baseline is missing.")

    with tempfile.TemporaryDirectory(prefix="ukam-secrets-") as temporary_directory:
        temporary_baseline = Path(temporary_directory) / "secrets.baseline"
        shutil.copyfile(arguments.baseline, temporary_baseline)
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "detect_secrets.pre_commit_hook",
                "--baseline",
                str(temporary_baseline),
                *arguments.filenames,
            ],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    if result.returncode == 3:
        return 0
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
