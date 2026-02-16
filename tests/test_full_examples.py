import os
import subprocess


def test_example_matching():
    env = os.environ.copy()

    # Set flag to limit the number of records for testing
    env["TEST_LIMIT"] = "1"

    result = subprocess.run(
        ["uv", "run", "python", "examples/example_matching.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
