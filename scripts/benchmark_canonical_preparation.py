"""Time a temporary canonical build from cached builder Parquets.

Use the same runner and settings on the baseline and integration branches.
Only the small JSON report survives; preparation outputs and scratch are deleted.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import tempfile
import time
from pathlib import Path

import duckdb

import uk_address_matcher
from uk_address_matcher import prepare_canonical_folder


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Builder Parquet file or directory")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--work-dir", type=Path, help="Scratch parent with ample disk space"
    )
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--chunks", type=int, default=20)
    parser.add_argument("--output-chunks", type=int, default=10)
    parser.add_argument("--spill-limit", default="70GB")
    parser.add_argument("--limit", type=int, help="Optional small smoke test")
    args = parser.parse_args()
    if args.report.exists():
        parser.error("Report already exists; choose a new path")
    if args.work_dir:
        args.work_dir.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    checkout = Path(__file__).resolve().parents[1]
    module_path = Path(uk_address_matcher.__file__).resolve()
    if not module_path.is_relative_to(checkout):
        parser.error("Run with uv from this checkout so its code is imported")
    report = {
        "matcher_module": str(module_path),
        "commit": subprocess.check_output(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True
        ).strip(),
        "dirty": bool(
            subprocess.check_output(
                ["git", "-C", str(checkout), "status", "--porcelain"], text=True
            ).strip()
        ),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "duckdb": duckdb.__version__,
        "settings": vars(args),
        "scope": "public API call; file-backed, unordered; no phase checkpoints",
        "status": "failed",
    }
    previous_tempdir = tempfile.tempdir
    try:
        with tempfile.TemporaryDirectory(
            prefix="ukam-preparation-benchmark-", dir=args.work_dir
        ) as scratch:
            # Also contain temporary partition files created by preparation.
            tempfile.tempdir = scratch
            config = {
                "memory_limit": args.memory_limit,
                "threads": args.threads,
                "temp_directory": str(Path(scratch) / "spill"),
                "max_temp_directory_size": args.spill_limit,
                "preserve_insertion_order": False,
            }
            with duckdb.connect(
                str(Path(scratch) / "working.duckdb"), config=config
            ) as con:
                source_path = (
                    args.input / "*.parquet" if args.input.is_dir() else args.input
                )
                source = con.read_parquet(str(source_path))
                if args.limit:
                    source = source.limit(args.limit)
                report["input_rows"] = source.count("*").fetchone()[0]
                output = Path(scratch) / "prepared"
                started, cpu_started = time.perf_counter(), time.process_time()
                try:
                    prepare_canonical_folder(
                        source,
                        output,
                        con=con,
                        num_of_chunks=args.chunks,
                        output_chunk_count=args.output_chunks,
                        show_progress="stages",
                    )
                finally:
                    report["api_seconds"] = time.perf_counter() - started
                    report["cpu_seconds"] = time.process_time() - cpu_started
                report["manifest"] = json.loads(
                    (output / "ukam_manifest.json").read_text()
                )
                report["status"] = "complete"
    except BaseException as exc:
        # Do not persist addresses or potentially authenticated input paths in errors.
        report["error_type"] = type(exc).__name__
        raise
    finally:
        tempfile.tempdir = previous_tempdir
        report["generated_data_removed"] = (
            "scratch" not in locals() or not Path(scratch).exists()
        )
        args.report.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(  # noqa: T201 - command-line result
        json.dumps({key: report[key] for key in ("status", "api_seconds", "cpu_seconds")})
    )


if __name__ == "__main__":
    main()
