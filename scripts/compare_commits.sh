#!/usr/bin/env bash
#
# compare_commits.sh
# ------------------------------------------------------------------------------
# Benchmark and overlay-compare two versions of the uk_address_matcher code,
# pinned to git commits (or the local working tree), using the same harness as
# benchmarking/run_benchmarking.py.
#
# What it does
#   1. Checks out each git ref into a disposable `git worktree` (your current
#      checkout is never touched) and runs the benchmark from that code.
#   2. Runs for any number of datasets (same dataset keys as run_benchmarking.py).
#   3. Persists both runs into the SAME results root and generates per-dataset
#      overlay precision-recall charts (candidate vs baseline), exactly like
#      run_benchmarking.py does via comparison_baseline_run_id.
#
# The second ref may be the literal string `local`, which runs your current
# working tree (including uncommitted changes) instead of a clean commit.
#
# Usage
#   scripts/compare_commits.sh -b <baseline_ref> -c <candidate_ref|local> \
#       -d <dataset[,dataset,...]> [-w <splink_weight>] [-r <results_root>] [--fetch]
#
# Examples
#   # Compare main against your local working tree on two datasets
#   scripts/compare_commits.sh -b main -c local -d hackney,rhondda
#
#   # Compare two commits across the four core datasets
#   scripts/compare_commits.sh -b 4b9e3e1 -c 0a2fccf \
#       -d rhondda,aberdeenshire,hackney,mid_sussex
#
# Benchmark configuration (data paths)
#   Resolved the same way as a normal benchmark run: from real environment
#   variables, OR from the private (untracked) benchmarking/.config.json, e.g.
#       UKAM_OS_CANONICAL_PREPARED  - prepared canonical folder
#       UKAM_<DATASET>_DATA_PATH    - source path for each selected dataset
#   The config file is copied into each worktree automatically so pinned-commit
#   runs can resolve the same data paths.
#
# Notes
#   * Each commit is benchmarked with ITS OWN code (harness + library + model).
#     The candidate run reads the baseline run's persisted PR curve to build the
#     overlay, so keep the candidate as the newer/local side where possible.
#   * Every run uses `uv` to resolve the environment for that commit, so the
#     first run against a fresh commit may be slow while uv syncs dependencies.
# ------------------------------------------------------------------------------

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults and argument parsing
# ---------------------------------------------------------------------------
BASELINE_REF=""
CANDIDATE_REF=""
DATASETS=""
SPLINK_WEIGHT=""
RESULTS_ROOT=""
DO_FETCH=0

usage() {
    sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--baseline)        BASELINE_REF="$2"; shift 2 ;;
        --baseline=*)         BASELINE_REF="${1#*=}"; shift ;;
        -c|--candidate)       CANDIDATE_REF="$2"; shift 2 ;;
        --candidate=*)        CANDIDATE_REF="${1#*=}"; shift ;;
        -d|--datasets)        DATASETS="$2"; shift 2 ;;
        --datasets=*)         DATASETS="${1#*=}"; shift ;;
        -w|--weight)          SPLINK_WEIGHT="$2"; shift 2 ;;
        --weight=*)           SPLINK_WEIGHT="${1#*=}"; shift ;;
        -r|--results-root)    RESULTS_ROOT="$2"; shift 2 ;;
        --results-root=*)     RESULTS_ROOT="${1#*=}"; shift ;;
        --fetch)              DO_FETCH=1; shift ;;
        -h|--help)            usage 0 ;;
        *) echo "ERROR: unknown argument '$1'" >&2; usage 1 ;;
    esac
done

if [[ -z "$BASELINE_REF" || -z "$CANDIDATE_REF" || -z "$DATASETS" ]]; then
    echo "ERROR: --baseline, --candidate and --datasets are all required." >&2
    usage 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: 'uv' is not on PATH. Install uv or activate the project shell." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Repo + results-root resolution
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

# Benchmark data paths are read from this private (untracked) config by
# benchmarking.settings via apply_env_from_private_config(); they are NOT
# necessarily exported as shell environment variables.
PRIVATE_CONFIG="$REPO_ROOT/benchmarking/.config.json"
if [[ -z "${UKAM_OS_CANONICAL_PREPARED:-}" && ! -f "$PRIVATE_CONFIG" ]]; then
    echo "ERROR: no benchmark config found. Set UKAM_OS_CANONICAL_PREPARED in the" \
         "environment or create $PRIVATE_CONFIG." >&2
    exit 1
fi

if [[ -z "$RESULTS_ROOT" ]]; then
    RESULTS_ROOT="$REPO_ROOT/benchmarking/results"
fi
# Absolute path so every worktree persists into the same shared results store.
mkdir -p "$RESULTS_ROOT"
RESULTS_ROOT="$(cd "$RESULTS_ROOT" && pwd)"

# ---------------------------------------------------------------------------
# Worktree management + cleanup
# ---------------------------------------------------------------------------
WORKTREE_BASE="$(mktemp -d "${TMPDIR:-/tmp}/ukam_cmp_XXXXXX")"
CREATED_WORKTREES=()

cleanup() {
    local wt
    for wt in "${CREATED_WORKTREES[@]:-}"; do
        [[ -n "$wt" ]] || continue
        git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || true
    done
    rm -rf "$WORKTREE_BASE" >/dev/null 2>&1 || true
    git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1 || true
}
trap cleanup EXIT

resolve_commit() {
    # Echo the full commit sha for a ref, fetching once if it is missing.
    local ref="$1" sha
    sha="$(git -C "$REPO_ROOT" rev-parse --verify --quiet "${ref}^{commit}" || true)"
    if [[ -z "$sha" && "$DO_FETCH" -eq 0 ]]; then
        echo "INFO: ref '$ref' not found locally; fetching..." >&2
        git -C "$REPO_ROOT" fetch --all --tags --quiet || true
        sha="$(git -C "$REPO_ROOT" rev-parse --verify --quiet "${ref}^{commit}" || true)"
    fi
    [[ -n "$sha" ]] || { echo "ERROR: could not resolve git ref '$ref'." >&2; exit 1; }
    echo "$sha"
}

if [[ "$DO_FETCH" -eq 1 ]]; then
    git -C "$REPO_ROOT" fetch --all --tags --quiet || true
fi

# ---------------------------------------------------------------------------
# Embedded benchmark driver (parameterised via CMP_* env vars).
# Mirrors benchmarking/run_benchmarking.py but reads config from the
# environment and only passes kwargs the resolved harness actually supports,
# so it tolerates minor signature drift across commits.
# ---------------------------------------------------------------------------
read -r -d '' BENCH_DRIVER <<'PYEOF' || true
import inspect
import os

from benchmarking import constants as C
from benchmarking.runner import run_selected_datasets
from benchmarking.settings import CANONICAL_PATH, SAMPLE_MODE
from uk_address_matcher import ExactMatchStage, PeeledAddressStage, SplinkStage

datasets = [d.strip() for d in os.environ["CMP_DATASETS"].split(",") if d.strip()]
run_id = os.environ["CMP_RUN_ID"]
baseline = os.environ.get("CMP_BASELINE_RUN_ID") or None
results_root = os.environ.get("CMP_RESULTS_ROOT") or C.BENCHMARK_RESULTS_ROOT

weight_env = os.environ.get("CMP_SPLINK_WEIGHT", "").strip()
weight = float(weight_env) if weight_env else C.SPLINK_BASELINE_WEIGHT

stages = [
    ExactMatchStage(True),
    PeeledAddressStage(),
    SplinkStage(final_match_weight_threshold=weight),
]

apply_filter = bool(getattr(C, "APPLY_CANONICAL_FILTER", False))
canonical_filter = getattr(C, "CANONICAL_FILTER_SQL", None) if apply_filter else None
cleaning_chunks = getattr(C, "CLEANING_NUM_CHUNKS", 1)

kwargs = dict(
    selected_datasets=datasets,
    canonical_path=CANONICAL_PATH,
    stages=stages,
    persist_results=True,
    results_root=results_root,
)
optional = {
    "run_id": run_id,
    "sample_mode": SAMPLE_MODE,
    "canonical_address_filter": canonical_filter,
    "cleaning_num_chunks": cleaning_chunks,
    "enable_comparison_charts": bool(baseline),
    "comparison_baseline_run_id": baseline,
}
sig = inspect.signature(run_selected_datasets)
for key, value in optional.items():
    if key in sig.parameters:
        kwargs[key] = value

print(
    f"[compare_commits] run_id={run_id} datasets={datasets} "
    f"baseline={baseline or '<none>'} results_root={results_root} weight={weight}"
)
run_selected_datasets(**kwargs)
print(f"[compare_commits] finished run_id={run_id}")
PYEOF

run_benchmark_in_dir() {
    local workdir="$1" run_id="$2" baseline_run_id="$3"
    echo
    echo "=============================================================="
    echo ">>> Benchmark: dir=$workdir"
    echo ">>>           run_id=$run_id baseline=${baseline_run_id:-<none>}"
    echo "=============================================================="
    (
        cd "$workdir"
        export CMP_DATASETS="$DATASETS"
        export CMP_RUN_ID="$run_id"
        export CMP_BASELINE_RUN_ID="$baseline_run_id"
        export CMP_RESULTS_ROOT="$RESULTS_ROOT"
        export CMP_SPLINK_WEIGHT="$SPLINK_WEIGHT"
        printf '%s' "$BENCH_DRIVER" | uv run python -
    )
}

# Materialise a commit (or the local tree) into PREPARED_DIR. Worktree creation
# and bookkeeping happen in the main shell (NOT a command-substitution subshell)
# so the cleanup trap can see every worktree it must remove.
PREPARED_DIR=""

prepare_workdir() {
    # Args: <sha-or-"local"> <short-label>. Sets PREPARED_DIR.
    local sha="$1" short="$2" wt
    if [[ "$sha" == "local" ]]; then
        PREPARED_DIR="$REPO_ROOT"
        return 0
    fi
    wt="$WORKTREE_BASE/$short"
    git -C "$REPO_ROOT" worktree add --detach --quiet "$wt" "$sha"
    CREATED_WORKTREES+=("$wt")
    # The private data-path config is untracked, so a clean worktree checkout
    # won't contain it. Copy it in so the benchmark can resolve dataset paths.
    if [[ -f "$PRIVATE_CONFIG" && -d "$wt/benchmarking" ]]; then
        cp "$PRIVATE_CONFIG" "$wt/benchmarking/.config.json"
    fi
    PREPARED_DIR="$wt"
}

# ---------------------------------------------------------------------------
# Resolve refs to commit shas up front (this is also where any fetch happens),
# derive stable run ids, and guard against an unusable self-comparison.
# ---------------------------------------------------------------------------
if [[ "$BASELINE_REF" == "local" ]]; then
    BASELINE_SHA="local"; BASELINE_SHORT="local"; BASELINE_RUN_ID="cmp_local"
else
    BASELINE_SHA="$(resolve_commit "$BASELINE_REF")"
    BASELINE_SHORT="$(git -C "$REPO_ROOT" rev-parse --short "$BASELINE_SHA")"
    BASELINE_RUN_ID="cmp_$BASELINE_SHORT"
fi

if [[ "$CANDIDATE_REF" == "local" ]]; then
    CANDIDATE_SHA="local"; CANDIDATE_SHORT="local"; CANDIDATE_RUN_ID="cmp_local"
else
    CANDIDATE_SHA="$(resolve_commit "$CANDIDATE_REF")"
    CANDIDATE_SHORT="$(git -C "$REPO_ROOT" rev-parse --short "$CANDIDATE_SHA")"
    CANDIDATE_RUN_ID="cmp_$CANDIDATE_SHORT"
fi

if [[ "$BASELINE_RUN_ID" == "$CANDIDATE_RUN_ID" ]]; then
    echo "ERROR: baseline and candidate resolve to the same run id" \
         "('$BASELINE_RUN_ID'); choose two different refs." >&2
    exit 1
fi

echo "Comparison plan:"
echo "  baseline  ref=$BASELINE_REF   -> run_id=$BASELINE_RUN_ID"
echo "  candidate ref=$CANDIDATE_REF  -> run_id=$CANDIDATE_RUN_ID"
echo "  datasets       = $DATASETS"
echo "  results root   = $RESULTS_ROOT"
echo "  splink weight  = ${SPLINK_WEIGHT:-<commit default>}"

# ---------------------------------------------------------------------------
# 1) Baseline run (no overlay)  2) Candidate run (overlay vs baseline)
# ---------------------------------------------------------------------------
prepare_workdir "$BASELINE_SHA" "$BASELINE_SHORT"
run_benchmark_in_dir "$PREPARED_DIR" "$BASELINE_RUN_ID" ""

prepare_workdir "$CANDIDATE_SHA" "$CANDIDATE_SHORT"
run_benchmark_in_dir "$PREPARED_DIR" "$CANDIDATE_RUN_ID" "$BASELINE_RUN_ID"

# ---------------------------------------------------------------------------
# Summary: point at the per-dataset overlay artefacts in the candidate run dirs
# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo "Comparison complete: $BASELINE_RUN_ID (baseline) vs $CANDIDATE_RUN_ID (candidate)"
echo "Per-dataset artefacts under: $RESULTS_ROOT/<dataset>/<date>/$CANDIDATE_RUN_ID/"
echo "  overlay chart : charts/precision_recall_overlay_${BASELINE_RUN_ID}_vs_${CANDIDATE_RUN_ID}.html"
echo "  summary json  : comparison_summary_${BASELINE_RUN_ID}_vs_${CANDIDATE_RUN_ID}.json"
echo "  report md     : comparison_report_${BASELINE_RUN_ID}_vs_${CANDIDATE_RUN_ID}.md"
echo "=============================================================="
for ds in ${DATASETS//,/ }; do
    latest_dir="$(ls -dt "$RESULTS_ROOT/$ds"/*/"$CANDIDATE_RUN_ID" 2>/dev/null | head -n1 || true)"
    if [[ -n "$latest_dir" ]]; then
        echo "  $ds -> ${latest_dir#"$REPO_ROOT/"}"
    fi
done
