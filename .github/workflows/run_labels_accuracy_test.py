import os
import subprocess
import sys
from pathlib import Path

import duckdb

from uk_address_matcher import ExactMatchStage, SplinkStage, prepare_data_for_matching
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.training import get_settings_for_training
from uk_address_matcher.post_linkage.accuracy_from_labels import (
    evaluate_predictions_against_labels,
)


def _labels_git_ref(repo_dir: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo_dir), "rev-parse", "--short", "HEAD"],
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _write_error_comment(message: str) -> None:
    comment_path = Path.cwd() / "github-comment.md"
    with comment_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write("## ❌ Labels accuracy run failed\n\n")
        file_handle.write(f"{message}\n\n")
        file_handle.write("Please check the workflow logs for details.\n")


def _write_success_comment(
    *,
    labels_repo_dir: Path,
    total_labelled: int,
    evaluation_rows: list[tuple],
    wrong_predictions: list[tuple],
) -> None:
    comment_path = Path.cwd() / "github-comment.md"
    with comment_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write("## 📊 Address matcher accuracy (external labels)\n\n")
        file_handle.write(f"- Labels repo: `{labels_repo_dir.name}`\n")
        file_handle.write(f"- Labels ref: `{_labels_git_ref(labels_repo_dir)}`\n")
        file_handle.write(f"- Labelled records used: **{total_labelled}**\n\n")

        file_handle.write("### Summary\n\n")
        file_handle.write("| Status | Count | Percentage |\n")
        file_handle.write("| ------ | -----:| ----------:|\n")

        for status, count, _percentage, percentage_fmt in evaluation_rows:
            safe_status = status if status is not None else "Unknown"
            safe_percentage = percentage_fmt if percentage_fmt is not None else "N/A"
            file_handle.write(f"| {safe_status} | {count} | {safe_percentage} |\n")

        if wrong_predictions:
            file_handle.write("\n### Sample wrong predictions (up to 20)\n\n")
            file_handle.write("| unique_id | expected | predicted |\n")
            file_handle.write("| --------- | -------- | --------- |\n")
            for unique_id, expected, predicted in wrong_predictions:
                file_handle.write(
                    f"| {unique_id} | {expected} | {predicted if predicted is not None else 'NULL'} |\n"
                )


def run_labels_accuracy() -> int:
    labels_repo = Path(os.environ.get("LABELS_REPO_DIR", "address_matching_labels"))
    labels_path = labels_repo / "export.csv"
    canonical_path = labels_repo / "small.parquet"

    if not labels_path.exists() or not canonical_path.exists():
        _write_error_comment(
            "Required labels files were not found. "
            f"Expected `{labels_path}` and `{canonical_path}`."
        )
        return 1

    con = duckdb.connect(database=":memory:")

    try:
        labels_export = con.read_csv(str(labels_path))
        con.register("labels_export", labels_export)

        labels_rel = con.sql(
            """
            SELECT
                unique_id_r::VARCHAR AS unique_id,
                unique_id_l::VARCHAR AS correct_unique_id
            FROM labels_export
            WHERE human_label = 1
            """
        )

        messy_data_rel = con.sql(
            """
            SELECT
                unique_id_r::VARCHAR AS unique_id,
                messy_address AS address_concat,
                messy_postcode AS postcode,
                unique_id_l::VARCHAR AS ukam_label
            FROM labels_export
            WHERE human_label = 1
            """
        )

        canonical_rel = con.read_parquet(str(canonical_path)).select(
            "unique_id::VARCHAR AS unique_id, address_concat, postcode"
        )

        messy_clean_rel = prepare_data_for_matching(messy_data_rel, con=con)
        canonical_clean_rel = prepare_data_for_matching(canonical_rel, con=con)

        settings = get_settings_for_training()
        match_candidates_rel = _run_matching(
            con=con,
            df_messy_clean=messy_clean_rel,
            df_canonical_clean=canonical_clean_rel,
            stages=[
                ExactMatchStage(),
                SplinkStage(
                    predict_threshold_match_weight=-20,
                    improve_threshold_match_weight=-10,
                    improve_top_n_matches=3,
                    improve_use_bigrams=True,
                    final_match_weight_threshold=-10,
                    final_distinguishability_threshold=None,
                    include_full_postcode_block=False,
                    include_outside_postcode_block=True,
                    retain_intermediate_calculation_columns=False,
                    settings=settings,
                ),
            ],
        )

        evaluation_rel = evaluate_predictions_against_labels(
            match_candidates=match_candidates_rel,
            con=con,
        )

        con.register("__match_candidates", match_candidates_rel)
        wrong_predictions = con.sql(
            """
            SELECT
                unique_id,
                ukam_label::VARCHAR AS expected,
                resolved_canonical_id::VARCHAR AS predicted
            FROM __match_candidates
            WHERE ukam_label IS NOT NULL
              AND (resolved_canonical_id IS NULL OR resolved_canonical_id::VARCHAR <> ukam_label::VARCHAR)
            LIMIT 20
            """
        ).fetchall()

        total_labelled = labels_rel.count("*").fetchone()[0]
        evaluation_rows = evaluation_rel.fetchall()

        _write_success_comment(
            labels_repo_dir=labels_repo,
            total_labelled=total_labelled,
            evaluation_rows=evaluation_rows,
            wrong_predictions=wrong_predictions,
        )

        print(f"Completed labels accuracy run with {total_labelled} labelled records.")
        return 0

    except Exception as error:
        _write_error_comment(f"Unexpected error: {error}")
        print(f"Unexpected error: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(run_labels_accuracy())
