import os
import sys
from pathlib import Path

import duckdb

from uk_address_matcher import ExactMatchStage, SplinkStage, prepare_data_for_matching
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.training import get_settings_for_training


def _write_error_comment(message: str) -> None:
    comment_path = Path.cwd() / "github-comment.md"
    with comment_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write("## ❌ Labels accuracy run failed\n\n")
        file_handle.write(f"{message}\n\n")
        file_handle.write("Please check the workflow logs for details.\n")


def _write_success_comment(
    *,
    total_cases: int,
    correct_matches: int,
    match_rate: float,
) -> None:
    comment_path = Path.cwd() / "github-comment.md"
    with comment_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write("## 📊 Address Matcher Test Results\n\n")
        file_handle.write("### Statistics\n\n")
        file_handle.write(f"- **Total test cases:** {total_cases}\n")
        file_handle.write(f"- **Correct matches:** {correct_matches}\n")
        file_handle.write(f"- **Incorrect matches:** {total_cases - correct_matches}\n")
        file_handle.write(f"- **Match rate:** {match_rate:.2f}%\n")
        file_handle.write("- **Total reward:** N/A\n")


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

        messy_data_rel = con.sql(
            """
            SELECT
                id::VARCHAR AS unique_id,
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
        con.register("canonical_data", canonical_rel)

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

        con.register("__match_candidates", match_candidates_rel)
        stats_row = con.sql(
            """
            SELECT
                COUNT(*) AS total_cases,
                SUM(
                    CASE
                        WHEN resolved_canonical_id::VARCHAR = ukam_label::VARCHAR THEN 1
                        ELSE 0
                    END
                )::BIGINT AS correct_matches
            FROM __match_candidates
            WHERE ukam_label IS NOT NULL
            """
        ).fetchone()

        total_cases = int(stats_row[0] or 0)
        correct_matches = int(stats_row[1] or 0)
        match_rate = (correct_matches / total_cases) * 100.0 if total_cases > 0 else 0.0

        _write_success_comment(
            total_cases=total_cases,
            correct_matches=correct_matches,
            match_rate=match_rate,
        )

        print(f"Completed labels accuracy run with {total_cases} labelled records.")
        return 0

    except Exception as error:
        _write_error_comment(f"Unexpected error: {error}")
        print(f"Unexpected error: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(run_labels_accuracy())
