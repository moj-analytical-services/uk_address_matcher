from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
from math import isclose
from typing import TYPE_CHECKING, Any

from duckdb import DuckDBPyRelation

from uk_address_matcher.post_linkage.match_result.splink_inspector import _sql_literal
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason

if TYPE_CHECKING:
    from uk_address_matcher.post_linkage.match_result.result import MatchResult


_REPORT_LABEL_WIDTH = 27


class _MatchResultDebugTools:
    def __init__(self, owner: MatchResult) -> None:
        self._owner = owner
        self._splink_match_reason = MatchReason.SPLINK.value
        self._report_label_width = _REPORT_LABEL_WIDTH

    def _splink_best_matches_table(self) -> str:
        stage = self._owner._require_splink_stage()
        if stage.best_matches_table is None:
            raise ValueError(
                "SplinkStage is configured but did not produce a best-matches "
                "table. This can happen when earlier stages matched all "
                "records before the Splink stage ran."
            )
        return stage.best_matches_table

    def _splink_improved_predictions_table(self) -> str:
        stage = self._owner._require_splink_stage()
        if stage.improved_predictions_table is None:
            raise ValueError(
                "SplinkStage is configured but did not produce an improved "
                "predictions table. This can happen when earlier stages "
                "matched all records before the Splink stage ran."
            )
        return stage.improved_predictions_table

    def _splink_predictions_table(self) -> str:
        stage = self._owner._require_splink_stage()
        if stage.predictions_table is None:
            raise ValueError(
                "SplinkStage is configured but did not produce a predictions "
                "table. This can happen when earlier stages matched all "
                "records before the Splink stage ran."
            )
        return stage.predictions_table

    def _relation_to_ascii(self, relation: DuckDBPyRelation) -> str:
        buffer = StringIO()
        with redirect_stdout(buffer):
            relation.show(max_width=10000)
        return buffer.getvalue().rstrip()

    def _rows_from_relation(
        self,
        relation: DuckDBPyRelation,
    ) -> list[dict[str, Any]]:
        rows = relation.fetchall()
        columns = relation.columns
        return [dict(zip(columns, row)) for row in rows]

    def _query_rows(self, query: str) -> list[dict[str, Any]]:
        return self._rows_from_relation(self._owner.con.sql(query))

    def _query_one_row(self, query: str) -> dict[str, Any] | None:
        rows = self._query_rows(query)
        if not rows:
            return None
        return rows[0]

    def _format_scalar(self, value: Any) -> str:
        if value is None:
            return "NULL"
        return repr(value) if isinstance(value, str) else str(value)

    def _format_address_summary(
        self,
        row: dict[str, Any] | None,
        *,
        include_ukam_label: bool = False,
    ) -> str:
        if row is None:
            return "Unavailable"

        address = " ".join(
            str(part)
            for part in [
                row.get("original_address_concat") or row.get("clean_full_address"),
                row.get("postcode"),
            ]
            if part not in {None, ""}
        )
        parts = [address or "Unavailable"]

        if "unique_id" in row:
            parts.append(f"unique_id {self._format_scalar(row.get('unique_id'))}")
        if "ukam_address_id" in row:
            parts.append(
                f"ukam_address_id {self._format_scalar(row.get('ukam_address_id'))}"
            )
        if include_ukam_label and "ukam_label" in row:
            parts.append(f"ukam_label {self._format_scalar(row.get('ukam_label'))}")

        if len(parts) == 1:
            return parts[0]
        return parts[0] + " (" + ", ".join(parts[1:]) + ")"

    def _align_detail_column(self, lines: list[str]) -> list[str]:
        split_lines: list[tuple[str, str]] = []
        max_address_width = 0

        for line in lines:
            if " (" in line:
                address, detail = line.split(" (", maxsplit=1)
                detail = "(" + detail
            else:
                address, detail = line, ""
            split_lines.append((address, detail))
            max_address_width = max(max_address_width, len(address))

        aligned_lines: list[str] = []
        for address, detail in split_lines:
            if not detail:
                aligned_lines.append(address)
                continue
            aligned_lines.append(f"{address:<{max_address_width + 1}}{detail}")

        return aligned_lines

    def _format_variant_lines(self, rows: list[dict[str, Any]]) -> list[str]:
        if not rows:
            return ["None"]

        lines: list[str] = []
        for row in rows:
            address = " ".join(
                str(part)
                for part in [
                    row.get("original_address_concat") or row.get("clean_full_address"),
                    row.get("postcode"),
                ]
                if part not in {None, ""}
            )
            lines.append(
                f"{address} "
                f"(unique_id={self._format_scalar(row.get('unique_id'))}, "
                "ukam_address_id="
                f"{self._format_scalar(row.get('ukam_address_id'))})"
            )
        return lines

    def _format_ranked_match_entries(
        self,
        rows: list[dict[str, Any]],
    ) -> list[tuple[str, str]]:
        if not rows:
            return [("", "None")]

        suffix_lookup = {1: "st", 2: "nd", 3: "rd"}
        entries: list[tuple[str, str]] = []
        for row in rows:
            rank = row.get("candidate_rank")
            suffix = suffix_lookup.get(rank, "th")
            address = " ".join(
                str(part)
                for part in [
                    row.get("original_address_concat_l")
                    or row.get("clean_full_address_l")
                    or row.get("original_address"),
                    row.get("postcode_l") or row.get("postcode"),
                ]
                if part not in {None, ""}
            )
            entries.append(
                (
                    f"{rank}{suffix}:",
                    f"{address} "
                    f"(unique_id={self._format_scalar(row.get('unique_id_l'))}, "
                    "ukam_address_id="
                    f"{self._format_scalar(row.get('ukam_address_id_l'))}, "
                    f"match_weight={self._format_scalar(row.get('match_weight'))}, "
                    "distinguishability="
                    f"{self._format_scalar(row.get('distinguishability'))})",
                )
            )
        return entries

    def _format_labelled_lines(self, label: str, lines: list[str]) -> list[str]:
        padded_label = f"{label:<{self._report_label_width}}"
        continuation = " " * self._report_label_width
        content_lines = lines or ["None"]

        formatted = [padded_label + content_lines[0]]
        formatted.extend(continuation + line for line in content_lines[1:])
        return formatted

    def _format_labelled_ranked_lines(
        self,
        label: str,
        entries: list[tuple[str, str]],
    ) -> list[str]:
        continuation_label = " " * len(label)
        formatted: list[str] = []

        for index, (rank_label, content) in enumerate(entries):
            current_label = (
                f"{label}{rank_label}"
                if index == 0
                else f"{continuation_label}{rank_label}"
            )
            formatted.append(f"{current_label:<{self._report_label_width}}{content}")

        return formatted

    def _top_prediction_row_for_unique_ids(
        self,
        *,
        messy_id: str | int,
        candidate_id: str | int,
    ) -> DuckDBPyRelation:
        return self._owner.con.sql(f"""
            SELECT *
            FROM {self._splink_predictions_table()}
            WHERE unique_id_r = {_sql_literal(messy_id)}
            AND unique_id_l = {_sql_literal(candidate_id)}
            ORDER BY match_weight DESC, ukam_address_id_l
            LIMIT 1
        """)

    def _chart_to_dict(self, chart: Any) -> dict[str, Any] | None:
        if isinstance(chart, dict):
            return chart

        to_dict = getattr(chart, "to_dict", None)
        if callable(to_dict):
            chart_dict = to_dict()
            if isinstance(chart_dict, dict):
                return chart_dict

        return None

    def _format_signed_weight(self, value: float) -> str:
        return f"{value:+.2f}"

    def _format_weight(self, value: float) -> str:
        return f"{value:.2f}"

    def _waterfall_step_heading(self, row: dict[str, Any]) -> str:
        column_name = str(row.get("column_name") or "Unknown")

        if column_name in {"Prior", "Prior match weight"}:
            return "Prior (starting match weight)"
        if column_name == "Final score":
            return "Final score"

        label = row.get("label_for_charts")
        if isinstance(label, str) and label and label not in {column_name, "Final score"}:
            return f"{column_name} ({label})"

        return column_name

    def _waterfall_rows_from_chart(self, chart: Any) -> list[dict[str, Any]]:
        chart_dict = self._chart_to_dict(chart)
        if chart_dict is None:
            return []

        data_name = chart_dict.get("data", {}).get("name")
        datasets = chart_dict.get("datasets", {})
        if not isinstance(data_name, str) or data_name not in datasets:
            return []

        rows = datasets.get(data_name)
        if not isinstance(rows, list):
            return []

        filtered_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            bayes_factor = row.get("bayes_factor")
            column_name = row.get("column_name")
            if (
                isinstance(bayes_factor, (int, float))
                and isclose(float(bayes_factor), 1.0)
                and column_name not in {"Prior", "Prior match weight", "Final score"}
            ):
                continue
            filtered_rows.append(row)

        return sorted(
            filtered_rows,
            key=lambda row: (
                row.get("record_number", 0),
                row.get("bar_sort_order", 0),
            ),
        )

    def _waterfall_text_summary(self, title: str, chart: Any) -> str | None:
        rows = self._waterfall_rows_from_chart(chart)
        if not rows:
            return None

        summary_lines = [title]
        cumulative_sum = 0.0

        for row in rows:
            column_name = row.get("column_name")
            log2_bayes_factor = row.get("log2_bayes_factor")
            if not isinstance(log2_bayes_factor, (int, float)):
                continue

            weight = float(log2_bayes_factor)
            heading = self._waterfall_step_heading(row)

            if column_name in {"Prior", "Prior match weight"}:
                cumulative_sum = weight
                summary_lines.append(f"{heading}: {self._format_weight(weight)}")
                continue

            if column_name == "Final score":
                probability = row.get("prob")
                if not isinstance(probability, (int, float)):
                    probability = 1.0 / (1.0 + 2.0 ** (-weight))
                summary_lines.append(
                    f"{heading}: {self._format_weight(weight)}. "
                    f"Match probability: {float(probability):.4f}"
                )
                continue

            cumulative_sum += weight
            summary_lines.append(
                f"{heading}: {self._format_signed_weight(weight)}. "
                f"Cumulative sum: {self._format_weight(cumulative_sum)}"
            )

        if len(summary_lines) == 1:
            return None

        return "\n".join(summary_lines)

    def _maybe_display_messy_id_waterfall_charts(
        self,
        *,
        messy_id: str | int,
        best_id: str | int | None,
        true_id: str | int | None,
    ) -> dict[str, Any]:
        stage = self._owner._splink_stage
        if stage is None or getattr(stage, "linker", None) is None:
            return {"waterfalls": [], "warnings": []}

        try:
            from IPython.display import display
        except ImportError:
            return {"waterfalls": [], "warnings": []}

        best_is_true_match = best_id is not None and best_id == true_id
        charts_to_show: list[tuple[str, str | int]] = []
        waterfall_results: list[dict[str, Any]] = []
        warnings: list[str] = []

        if best_id is not None:
            charts_to_show.append(
                (
                    (
                        "Best and true match waterfall chart"
                        if best_is_true_match
                        else "Best match waterfall chart"
                    ),
                    best_id,
                )
            )

        if true_id is not None and not best_is_true_match:
            charts_to_show.append(("True match waterfall chart", true_id))

        for title, candidate_id in charts_to_show:
            records = self._top_prediction_row_for_unique_ids(
                messy_id=messy_id,
                candidate_id=candidate_id,
            )

            if records.limit(1).fetchone() is None:
                continue

            try:
                chart = self._owner._splink_waterfall_chart(records)
            except ValueError as exc:
                if "retain_intermediate_calculation_columns=True" in str(exc):
                    warnings.append(
                        "Waterfall charts unavailable: set "
                        "retain_intermediate_calculation_columns=True on "
                        "your SplinkStage to retain the intermediate Splink "
                        "comparison columns needed for waterfall charts."
                    )
                continue
            except AttributeError:
                continue

            waterfall_results.append(
                {
                    "title": title,
                    "chart": chart,
                    "text": self._waterfall_text_summary(title, chart),
                    "display": display,
                }
            )

        deduplicated_warnings = list(dict.fromkeys(warnings))
        return {
            "waterfalls": waterfall_results,
            "warnings": deduplicated_warnings,
        }

    def _aligned_record_select_sql(
        self,
        *,
        alias: str,
        available_columns: set[str],
        all_columns: list[str],
        record_type: str,
    ) -> str:
        select_parts = [f"{_sql_literal(record_type)} AS record_type"]
        for column in all_columns:
            if column in available_columns:
                select_parts.append(f"{alias}.{column} AS {column}")
            else:
                select_parts.append(f"NULL AS {column}")
        return ",\n                ".join(select_parts)

    def _clean_features_relation(
        self,
        *,
        messy_id: str | int,
        best_id: str | int | None,
        true_id: str | int | None,
    ) -> DuckDBPyRelation | None:
        messy_relation = self._owner._messy_relation
        canonical_relation = self._owner._canonical_relation

        if messy_relation is None and canonical_relation is None:
            return None

        messy_columns = messy_relation.columns if messy_relation else []
        canonical_columns = canonical_relation.columns if canonical_relation else []
        all_columns = list(dict.fromkeys([*messy_columns, *canonical_columns]))
        tf_columns = [
            column
            for column in [
                "tf_numeric_token_1",
                "tf_numeric_token_2",
                "tf_numeric_token_3",
            ]
            if column in all_columns
        ]
        all_columns = [
            column for column in all_columns if column not in set(tf_columns)
        ] + tf_columns
        selects: list[str] = []
        best_is_true_match = best_id is not None and best_id == true_id

        if messy_relation is not None:
            selects.append(
                f"""
                SELECT
                    {
                    self._aligned_record_select_sql(
                        alias="messy",
                        available_columns=set(messy_columns),
                        all_columns=all_columns,
                        record_type="Messy",
                    )
                }
                FROM ({messy_relation.sql_query()}) AS messy
                WHERE messy.unique_id = {_sql_literal(messy_id)}
                """
            )

        if canonical_relation is not None and best_id is not None:
            selects.append(
                f"""
                SELECT
                    {
                    self._aligned_record_select_sql(
                        alias="canonical",
                        available_columns=set(canonical_columns),
                        all_columns=all_columns,
                        record_type=(
                            "Best and True Match" if best_is_true_match else "Best Match"
                        ),
                    )
                }
                FROM ({canonical_relation.sql_query()}) AS canonical
                WHERE canonical.unique_id = {_sql_literal(best_id)}
                """
            )

        if (
            canonical_relation is not None
            and true_id is not None
            and not best_is_true_match
        ):
            selects.append(
                f"""
                SELECT
                    {
                    self._aligned_record_select_sql(
                        alias="canonical",
                        available_columns=set(canonical_columns),
                        all_columns=all_columns,
                        record_type="True Match",
                    )
                }
                FROM ({canonical_relation.sql_query()}) AS canonical
                WHERE canonical.unique_id = {_sql_literal(true_id)}
                """
            )

        if not selects:
            return None

        union_sql = "\nUNION ALL\n".join(selects)
        return self._owner.con.sql(f"""
            SELECT *
            FROM (
                {union_sql}
            ) AS clean_features
            ORDER BY CASE record_type
                WHEN 'Messy' THEN 1
                WHEN 'Best and True Match' THEN 2
                WHEN 'Best Match' THEN 2
                WHEN 'True Match' THEN 3
                ELSE 99
            END,
            unique_id,
            ukam_address_id
        """)

    def _distinguishability_relation(
        self,
        *,
        messy_id: str | int,
        best_id: str | int | None,
        true_id: str | int | None,
    ) -> DuckDBPyRelation:
        selects: list[str] = []
        table_name = self._splink_improved_predictions_table()
        best_is_true_match = best_id is not None and best_id == true_id

        if best_id is not None:
            record_type = (
                "Messy vs Best and True Match"
                if best_is_true_match
                else "Messy vs Best Match"
            )
            selects.append(
                f"""
                SELECT
                    '{record_type}' AS record_type,
                    *
                FROM {table_name}
                WHERE unique_id_r = {_sql_literal(messy_id)}
                AND unique_id_l = {_sql_literal(best_id)}
                """
            )

        if true_id is not None and not best_is_true_match:
            selects.append(
                f"""
                SELECT
                    'Messy vs True Match' AS record_type,
                    *
                FROM {table_name}
                WHERE unique_id_r = {_sql_literal(messy_id)}
                AND unique_id_l = {_sql_literal(true_id)}
                """
            )

        if not selects:
            return self._owner.con.sql(
                "SELECT 'No distinguishability rows available' AS record_type"
            )

        union_sql = "\nUNION ALL\n".join(selects)
        return self._owner.con.sql(f"""
            SELECT *
            FROM (
                {union_sql}
            ) AS distinguishability_rows
            ORDER BY CASE record_type
                WHEN 'Messy vs Best and True Match' THEN 1
                WHEN 'Messy vs Best Match' THEN 1
                WHEN 'Messy vs True Match' THEN 2
                ELSE 99
            END,
            unique_id_l,
            ukam_address_id_l
        """)

    def splink_results_for_messy_id(
        self,
        messy_id: str | int,
    ) -> DuckDBPyRelation:
        emitted_row = self._query_one_row(f"""
            SELECT match_reason
            FROM ({self._owner._relation.sql_query()}) AS matches
            WHERE unique_id = {_sql_literal(messy_id)}
        """)

        if emitted_row is None:
            raise ValueError(
                f"No emitted match result exists for messy_id={messy_id!r}. "
                "Check that the ID is present in result.matches()."
            )

        match_reason = emitted_row["match_reason"]
        if match_reason != self._splink_match_reason:
            raise ValueError(
                f"messy_id={messy_id!r} was emitted as {match_reason!r}, not "
                f"{self._splink_match_reason!r}. Only rows whose final emitted "
                "match came from Splink can be inspected with this method."
            )

        table_name = self._splink_best_matches_table()
        best_columns = self._owner.con.table(table_name).columns
        messy_clean_sql = (
            "best.clean_full_address_r"
            if "clean_full_address_r" in best_columns
            else "NULL::VARCHAR"
        )
        canonical_clean_sql = (
            "best.clean_full_address_l"
            if "clean_full_address_l" in best_columns
            else "NULL::VARCHAR"
        )
        messy_address_sql = messy_clean_sql
        canonical_address_sql = canonical_clean_sql
        relation_joins = ""
        if self._owner._messy_relation is not None:
            messy_address_sql = (
                f"COALESCE(messy.original_address_concat, {messy_clean_sql})"
            )
            relation_joins += f"""
                LEFT JOIN ({self._owner._messy_relation.sql_query()}) AS messy
                    ON messy.ukam_address_id = best.ukam_address_id_r
            """
        if self._owner._canonical_relation is not None:
            if "original_address_concat" in self._owner._canonical_relation.columns:
                canonical_address_sql = (
                    f"COALESCE(canonical.original_address_concat, {canonical_clean_sql})"
                )
            relation_joins += f"""
                LEFT JOIN ({self._owner._canonical_relation.sql_query()}) AS canonical
                    ON canonical.ukam_address_id = best.ukam_address_id_l
            """
        return self._owner.con.sql(f"""
            SELECT
                best.candidate_rank,
                {messy_address_sql} AS original_address_messy,
                best.postcode_r AS original_postcode_messy,
                {canonical_address_sql} AS original_address_canonical,
                best.postcode_l AS original_postcode_canonical,
                best.match_weight,
                best.distinguishability,
                best.unique_id_r,
                best.unique_id_l,
                best.ukam_address_id_r,
                best.ukam_address_id_l
            FROM {table_name} AS best
            {relation_joins}
            WHERE best.unique_id_r = {_sql_literal(messy_id)}
            ORDER BY best.candidate_rank ASC, best.match_weight DESC,
                best.unique_id_l, best.ukam_address_id_l
        """)

    def messy_id_report(
        self,
        messy_id: str | int,
        *,
        display_output: bool = True,
        charts_as_text: bool = False,
    ) -> dict[str, Any]:
        emitted_row = self._query_one_row(f"""
            SELECT *
            FROM ({self._owner._relation.sql_query()}) AS matches
            WHERE unique_id = {_sql_literal(messy_id)}
        """)

        if emitted_row is None:
            raise ValueError(
                f"No emitted match result exists for messy_id={messy_id!r}. "
                "Check that the ID is present in result.matches()."
            )

        match_reason = emitted_row.get("match_reason")
        if match_reason != self._splink_match_reason:
            raise ValueError(
                f"messy_id={messy_id!r} was emitted as {match_reason!r}, not "
                f"{self._splink_match_reason!r}. Only rows whose final emitted "
                "match came from Splink can be reported with this method."
            )

        best_id = emitted_row.get("resolved_canonical_id")
        true_id = emitted_row.get("ukam_label")
        best_is_true_match = best_id is not None and best_id == true_id
        is_true_positive = best_is_true_match

        messy_row = None
        if self._owner._messy_relation is not None:
            messy_row = self._query_one_row(f"""
                SELECT *
                FROM ({self._owner._messy_relation.sql_query()}) AS messy
                WHERE unique_id = {_sql_literal(messy_id)}
            """)

        best_variants: list[dict[str, Any]] = []
        true_variants: list[dict[str, Any]] = []
        canonical_order_column = "clean_full_address"
        if (
            self._owner._canonical_relation is not None
            and "original_address_concat" in self._owner._canonical_relation.columns
        ):
            canonical_order_column = "original_address_concat"
        if self._owner._canonical_relation is not None and best_id is not None:
            best_variants = self._query_rows(f"""
                SELECT *
                FROM ({self._owner._canonical_relation.sql_query()}) AS canonical
                WHERE unique_id = {_sql_literal(best_id)}
                ORDER BY ukam_address_id, {canonical_order_column}
            """)
        if self._owner._canonical_relation is not None and true_id is not None:
            true_variants = self._query_rows(f"""
                SELECT *
                FROM ({self._owner._canonical_relation.sql_query()}) AS canonical
                WHERE unique_id = {_sql_literal(true_id)}
                ORDER BY ukam_address_id, {canonical_order_column}
            """)

        match_scores = self._owner.con.sql(f"""
            SELECT
                candidate_rank,
                clean_full_address_l AS original_address,
                postcode_l AS postcode,
                match_weight,
                distinguishability,
                unique_id_l AS unique_id,
                ukam_address_id_l AS ukam_address_id
            FROM {self._splink_best_matches_table()}
            WHERE unique_id_r = {_sql_literal(messy_id)}
            ORDER BY candidate_rank, unique_id_l, ukam_address_id_l
        """)

        ranked_rows = self._rows_from_relation(match_scores)
        nth_place_rows = [
            row for row in ranked_rows if (row.get("candidate_rank") or 0) >= 2
        ]

        clean_features = self._clean_features_relation(
            messy_id=messy_id,
            best_id=best_id,
            true_id=true_id,
        )
        distinguishability = self._distinguishability_relation(
            messy_id=messy_id,
            best_id=best_id,
            true_id=true_id,
        )
        waterfall_data = self._maybe_display_messy_id_waterfall_charts(
            messy_id=messy_id,
            best_id=best_id,
            true_id=true_id,
        )

        summary_groups: list[tuple[str, list[str]]] = [
            (
                "Messy address is:",
                [
                    self._format_address_summary(
                        messy_row or emitted_row,
                        include_ukam_label=True,
                    )
                ],
            )
        ]

        if best_is_true_match:
            summary_groups.append(
                (
                    "The best and true match:",
                    self._format_variant_lines(best_variants),
                )
            )
        else:
            summary_groups.extend(
                [
                    ("The true match:", self._format_variant_lines(true_variants)),
                    ("The best match:", self._format_variant_lines(best_variants)),
                ]
            )

        ranked_entries = self._format_ranked_match_entries(nth_place_rows)
        summary_groups.append(
            (
                "nth place matches - ",
                [content for _, content in ranked_entries],
            )
        )

        aligned_summary_lines = self._align_detail_column(
            [line for _, lines in summary_groups for line in lines]
        )

        summary_sections: list[str] = []
        line_offset = 0
        for label, lines in summary_groups:
            group_lines = aligned_summary_lines[line_offset : line_offset + len(lines)]
            line_offset += len(lines)

            if label == "nth place matches - ":
                summary_sections.extend(
                    self._format_labelled_ranked_lines(
                        label,
                        list(
                            zip(
                                [rank_label for rank_label, _ in ranked_entries],
                                group_lines,
                            )
                        ),
                    )
                )
                continue

            summary_sections.extend(self._format_labelled_lines(label, group_lines))

        sections = [
            (
                "True positive: the model found the correct match"
                if is_true_positive
                else "False positive: the model found the wrong match"
            ),
            "",
            *summary_sections,
            "",
            "## Match scores:",
            self._relation_to_ascii(match_scores),
            "",
            "## Features from clean table:",
            (
                self._relation_to_ascii(clean_features)
                if clean_features is not None
                else "Clean table relations unavailable"
            ),
            "",
            "## Distinguishability:",
            self._relation_to_ascii(distinguishability),
        ]

        report = "\n".join(sections).rstrip()

        if display_output:
            print(report)  # noqa: T201
            for warning in waterfall_data["warnings"]:
                print(f"\nWarning: {warning}")  # noqa: T201
            for waterfall in waterfall_data["waterfalls"]:
                if charts_as_text and waterfall.get("text"):
                    print(f"\n{waterfall['text']}")  # noqa: T201
                    continue
                waterfall["display"](waterfall["title"])
                waterfall["display"](waterfall["chart"])

        return {
            "report": report,
            "waterfalls": [
                {
                    "title": waterfall["title"],
                    "chart": waterfall["chart"],
                    "text": waterfall.get("text"),
                }
                for waterfall in waterfall_data["waterfalls"]
            ],
            "warnings": waterfall_data["warnings"],
        }
