from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from splink.internals.comparison import Comparison

if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.settings import Settings


def extend_unique_columns(
    columns: list[str], additions: list[str] | tuple[str, ...]
) -> None:
    for addition in additions:
        if addition not in columns:
            columns.append(addition)


@dataclass
class PreSplinkIntegration:
    name: str
    comparison_output_column: str | None = None
    required_match_columns: tuple[str, ...] = ()
    core_feature_columns: tuple[str, ...] = ()
    comparison: Comparison | None = field(default=None, init=False, repr=False)

    @property
    def retained_feature_columns(self) -> tuple[str, ...]:
        return self.core_feature_columns

    def pop_comparison_dict(self, settings_as_dict: dict) -> dict | None:
        if self.comparison_output_column is None:
            return None

        comparisons = settings_as_dict.get("comparisons")
        if not isinstance(comparisons, list):
            return None

        for index, comparison in enumerate(comparisons):
            if not isinstance(comparison, dict):
                continue
            if comparison.get("output_column_name") != self.comparison_output_column:
                continue
            return comparisons.pop(index)

        return None

    def build_comparison(
        self,
        *,
        comparison_dict: dict,
        linker: Linker,
    ) -> Comparison:
        return Comparison(
            comparison_levels=comparison_dict["comparison_levels"],
            sqlglot_dialect=linker._sql_dialect_str,
            output_column_name=comparison_dict["output_column_name"],
            comparison_description=comparison_dict.get("comparison_description"),
            column_info_settings=linker._settings_obj.column_info_settings,
        )

    def attach_to_linker(
        self,
        *,
        linker: Linker,
        comparison_dict: dict | None,
    ) -> None:
        if comparison_dict is None:
            return
        self.comparison = self.build_comparison(
            comparison_dict=comparison_dict,
            linker=linker,
        )

    def enqueue_blocked_pair_feature_sql(
        self,
        pipeline: CTEPipeline,
        *,
        input_table: str,
        nodes_table: str,
    ) -> str:
        raise NotImplementedError

    def enqueue_comparison_vector_sql(
        self,
        pipeline: CTEPipeline,
        *,
        input_table: str,
    ) -> str:
        if self.comparison is None:
            return input_table

        table_name = f"__ukam__comparison_vectors_with_{self.name}"
        pipeline.enqueue_sql(
            f"""
            SELECT
                cv.*,
                {self.comparison._case_statement}
            FROM {input_table} AS cv
            """,
            table_name,
        )
        return table_name

    def extend_match_weight_part_columns(
        self,
        columns: list[str],
    ) -> list[str]:
        extend_unique_columns(columns, self.retained_feature_columns)
        if self.comparison is None:
            return []

        columns.append(self.comparison._gamma_column_name)
        bf_column_name = self.comparison._bf_column_name
        bf_sql = " ".join(
            level._bayes_factor_sql(self.comparison._gamma_column_name)
            for level in self.comparison.comparison_levels
        )
        columns.append(f"CASE {bf_sql} END as {bf_column_name}")
        return [bf_column_name]

    def extend_predict_columns(
        self,
        columns: list[str],
        *,
        settings: Settings,
    ) -> None:
        extend_unique_columns(columns, self.retained_feature_columns)
        if self.comparison is None:
            return

        if settings._retain_matching_columns:
            extend_unique_columns(columns, [self.comparison._gamma_column_name])
        if settings._retain_intermediate_calculation_columns:
            extend_unique_columns(columns, [self.comparison._bf_column_name])
