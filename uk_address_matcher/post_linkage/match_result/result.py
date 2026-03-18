from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Literal

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.post_linkage.analyse_results import _calculate_match_metrics
from uk_address_matcher.post_linkage.match_result.splink_inspector import (
    _SplinkInspector,
)

if TYPE_CHECKING:
    from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

_SPLINK_MATCH_REASON = "splink: probabilistic match"


def _build_threshold_metrics_sql(rounding_expr: str) -> str:
    """Return threshold metrics SQL parameterised by the score-rounding expression.

    This query evaluates top-1 outcomes (one emitted decision per input row):

    - A row is "positive" when ``ukam_label`` exists in the canonical dataset.
    - A row is a true positive when the emitted ``resolved_canonical_id`` equals
      ``ukam_label`` and the decision score is above threshold.
    - Wrong-ID rows are treated as false positives at the emitted score, while
      also reducing recall via ``FN = P - TP``.

    This means wrong-ID rows are not floored to ``-999``; their emitted score is
    retained so precision-recall thresholding behaves as expected for top-1
    systems.

    Two FP concepts are tracked separately:

    - ``fp`` (fp_all): every non-TP accepted row, used for precision.
    - ``fp_neg``: non-TP rows whose ``ukam_label`` is *not* in the canonical
      dataset (genuine negatives wrongly accepted).  Used to derive ``tn``,
      ``tn_rate``, and ``fp_rate``.

    Separating the two prevents TN from going negative when matchable records
    receive wrong-ID decisions, because wrong-ID rows inflate ``fp`` but should
    not reduce the count of correctly-rejected genuine negatives.
    """
    return f"""
    WITH canonical_ids AS (
        SELECT DISTINCT unique_id FROM __ukam_threshold_canonical__
    ),
    labelled AS (
        SELECT
            m.unique_id,
            CASE WHEN c.unique_id IS NOT NULL THEN 1 ELSE 0 END AS clerical_positive,
            CASE
                WHEN c.unique_id IS NOT NULL
                 AND m.resolved_canonical_id = m.ukam_label
                THEN 1
                ELSE 0
            END AS true_positive_row,
            CASE
                WHEN m.match_reason IS NULL THEN CAST(-999 AS DOUBLE)
                WHEN m.match_reason = '{_SPLINK_MATCH_REASON}' THEN {rounding_expr}
                ELSE CAST(999 AS DOUBLE)
            END AS match_weight_adj
        FROM __ukam_threshold_matches__ m
        LEFT JOIN canonical_ids c ON m.ukam_label = c.unique_id
    ),
    grouped AS (
        SELECT
            match_weight_adj                                           AS truth_threshold,
            SUM(true_positive_row)                                     AS tp_row,
            SUM(1 - true_positive_row)                                 AS not_tp_row,
            SUM(CASE WHEN true_positive_row = 0 AND clerical_positive = 0
                     THEN 1 ELSE 0 END)                                AS fp_neg_at,
            SUM(clerical_positive)                                     AS cp,
            SUM(1 - clerical_positive)                                 AS cn
        FROM labelled
        GROUP BY match_weight_adj
    ),
    stats AS (
        SELECT
            truth_threshold,
            SUM(tp_row)    OVER (ORDER BY truth_threshold DESC)         AS tp,
            SUM(not_tp_row) OVER (ORDER BY truth_threshold DESC)        AS fp,
            SUM(fp_neg_at) OVER (ORDER BY truth_threshold DESC)         AS fp_neg,
            SUM(cp) OVER ()                                             AS p,
            SUM(cn) OVER ()                                             AS n
        FROM grouped
    ),
    truth_space AS (
        SELECT
            truth_threshold,
            p                                                          AS p,
            n                                                          AS n,
            CAST(tp              AS DOUBLE)                            AS tp,
            CAST(fp              AS DOUBLE)                            AS fp,
            CAST(fp_neg          AS DOUBLE)                            AS fp_neg,
            CAST(p - tp          AS DOUBLE)                            AS fn,
            CAST(GREATEST(n - fp_neg, 0) AS DOUBLE)                    AS tn
        FROM stats
    )
    SELECT
        truth_threshold,
        CASE
            WHEN truth_threshold >=  999 THEN 1.0
            WHEN truth_threshold <= -999 THEN 0.0
            ELSE power(2, truth_threshold)
                / (1.0 + power(2, truth_threshold))
        END AS match_probability,
        tp                                                              AS tp,
        tn                                                              AS tn,
        fp                                                              AS fp,
        fp_neg                                                          AS fp_neg,
        fn                                                              AS fn,
        tp / NULLIF(p, 0)                                               AS tp_rate,
        tn / NULLIF(CAST(n AS DOUBLE), 0)                               AS tn_rate,
        fp_neg / NULLIF(CAST(n AS DOUBLE), 0)                           AS fp_rate,
        fn / NULLIF(p, 0)                                               AS fn_rate,
        CASE WHEN tp + fp = 0 THEN 1.0 ELSE tp / (tp + fp) END         AS precision,
        tp / NULLIF(p, 0)                                               AS recall,
        CASE
            WHEN 2.0 * tp + fp + fn = 0 THEN 0.0
            ELSE 2.0 * tp / (2.0 * tp + fp + fn)
        END                                                             AS f1
    FROM truth_space
    ORDER BY truth_threshold ASC
    """


@dataclass
class MatchResult:
    """Wraps match output with connection-scoped inspection helpers.

    Access the underlying DuckDB relation via `.matches()`.

    Key methods:
        match_metrics      - match-reason breakdown with counts and percentages.
        match_reasons      - distinct match-reason values.
        splink_predictions - raw Splink predictions table (requires `SplinkStage`).
    """

    _relation: DuckDBPyRelation
    con: DuckDBPyConnection
    _splink_stage: SplinkStage | None = None
    _canonical_relation: DuckDBPyRelation | None = None

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"{class_name} object.\n"
            "Use .matches() to retrieve your raw results as a DuckDB table."
        )

    def matches(self, *, all_columns: bool = False) -> DuckDBPyRelation:
        """The underlying DuckDB relation containing match results.

        Args:
            all_columns: When True, return every column. By default only the
                key result columns are returned.
        """
        if all_columns:
            return self._relation
        preferred = [
            "unique_id",
            "resolved_canonical_id",
            "ukam_label",
            "original_address_concat",
            "original_address_concat_canonical",
            "match_reason",
            "match_weight",
            "distinguishability",
        ]
        available = set(self._relation.columns)
        cols = [c for c in preferred if c in available]
        return self._relation.select(*cols)

    def match_metrics(
        self,
        *,
        order: Literal["descending", "ascending"] = "descending",
    ) -> DuckDBPyRelation:
        """Match-reason breakdown with counts and percentages"""

        return _calculate_match_metrics(self._relation, order=order)

    def _has_splink(self) -> bool:
        """True when a Splink stage was configured in the pipeline."""
        return self._splink_stage is not None

    def _require_splink_stage(self) -> SplinkStage:
        """Return the configured SplinkStage or raise if unavailable."""
        if self._splink_stage is None:
            raise ValueError(
                "No SplinkStage was configured in the matching pipeline. "
                "Include a SplinkStage in your stages list to enable "
                "Splink inspection."
            )
        return self._splink_stage

    def _require_splink(self) -> _SplinkInspector:
        """Return the Splink inspector or raise if unavailable."""
        stage = self._require_splink_stage()
        if stage.linker is None:
            raise ValueError(
                "SplinkStage is configured but did not run. "
                "This can happen when earlier stages matched all records."
            )
        return _SplinkInspector(con=self.con, stage=stage)

    def _splink_best_matches_table(self) -> str:
        """Return the name of the DuckDB table holding Splink best matches.

        Raises:
            ValueError: When no SplinkStage was configured, or when the stage
                was configured but did not run far enough to create the table.
        """
        stage = self._require_splink_stage()
        if stage.best_matches_table is None:
            raise ValueError(
                "SplinkStage is configured but did not produce a best-matches "
                "table. This can happen when earlier stages matched all "
                "records before the Splink stage ran."
            )
        return stage.best_matches_table

    def _splink_predictions(
        self,
        limit: int | None = None,
        ukam_ids: list[str | int] | None = None,
        *,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
    ) -> DuckDBPyRelation:
        """Splink predictions as a DuckDB relation.

        Use ``ukam_ids`` to filter on the input-side identifier.
        """
        return self._require_splink().predictions(
            limit=limit,
            ukam_ids=ukam_ids,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )

    def accuracy_data(
        self,
        *,
        match_weight_round_to_nearest: float | None = 0.1,
    ) -> list[dict]:
        """Compute threshold metrics swept over every emitted decision score.

        Each row in the returned list corresponds to one threshold value and
        contains the confusion-matrix counts (tp, tn, fp, fn) plus the derived
        rates (tp_rate, fp_rate, precision, recall, f1).

        Important semantics: this uses top-1 outcome evaluation.  Wrong-ID rows
        are false positives at their emitted score; they are not score-floored.
        Recall is derived from true positives as ``TP/P`` (equivalently
        ``FN = P - TP``).

        The ground-truth positive class is determined by looking up each record's
        ``ukam_label`` in the canonical dataset.  A record whose ``ukam_label``
        matches a canonical ``unique_id`` is treated as an expected match;
        all others are treated as expected non-matches.

        The score used as the decision threshold is:

                - ``+999`` for non-splink matches (exact, peeled, trigram).
        - The actual ``match_weight`` for splink probabilistic matches.
                - ``-999`` for rows with ``match_reason`` = ``NULL``.

        Args:
            match_weight_round_to_nearest: Round splink match weights to this
                increment before grouping to reduce the number of threshold
                points.  Pass ``None`` to keep full precision.  Defaults to 0.1.

        Returns:
            List of dicts with keys: ``truth_threshold``, ``match_probability``,
            ``tp``, ``tn``, ``fp``, ``fp_neg``, ``fn``, ``tp_rate``,
            ``tn_rate``, ``fp_rate``, ``fn_rate``, ``precision``, ``recall``,
            ``f1``.

            ``fp`` is every accepted non-TP row (used by ``precision``).
            ``fp_neg`` counts only the accepted rows whose label is absent from
            the canonical dataset; it is used to derive ``tn``, ``tn_rate``,
            and ``fp_rate``.
        """
        if "ukam_label" not in self._relation.columns:
            raise ValueError(
                "accuracy_data requires a 'ukam_label' column in the match results. "
                "Add a ground-truth label column to the input addresses_to_match data."
            )
        if self._canonical_relation is None:
            raise ValueError(
                "accuracy_data requires access to the canonical dataset to determine "
                "the ground-truth positive class.  This is set automatically when "
                "matching via AddressMatcher."
            )

        if match_weight_round_to_nearest is not None:
            rounding_expr = (
                f"CAST({match_weight_round_to_nearest} AS DOUBLE) "
                f"* round(m.match_weight / {match_weight_round_to_nearest})"
            )
        else:
            rounding_expr = "m.match_weight"

        sql = _build_threshold_metrics_sql(rounding_expr)

        self.con.register("__ukam_threshold_matches__", self._relation)
        self.con.register("__ukam_threshold_canonical__", self._canonical_relation)
        try:
            rel = self.con.sql(sql)
            rows = rel.fetchall()
            cols = rel.columns
        finally:
            self.con.unregister("__ukam_threshold_matches__")
            self.con.unregister("__ukam_threshold_canonical__")

        return [dict(zip(cols, row)) for row in rows]

    def accuracy_analysis(
        self,
        *,
        match_weight_round_to_nearest: float | None = 0.1,
        output_type: Literal["threshold_selection", "precision_recall", "table"] = (
            "threshold_selection"
        ),
        add_metrics: List[
            Literal["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]
        ]
        | None = None,
    ) -> Any:
        """Generate an accuracy chart or table from labelled match results.

        Mirrors Splink's ``linker.evaluation.accuracy_analysis_from_labels_table``
        API.  Requires a ``ukam_label`` column in the input addresses.

        Args:
            match_weight_round_to_nearest: Round splink match weights to this
                increment before grouping.  Pass ``None`` for full precision.
                Defaults to 0.1.
            output_type: One of:

                - ``"threshold_selection"`` *(default)* — interactive panel
                  showing precision/recall curves against match-weight threshold.
                - ``"precision_recall"`` — precision vs recall curve.
                - ``"table"`` — the raw truth-space data as a list of dicts.

                ``"roc"`` is intentionally not supported here because this
                record-level evaluation does not observe the full negative-pair
                universe, making TN-dependent ROC interpretation unreliable.

            add_metrics: Extra metrics to include in the ``"threshold_selection"``
                chart.  Accepted values: ``"specificity"``, ``"npv"``,
                ``"accuracy"``, ``"f1"``, ``"f2"``, ``"f0_5"``, ``"p4"``, ``"phi"``.

        Returns:
            An Altair chart, or a list of dicts when ``output_type="table"``.
        """
        from splink.internals.charts import (
            precision_recall_chart as _splink_pr_chart,
            threshold_selection_tool as _splink_threshold_tool,
        )

        if add_metrics is None:
            add_metrics = []

        records = self.accuracy_data(
            match_weight_round_to_nearest=match_weight_round_to_nearest
        )

        if output_type == "threshold_selection":
            return _splink_threshold_tool(records, add_metrics=add_metrics)
        elif output_type == "precision_recall":
            return _splink_pr_chart(records)
        elif output_type == "table":
            return records
        else:
            raise ValueError(
                "Invalid output_type. Allowed values are: "
                "'threshold_selection', 'precision_recall', 'table'."
            )

    def _splink_waterfall_chart(
        self,
        records: Any,
        *,
        filter_nulls: bool = True,
        remove_sensitive_data: bool = False,
        as_dict: bool = False,
    ) -> Any:
        """Splink waterfall chart for prediction records.

        ``records`` must match the structure returned by Splink's
        ``as_record_dict``. DuckDB relations and Splink prediction dataframes
        are converted to dictionaries automatically.

        Requires ``retain_intermediate_calculation_columns=True`` on your
        ``SplinkStage`` so that the comparison-vector columns needed by the
        waterfall are present in the predictions table.
        """
        stage = self._require_splink_stage()
        record_dicts = _ensure_record_dicts(records)

        try:
            return stage.linker.visualisations.waterfall_chart(
                record_dicts,
                filter_nulls=filter_nulls,
                remove_sensitive_data=remove_sensitive_data,
                as_dict=as_dict,
            )
        except ValueError as e:
            if "retain_intermediate_calculation_columns" in str(e):
                raise ValueError(
                    "Waterfall charts require "
                    "retain_intermediate_calculation_columns=True on your "
                    "SplinkStage. For example:\n\n"
                    "    SplinkStage(\n"
                    "        retain_intermediate_calculation_columns=True,\n"
                    "        ...\n"
                    "    )"
                ) from e
            raise


def _ensure_record_dicts(records: Any) -> list[dict[str, Any]]:
    if isinstance(records, DuckDBPyRelation):
        rows = records.fetchall()
        columns = records.columns
        return [dict(zip(columns, row)) for row in rows]
    if hasattr(records, "as_record_dict"):
        return records.as_record_dict()
    if isinstance(records, list):
        if not records:
            return records
        if isinstance(records[0], dict):
            return records
    raise TypeError(
        "Waterfall charts expect a DuckDB relation, a Splink predictions "
        "dataframe with as_record_dict(), or a list of record dictionaries."
    )
