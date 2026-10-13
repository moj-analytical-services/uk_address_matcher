"""Exact global support for the next two suffix segments of a local first cut."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.materialisation import _CanonicalIntermediates
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@pipeline_stage(name="hierarchy_tails", description="Deduplicate locally shared suffixes")
def _hierarchy_tails():
    return [
        CTEStep(
            "final",
            """
        SELECT DISTINCT common_adj_start_tokens AS tokens,
            array_to_string(common_adj_start_tokens, ' ') AS tail,
            __shared_suffix_depth AS depth
        FROM {input} WHERE __shared_suffix_depth > 0
    """,
        )
    ]


@pipeline_stage(name="hierarchy_wanted", description="Encode required suffix ancestors")
def _hierarchy_wanted():
    return [
        CTEStep(
            "expanded",
            """
            SELECT tokens, unnest(range(1, depth + 1)) AS depth FROM {input}
        """,
        ),
        CTEStep(
            "distinct_suffixes",
            """
            SELECT DISTINCT array_to_string(
                list_slice(tokens, len(tokens) - depth + 1, len(tokens)), ' '
            ) AS suffix FROM {expanded}
        """,
        ),
        CTEStep(
            "final",
            """
            SELECT suffix, row_number() OVER ()::BIGINT AS suffix_id
            FROM {distinct_suffixes}
        """,
        ),
    ]


@pipeline_stage(
    name="hierarchy_support", description="Count distinct properties globally"
)
def _hierarchy_support():
    return [
        CTEStep(
            "depths",
            """
            SELECT unique_id, clean_full_address_tokens AS tokens,
                unnest(range(1, len(clean_full_address_tokens) + 1)) AS depth
            FROM {canonical}
        """,
        ),
        CTEStep(
            "expanded",
            """
            SELECT unique_id, array_to_string(
                list_slice(tokens, len(tokens) - depth + 1, len(tokens)), ' '
            ) AS suffix FROM {depths}
        """,
        ),
        CTEStep(
            "matched",
            """
            SELECT w.suffix_id, e.unique_id FROM {expanded} e
            INNER JOIN {wanted} w USING (suffix)
        """,
        ),
        CTEStep(
            "counted",
            """
            SELECT suffix_id, count(DISTINCT unique_id)::BIGINT AS support
            FROM {matched} GROUP BY suffix_id
        """,
        ),
        CTEStep(
            "final",
            """
            SELECT w.suffix, c.support FROM {counted} c
            JOIN {wanted} w USING (suffix_id)
        """,
        ),
    ]


@pipeline_stage(name="hierarchy_parents", description="Select the next support increase")
def _hierarchy_parents():
    return [
        CTEStep(
            "depths",
            """
            SELECT *, unnest(range(1, depth)) AS parent_depth FROM {tails}
        """,
        ),
        CTEStep(
            "possibilities",
            """
            SELECT t.tail, t.parent_depth, p.support AS parent_support
            FROM {depths} t JOIN {support} s ON s.suffix = t.tail
            JOIN {support} p ON p.suffix = array_to_string(list_slice(
                t.tokens, len(t.tokens) - t.parent_depth + 1, len(t.tokens)), ' ')
            WHERE p.support > s.support
        """,
        ),
        CTEStep(
            "selected",
            """
            SELECT *, row_number() OVER (
                PARTITION BY tail ORDER BY parent_depth DESC
            ) AS choice FROM {possibilities}
        """,
        ),
        CTEStep(
            "final",
            """
            SELECT t.tail, t.tokens, t.depth,
                coalesce(p.parent_depth, 0)::BIGINT AS parent_depth,
                struct_pack(
                    tokens := list_slice(t.tokens, 1,
                        t.depth - coalesce(p.parent_depth, 0)),
                    support := s.support
                ) AS shared_part
            FROM {tails} t JOIN {support} s ON s.suffix = t.tail
            LEFT JOIN {selected} p ON p.tail = t.tail AND p.choice = 1
        """,
        ),
    ]


@pipeline_stage(
    name="hierarchy_third_tails", description="Deduplicate remaining parent suffixes"
)
def _hierarchy_third_tails():
    return [
        CTEStep(
            "final",
            """
        SELECT DISTINCT array_to_string(list_slice(tokens,
            depth - parent_depth + 1, depth), ' ') AS tail,
            list_slice(tokens, depth - parent_depth + 1, depth) AS tokens,
            parent_depth AS depth
        FROM {input} WHERE parent_depth > 0
    """,
        )
    ]


@pipeline_stage(name="hierarchy_output", description="Attach supported suffix segments")
def _hierarchy_output(*, depth: Literal[2, 3]):
    third_column = ""
    third_join = ""
    if depth == 3:
        third_column = """
            , CASE WHEN a.__shared_suffix_depth > 0 AND p.parent_depth > 0
                THEN q.shared_part END AS third_shared_part
        """
        third_join = """
            LEFT JOIN {third_parents} q ON q.tail = array_to_string(
                list_slice(p.tokens, p.depth - p.parent_depth + 1, p.depth), ' ')
        """
    return [
        CTEStep(
            "final",
            """
        SELECT a.* EXCLUDE (__shared_suffix_depth),
            CASE WHEN a.__shared_suffix_depth > 0 THEN p.shared_part
            END AS second_shared_part
    """
            + third_column
            + """
        FROM {adjacent} a LEFT JOIN {parents} p
            ON array_to_string(a.common_adj_start_tokens, ' ') = p.tail
    """
            + third_join,
        )
    ]


def _materialise_distinguishing_hierarchy(
    con: DuckDBPyConnection,
    canonical: DuckDBPyRelation,
    adjacent_table: str,
    *,
    depth: Literal[2, 3],
    output_table: str | None = None,
    debug_options: DebugOptions | None = None,
    _storage: _CanonicalIntermediates | None = None,
) -> DuckDBPyRelation:
    """Retain first-cut batch boundaries, but count support across all batches.

    Only required suffixes enter the distinct-property aggregate. Materialising
    its integer dictionary and counts avoids repeating the expensive aggregate
    for either parent lookup. The third level reuses those same counts.
    """
    from uk_address_matcher.cleaning.chunking_strategies import (
        _drop_materialised_relation,
        _materialise_relation,
    )

    names: list[str] = []

    def run(stage, *, output_name=None, **relations):
        name = output_name or f"__ukam_hierarchy_{_uid()}"
        names.append(name)
        pipeline = create_sql_pipeline(
            con,
            [InputBinding(key, value) for key, value in relations.items()],
            [stage],
        )
        return _materialise_relation(
            con,
            pipeline.run(debug_options),
            name,
            _storage=_storage,
            order_by="ukam_address_id" if output_name else None,
        )

    try:
        adjacent = con.table(adjacent_table)
        tails = run(_hierarchy_tails, adjacent=adjacent)
        wanted = run(_hierarchy_wanted, tails=tails)
        support = run(_hierarchy_support, canonical=canonical, wanted=wanted)
        parents = run(_hierarchy_parents, tails=tails, support=support)
        output_inputs = {"adjacent": adjacent, "parents": parents}
        if depth == 3:
            third_tails = run(_hierarchy_third_tails, parents=parents)
            output_inputs["third_parents"] = run(
                _hierarchy_parents,
                tails=third_tails,
                support=support,
            )
        # Persist to a fresh file path before dropping the bound first-cut input.
        # The caller owns this output through subsequent preparation and export.
        final_name = output_table or f"__ukam_supported_suffixes_{_uid()}"
        if final_name == adjacent_table:
            raise ValueError("Hierarchy output requires a fresh table name")
        output = run(
            _hierarchy_output(depth=depth), output_name=final_name, **output_inputs
        )
        _drop_materialised_relation(con, adjacent_table, _storage)
        names.remove(final_name)  # Output ownership transfers to the caller.
        return output
    finally:
        for name in reversed(names):
            _drop_materialised_relation(con, name, _storage)


@pipeline_stage(
    name="attach_distinguishing_hierarchy",
    description="Attach supported segments after term-frequency processing",
)
def _attach_distinguishing_hierarchy(*, depth: Literal[2, 3]):
    columns = ("second_shared_part", "third_shared_part")[: depth - 1]
    projection = ", ".join(f"h.{column}" for column in columns)
    return [
        CTEStep(
            "final",
            """
        SELECT p.*, """
            + projection
            + """
        FROM {processed} p LEFT JOIN {hierarchy} h USING (ukam_address_id)
    """,
        )
    ]
