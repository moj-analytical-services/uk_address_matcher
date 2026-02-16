from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from benchmarking.datasets.registry import DatasetInfo
from benchmarking.datasets.sources import SourceConfig, resolve_s3_path
from benchmarking.utils.io import load_duckdb_httpfs

if TYPE_CHECKING:
    import duckdb


def _strip_decimal_suffix(expr: str) -> str:
    """Remove trailing `.0` artefacts created when numeric IDs load as doubles."""
    pattern = r"\.0+$"
    return f"NULLIF(REGEXP_REPLACE(TRIM({expr}), '{pattern}', ''), '')"


# Source configurations for Lambeth datasets
_LAMBETH_SOURCES = (
    SourceConfig(
        name="council_tax",
        s3_key="ctax.parquet",
        unique_id_column="UPRN",
        ukam_label_column="UPRN",
        postcode_column="POSTCODE",
        address_columns=["ADDR1", "ADDR2", "ADDR3", "ADDR4"],
        unique_id_formatter=_strip_decimal_suffix,
        optional_filter="uprn != '10090204019'",
    ),
    SourceConfig(
        name="electoral_register",
        s3_key="elecreg.parquet",
        unique_id_column="Unique property reference number (UPRN)",
        ukam_label_column="Unique property reference number (UPRN)",
        postcode_column="Postcode",
        address_columns=[
            "Address 1",
            "Address 2",
            "Address 3",
            "Address 4",
        ],
        unique_id_formatter=_strip_decimal_suffix,
    ),
    SourceConfig(
        name="local_land_and_property_gazetteer",
        s3_key="llpg.parquet",
        unique_id_column="UPRN_BLPU",
        ukam_label_column="UPRN_BLPU",
        postcode_column="Postcode_LPI",
        address_columns=["Address_LPI"],
        prune_postcode_from_address=True,
    ),
)


LAMBETH_COUNCIL_INFO = DatasetInfo(
    name="Lambeth Council",
    description=(
        "Council tax and electoral register records from Lambeth Borough Council"
    ),
    source="S3: linking bucket (Lambeth labelled data)",
    notes=(
        "Data includes multiple source types (council tax, electoral register) "
        "with varying data quality"
    ),
)


@lru_cache(maxsize=None)
def get_lambeth_council_data(
    con: duckdb.DuckDBPyConnection,
    sample_mode: bool = False,
) -> duckdb.DuckDBPyRelation:
    base_path = resolve_s3_path("UKAM_LAMBETH_S3_BASE_PATH", "UKAM_LAMBETH_DATA_PATH")
    if base_path.startswith("s3://"):
        load_duckdb_httpfs(con)
    print(f"Reading Lambeth datasets from: {base_path}")
    union_sql = "\nUNION ALL\n".join(
        cfg.select_statement(base_path) for cfg in _LAMBETH_SOURCES
    )
    df_messy_raw = con.sql(union_sql).filter("unique_id IS NOT NULL")

    # Apply deterministic sampling if requested (pushed down to SQL for efficiency)
    # Use hash-based sampling to get a representative spread across all sources
    if sample_mode:
        df_messy_raw = con.sql(
            """
            SELECT * FROM df_messy_raw
            WHERE hash(unique_id || dataset_name) % 100 < 10
            ORDER BY unique_id, dataset_name
            LIMIT 10000
            """
        )

    return df_messy_raw
