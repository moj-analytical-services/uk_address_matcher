from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING

from benchmarking.datasets.registry import DatasetInfo
from benchmarking.datasets.sources import SourceConfig
from benchmarking.utils.io import get_env_setting, load_duckdb_httpfs

if TYPE_CHECKING:
    import duckdb


def _resolve_lambeth_s3_base_path() -> str:
    """Return the base S3 path for the Lambeth dataset from private settings."""

    explicit = os.getenv("UKAM_LAMBETH_S3_BASE_PATH")
    if explicit is not None and explicit.strip():
        path = explicit.strip().rstrip("/")
    else:
        prefix = get_env_setting("UKAM_S3_BASE_PREFIX")
        relative = get_env_setting("UKAM_LAMBETH_DATA_PATH")
        if not prefix.strip() or not relative.strip():
            raise RuntimeError(
                "Both UKAM_S3_BASE_PREFIX and UKAM_LAMBETH_DATA_PATH must be set "
                "to non-empty values."
            )
        path = f"{prefix.strip().rstrip('/')}/{relative.strip().lstrip('/')}"

    return path if path.endswith("/") else f"{path}/"


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
        postcode_column="POSTCODE",
        address_columns=["ADDR1", "ADDR2", "ADDR3", "ADDR4"],
        unique_id_formatter=_strip_decimal_suffix,
    ),
    SourceConfig(
        name="electoral_register",
        s3_key="elecreg.parquet",
        unique_id_column="Unique property reference number (UPRN)",
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
) -> duckdb.DuckDBPyRelation:
    # Ensure httpfs extension is loaded for S3 access
    load_duckdb_httpfs(con)

    base_path = _resolve_lambeth_s3_base_path()
    print(f"Reading Lambeth datasets from S3: {base_path}")
    union_sql = "\nUNION ALL\n".join(
        cfg.select_statement(base_path) for cfg in _LAMBETH_SOURCES
    )
    df_messy_raw = con.sql(union_sql).filter("unique_id IS NOT NULL")

    return df_messy_raw
