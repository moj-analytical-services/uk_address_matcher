from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarking.config.sources import resolve_data_source
from benchmarking.utils.io import load_duckdb_excel, load_duckdb_httpfs

if TYPE_CHECKING:
    import duckdb


def maybe_enable_s3_for_path(
    con: duckdb.DuckDBPyConnection,
    base_path: str,
) -> None:
    if not base_path.startswith("s3://"):
        return

    load_duckdb_httpfs(con)


def _normalise_uprn_expr(expr: str) -> str:
    pattern = r"\\.0+$"
    return f"""
        COALESCE(
            NULLIF(CAST(TRY_CAST({expr} AS BIGINT) AS VARCHAR), ''),
            NULLIF(REGEXP_REPLACE(TRIM(CAST({expr} AS VARCHAR)), '{pattern}', ''), '')
        )
    """


_DATASETS: dict[str, dict[str, str]] = {
    "aberdeenshire": {
        "label": "Aberdeenshire council tax",
        "file_name": "ABERDEENSHIRE_CTBANDS_ONSUD_202512.csv",
        "data_path_env": "UKAM_ABERDEENSHIRE_DATA_PATH",
    },
    "hackney": {
        "label": "Hackney council tax",
        "file_name": "HACKNEY_CTBANDS_ONSUD_202507.csv",
        "data_path_env": "UKAM_HACKNEY_DATA_PATH",
    },
    "lambeth_council_tax": {
        "label": "Lambeth council tax",
        "file_name": "ctax.parquet",
        "data_path_env": "UKAM_LAMBETH_DATA_PATH",
    },
    "lambeth_electoral_register": {
        "label": "Lambeth electoral register",
        "file_name": "elecreg.parquet",
        "data_path_env": "UKAM_LAMBETH_DATA_PATH",
    },
    "lambeth_llpg": {
        "label": "Lambeth LLPG",
        "file_name": "llpg.parquet",
        "data_path_env": "UKAM_LAMBETH_DATA_PATH",
    },
    "mid_sussex": {
        "label": "Mid Sussex business rates",
        "file_name": "mid_sussex_district_council_redacted-database-020226.xlsx",
        "data_path_env": "UKAM_MID_SUSSEX_DATA_PATH",
    },
    "rhondda": {
        "label": "Rhondda council tax",
        "file_name": "RHONDDA_CYNON_TAF_CTBANDS_ONSUD_202512.csv",
        "data_path_env": "UKAM_RHONDDA_DATA_PATH",
    },
    "pooled_councils": {
        "label": "Pooled councils (Hackney + Rhondda + Aberdeenshire)",
        "file_name": "<pooled-virtual-dataset>",
        "data_path_env": "<pooled-virtual-dataset>",
    },
}

# Member datasets combined by the virtual "pooled_councils" dataset. Each member
# is loaded with its own source resolver and the rows are unioned together with a
# dataset-prefixed unique_id to avoid PROPREF collisions across councils.
_POOLED_MEMBERS: tuple[str, ...] = ("hackney", "rhondda", "aberdeenshire")


def _file_reader_for(source_path: str) -> str:
    suffix = source_path.rsplit(".", maxsplit=1)[-1].lower()
    if suffix == "csv":
        return "read_csv"
    if suffix == "parquet":
        return "read_parquet"
    if suffix == "xlsx":
        return "read_xlsx"

    raise ValueError(f"Unsupported file format for dataset file '{source_path}'.")


def _resolve_dataset_source(dataset: dict[str, str]) -> str:
    return resolve_data_source(dataset["data_path_env"], dataset["file_name"])


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _clean_output(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    return con.sql(
        f"""
        SELECT
            unique_id,
            lower(TRIM(address_concat)) AS address_concat,
            ukam_label,
            postcode
        FROM ({relation.sql_query()}) AS src
        WHERE unique_id IS NOT NULL
          AND address_concat IS NOT NULL
          AND TRIM(address_concat) != ''
          AND postcode IS NOT NULL
        """
    )


def _load_hackney(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)
    address_expr = (
        'regexp_replace(trim(concat_ws(\' \', "ADDR1", "ADDR2", '
        "\"ADDR3\", \"ADDR4\")), '\\s+', ' ')"
    )
    relation = con.sql(
        f"""
        SELECT
            CAST("PROPREF" AS VARCHAR) AS unique_id,
            CAST("UPRN" AS VARCHAR) AS ukam_label,
            {address_expr} AS address_concat,
            "POSTCODE" AS postcode
        FROM {reader}('{source_path}')
        WHERE "UPRN" IS NOT NULL
        """
    )
    return _clean_output(con, relation)


def _load_lambeth_council_tax(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)
    uprn_expr = _normalise_uprn_expr('"UPRN"')
    address_expr = (
        'regexp_replace(trim(concat_ws(\' \', "ADDR1", "ADDR2", '
        "\"ADDR3\", \"ADDR4\")), '\\s+', ' ')"
    )
    relation = con.sql(
        f"""
        WITH source_rows AS (
            SELECT
                {uprn_expr} AS unique_id,
                {uprn_expr} AS ukam_label,
                {address_expr} AS address_concat,
                "POSTCODE" AS postcode
            FROM {reader}('{source_path}')
            WHERE "UPRN" IS NOT NULL
        )
        SELECT
            unique_id,
            ukam_label,
            address_concat,
            postcode
        FROM source_rows
        WHERE ukam_label != '10090204019'
        """
    )
    return _clean_output(con, relation)


def _load_lambeth_electoral_register(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)
    uprn_column = _quote_identifier("Unique property reference number (UPRN)")
    address_1 = _quote_identifier("Address 1")
    address_2 = _quote_identifier("Address 2")
    address_3 = _quote_identifier("Address 3")
    address_4 = _quote_identifier("Address 4")
    postcode = _quote_identifier("Postcode")
    uprn_expr = _normalise_uprn_expr(uprn_column)
    address_expr = (
        "regexp_replace(trim(concat_ws(' ', "
        f"{address_1}, {address_2}, {address_3}, {address_4}"
        ")), '\\s+', ' ')"
    )
    relation = con.sql(
        f"""
        SELECT
            {uprn_expr} AS unique_id,
            {uprn_expr} AS ukam_label,
            {address_expr} AS address_concat,
            {postcode} AS postcode
        FROM {reader}('{source_path}')
        WHERE {uprn_column} IS NOT NULL
        """
    )
    return _clean_output(con, relation)


def _load_lambeth_llpg(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)

    relation = con.sql(
        f"""
        SELECT
            CAST("UPRN_BLPU" AS VARCHAR) AS unique_id,
            CAST("UPRN_BLPU" AS VARCHAR) AS ukam_label,
            trim(regexp_replace(
                trim("Address_LPI"),
                concat('(^|\\s)', regexp_escape("Postcode_LPI"), '($|\\s)'),
                ' ',
                'i'
            )) AS address_concat,
            "Postcode_LPI" AS postcode
        FROM {reader}('{source_path}')
        WHERE "UPRN_BLPU" IS NOT NULL
        """
    )
    return _clean_output(con, relation)


def _load_rhondda(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)
    uprn_expr = _normalise_uprn_expr('"UPRN"')
    address_expr = (
        'regexp_replace(trim(concat_ws(\' \', "ADDR1", "ADDR2", '
        "\"ADDR3\", \"ADDR4\")), '\\s+', ' ')"
    )
    relation = con.sql(
        f"""
        SELECT
            CAST("PROPREF" AS VARCHAR) AS unique_id,
            {uprn_expr} AS ukam_label,
            {address_expr} AS address_concat,
            "POSTCODE" AS postcode
        FROM {reader}('{source_path}')
        WHERE "UPRN" IS NOT NULL
        """
    )
    return _clean_output(con, relation)


def _load_aberdeenshire(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    reader = _file_reader_for(source_path)
    uprn_expr = _normalise_uprn_expr('"UPRN"')
    address_expr = "regexp_replace(trim(\"ADDR\"), '\\s+', ' ')"
    relation = con.sql(
        f"""
        SELECT
            {uprn_expr} AS unique_id,
            {uprn_expr} AS ukam_label,
            {address_expr} AS address_concat,
            "POSTCODE" AS postcode
        FROM {reader}('{source_path}')
        WHERE "UPRN" IS NOT NULL
        """
    )
    return _clean_output(con, relation)


def _load_mid_sussex(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
) -> duckdb.DuckDBPyRelation:
    relation = con.sql(
        f"""
        WITH cleaned AS (
            SELECT
                NULLIF(NULLIF(TRIM("Property Reference"), ''), 'NULL') AS unique_id,
                {_normalise_uprn_expr('"UPRN"')} AS ukam_label,
                NULLIF(NULLIF(TRIM("Post Code"), ''), 'NULL') AS postcode_raw,
                NULLIF(NULLIF(TRIM("Property Name 1"), ''), 'NULL') AS property_name_1,
                NULLIF(NULLIF(TRIM("Property Name 2"), ''), 'NULL') AS property_name_2,
                NULLIF(NULLIF(TRIM("Address 1"), ''), 'NULL') AS address_1,
                NULLIF(NULLIF(TRIM("Address 2"), ''), 'NULL') AS address_2,
                NULLIF(NULLIF(TRIM("Address 3"), ''), 'NULL') AS address_3,
                NULLIF(NULLIF(TRIM("Address 4"), ''), 'NULL') AS address_4
            FROM read_xlsx('{source_path}', all_varchar = true)
        )
        SELECT
            unique_id,
            regexp_replace(
                trim(
                    concat_ws(
                        ' ',
                        property_name_1,
                        property_name_2,
                        address_1,
                        address_2
                    )
                ),
                '\\s+',
                ' '
            ) AS address_concat,
            ukam_label,
            UPPER(REPLACE(postcode_raw, ' ', '')) AS postcode
        FROM cleaned
        WHERE unique_id IS NOT NULL
          AND ukam_label IS NOT NULL
          AND (
              property_name_1 IS NOT NULL
              OR property_name_2 IS NOT NULL
              OR address_1 IS NOT NULL
              OR address_2 IS NOT NULL
              OR address_3 IS NOT NULL
              OR address_4 IS NOT NULL
          )
        """
    )
    return _clean_output(con, relation)


_POOLED_MEMBER_LOADERS = {
    "aberdeenshire": _load_aberdeenshire,
    "hackney": _load_hackney,
    "rhondda": _load_rhondda,
}


def _load_pooled_councils(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Union Hackney, Rhondda and Aberdeenshire into one pooled benchmark dataset.

    Each member is loaded via its own source resolver. ``unique_id`` is prefixed
    with the member key so the council-local PROPREF/identifier values cannot
    collide once combined. ``ukam_label`` (the national UPRN) is left untouched so
    accuracy scoring against the canonical dataset still works.
    """
    member_queries: list[str] = []
    for member_key in _POOLED_MEMBERS:
        loader = _POOLED_MEMBER_LOADERS[member_key]
        source_path = _resolve_dataset_source(get_dataset_definition(member_key))
        maybe_enable_s3_for_path(con, source_path)
        print(f"  pooled member '{member_key}' from: {source_path}")
        member_relation = loader(con, source_path)
        member_queries.append(
            f"""
            SELECT
                '{member_key}:' || unique_id AS unique_id,
                address_concat,
                ukam_label,
                postcode
            FROM ({member_relation.sql_query()}) AS {member_key}_src
            """
        )

    union_sql = "\nUNION ALL\n".join(member_queries)
    return con.sql(union_sql)


def list_dataset_keys() -> list[str]:
    return sorted(_DATASETS.keys())


def get_dataset_definition(dataset_key: str) -> dict[str, str]:
    try:
        return _DATASETS[dataset_key]
    except KeyError as exc:
        valid = ", ".join(list_dataset_keys())
        raise ValueError(
            f"Unknown dataset '{dataset_key}'. Valid options: {valid}."
        ) from exc


def load_dataset(
    con: duckdb.DuckDBPyConnection,
    dataset_key: str,
    sample_mode: bool = False,
) -> duckdb.DuckDBPyRelation:
    dataset = get_dataset_definition(dataset_key)

    if dataset_key == "pooled_councils":
        print(f"Reading {dataset['label']} (virtual pooled dataset)")
        df_messy = _load_pooled_councils(con)
    else:
        source_path = _resolve_dataset_source(dataset)

        maybe_enable_s3_for_path(con, source_path)
        if source_path.lower().endswith(".xlsx"):
            load_duckdb_excel(con)

        print(f"Reading {dataset['label']} from: {source_path}")

        loaders = {
            "aberdeenshire": _load_aberdeenshire,
            "hackney": _load_hackney,
            "lambeth_council_tax": _load_lambeth_council_tax,
            "lambeth_electoral_register": _load_lambeth_electoral_register,
            "lambeth_llpg": _load_lambeth_llpg,
            "mid_sussex": _load_mid_sussex,
            "rhondda": _load_rhondda,
        }
        df_messy = loaders[dataset_key](con, source_path)

    if sample_mode:
        df_messy = con.sql(
            """
            SELECT *
            FROM df_messy
            WHERE hash(unique_id) % 100 < 10
            ORDER BY unique_id
            LIMIT 10000
            """
        )

    return df_messy
