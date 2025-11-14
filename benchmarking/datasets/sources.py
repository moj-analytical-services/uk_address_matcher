from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from benchmarking.utils.io import get_env_setting

if TYPE_CHECKING:
    import duckdb

UniqueIdFormatter = Callable[[str], str]


def quote_identifier(identifier: str) -> str:
    """Return a DuckDB-safe quoted identifier."""
    return '"' + identifier.replace('"', '""') + '"'


@lru_cache(maxsize=None)
def load_canonical_data(
    con: duckdb.DuckDBPyConnection,
    canonical_config: CanonicalConfig | None = None,
) -> duckdb.DuckDBPyRelation:
    config = canonical_config or CanonicalConfig.default()
    print(f"Loading canonical OS data from {config.local_path}...")
    config.validate()
    return con.read_parquet(str(config.local_path))


@dataclass(frozen=True)
class SourceConfig:
    """Mapping for S3 datasets used in benchmarking.

    Defines how to read and transform a source dataset into
    the standard schema required for matching.
    """

    name: str
    s3_key: str
    unique_id_column: str
    postcode_column: str
    address_columns: list[str] | str
    # Returns unique_id by default
    optional_filter: str | None = None
    unique_id_formatter: UniqueIdFormatter = lambda x: x
    prune_postcode_from_address: bool = False

    def select_statement(self, base_path: str) -> str:
        """Generate SQL SELECT statement to read and transform this source.

        Handles both single address columns and multi-column addresses.
        If prune_postcode_from_address is True, removes the postcode value
        from the address string (case-insensitive).
        """
        # Build the address expression
        if isinstance(self.address_columns, str):
            # Single column: just trim and optionally prune postcode
            address_expr = f"trim({quote_identifier(self.address_columns)})"
        else:
            # Multiple columns: concatenate, trim, and normalise spaces
            cols = ", ".join(
                quote_identifier(col) for col in self.address_columns
            )
            address_expr = (
                "regexp_replace(trim(concat_ws(' ', {cols})), '\\s+', ' ')".format(
                    cols=cols
                )
            )

        # Apply postcode pruning if requested
        if self.prune_postcode_from_address:
            postcode_col = quote_identifier(self.postcode_column)
            # Case-insensitive removal: handle any casing of the postcode value
            address_expr = (
                f"trim(regexp_replace({address_expr}, "
                f"concat('(^|\\s)', regexp_escape({postcode_col}), '($|\\s)'), ' ', 'i'))"
            )

        unique_id_expr = self.unique_id_formatter(
            f"cast({quote_identifier(self.unique_id_column)} as varchar)"
        )
        return f"""
            SELECT
                {unique_id_expr} AS unique_id,
                {address_expr} AS address_concat,
                {quote_identifier(self.postcode_column)} AS postcode,
                '{self.name}' AS dataset_name
            FROM read_parquet('{base_path}{self.s3_key}')
            WHERE address_concat IS NOT NULL
                AND postcode IS NOT NULL
                {f'AND {self.optional_filter}' if self.optional_filter else ''}
        """


@dataclass(frozen=True)
class CanonicalConfig:
    """Configuration for the canonical Ordnance Survey dataset.

    The OS data serves as the reference dataset that all messy
    addresses are matched against.
    """

    local_path: Path
    description: str = "Pre-cleaned Ordnance Survey addresses"

    @classmethod
    def default(cls) -> CanonicalConfig:
        configured_path = get_env_setting(
            "UKAM_OS_CANONICAL_PATH"
        )
        if not configured_path:
            raise RuntimeError(
                "Environment variable UKAM_OS_CANONICAL_PATH must be set to the "
                "local path of the canonical OS dataset."
            )
        return cls(local_path=Path(configured_path))

    def validate(self) -> None:
        if not self.local_path.exists():
            raise FileNotFoundError(
                f"Canonical OS data not found at {self.local_path}. "
                "Run tmp/clean_os_data.py to generate it, or update the path "
                "in the configuration."
            )
