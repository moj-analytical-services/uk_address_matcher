from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from uuid import uuid4

import duckdb


@dataclass
class AddressRecord:
    """A single address to match.

    Args:
        address_concat: Full address text without the postcode.
        postcode: UK postcode.
        unique_id: Unique identifier for the record.

    Examples:
        ::

            from uk_address_matcher import AddressRecord

            record = AddressRecord(
                address_concat="10 downing street westminster london",
                postcode="SW1A 2AA",
                unique_id="1",
            )
    """

    address_concat: str
    postcode: str
    unique_id: str

    @classmethod
    def from_dict(cls, data: dict) -> AddressRecord:
        unique_id = data.get("unique_id")
        if unique_id is None:
            raise ValueError(
                "AddressRecord dict inputs must include a non-null 'unique_id'."
            )

        return cls(
            address_concat=data["address_concat"],
            postcode=data["postcode"],
            unique_id=unique_id,
        )

    def as_duckdb_relation(
        self, con: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyRelation:
        """Convert the record to a DuckDB relation for matching."""
        return self.to_duckdb_relation([self], con)

    @staticmethod
    def to_duckdb_relation(
        records: Iterable[AddressRecord],
        con: duckdb.DuckDBPyConnection,
    ) -> duckdb.DuckDBPyRelation:
        """Convert a list of records to a DuckDB relation for matching."""

        rows = [
            (record.unique_id, record.address_concat, record.postcode)
            for record in records
        ]
        if not rows:
            raise ValueError("At least one AddressRecord is required.")

        table_name = f"ukam_address_records_{uuid4().hex}"
        con.execute(
            f"CREATE TEMP TABLE {table_name} ("
            "unique_id VARCHAR, address_concat VARCHAR, postcode VARCHAR)"
        )
        con.executemany(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)",
            rows,
        )
        return con.table(table_name)
