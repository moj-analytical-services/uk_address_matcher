from __future__ import annotations

from dataclasses import dataclass
from types import EllipsisType
from typing import Iterable
from uuid import uuid4

import duckdb

from uk_address_matcher.helpers.canonical_inputs import _normalise_address_dict


@dataclass(init=False)
class AddressRecord:
    """A single address to match.

    Args:
        address_concat: Full address text, optionally including the postcode.
        postcode: Optional UK postcode; otherwise extracted from the address.
        address_id: Address entity identifier. May repeat across rows.
        unique_id: Supported alias for `address_id`; supply only one ID name.

    Examples:
        ::

            from uk_address_matcher import AddressRecord

            record = AddressRecord(
                address_concat="10 downing street westminster london",
                postcode="SW1A 2AA",
                address_id="1",
            )
    """

    address_concat: str
    address_id: str
    postcode: str | None = None

    def __init__(
        self,
        address_concat: str,
        postcode: str | None = None,
        unique_id: str | None | EllipsisType = ...,
        *,
        address_id: str | None | EllipsisType = ...,
    ) -> None:
        data = {"address_concat": address_concat, "postcode": postcode}
        if unique_id is not Ellipsis:
            data["unique_id"] = unique_id
        if address_id is not Ellipsis:
            data["address_id"] = address_id
        normalised = _normalise_address_dict(data)
        self.address_concat = normalised["address_concat"]
        self.address_id = normalised["address_id"]
        self.postcode = normalised["postcode"]

    @property
    def unique_id(self) -> str:
        """The supported alias for `address_id`."""
        return self.address_id

    @unique_id.setter
    def unique_id(self, value: str) -> None:
        self.address_id = value

    @classmethod
    def from_dict(cls, data: dict) -> AddressRecord:
        normalised = _normalise_address_dict(data)
        return cls(
            address_concat=normalised["address_concat"],
            postcode=normalised["postcode"],
            address_id=normalised["address_id"],
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
