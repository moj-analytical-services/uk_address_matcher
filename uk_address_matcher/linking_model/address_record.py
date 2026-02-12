from __future__ import annotations

from dataclasses import dataclass

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
        return cls(
            address_concat=data["address_concat"],
            postcode=data["postcode"],
            unique_id=data.get("unique_id"),
        )

    def as_duckdb_relation(
        self, con: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyRelation:
        """Convert the record to a DuckDB relation for matching."""

        return con.query(
            "select '{}' as unique_id, '{}' as address_concat, '{}' as postcode".format(
                self.unique_id, self.address_concat, self.postcode
            )
        )
