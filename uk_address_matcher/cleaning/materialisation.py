from __future__ import annotations

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.sql_pipeline.helpers import (
    _drop_table_and_registered_aliases,
    _quote_identifier,
    _relation_from_registered_alias,
    _uid,
)


def _parquet_preserves_type(kind) -> bool:
    # Parquet silently converts HUGEINT to DOUBLE, including inside nested types.
    # Fixed-size arrays, enums and other unproven types also remain native.
    if kind.id == "struct" and any(not field for field, _ in kind.children):
        return False
    if kind.id in {"list", "struct", "map"}:
        return all(_parquet_preserves_type(child) for _, child in kind.children)
    return kind.id in {
        "boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "utinyint",
        "usmallint",
        "uinteger",
        "ubigint",
        "float",
        "double",
        "decimal",
        "varchar",
        "blob",
        "date",
        "time",
        "timestamp",
        "timestamp_tz",
        "uuid",
    }


def _varchar_leaves(expression, kind):
    if kind.id == "varchar":
        yield expression
    elif kind.id == "struct":
        for index, (field, child) in enumerate(kind.children, 1):
            extract = (
                f"({expression}).{_quote_identifier(field)}"
                if field
                else f"({expression})[{index}]"
            )
            yield from _varchar_leaves(extract, child)
    elif kind.id in {"list", "array"}:
        yield from _varchar_leaves(f"({expression})[1]", dict(kind.children)["child"])
    elif kind.id == "map":
        for field, child in kind.children:
            function = "map_keys" if field == "key" else "map_values"
            yield from _varchar_leaves(f"{function}({expression})[1]", child)


def _input_has_collation(con: DuckDBPyConnection, source: DuckDBPyRelation) -> bool:
    # Logical type equality omits collation, even inside LIST/STRUCT/MAP. An
    # empty bound projection exposes each VARCHAR leaf's collation in native DDL.
    collation = con.execute("SELECT current_setting('default_collation')").fetchone()[0]
    if collation not in ("", "binary"):
        return True
    leaves = [
        leaf
        for name, kind in zip(source.columns, source.types, strict=True)
        for leaf in _varchar_leaves(_quote_identifier(name), kind)
    ]
    if not leaves:
        return False
    name = f"__ukam_collation_check_{_uid()}"
    projection = ", ".join(f"{leaf} AS field_{i}" for i, leaf in enumerate(leaves))
    try:
        con.execute(
            f"CREATE TEMPORARY TABLE {name} AS "
            f"SELECT {projection} FROM ({source.sql_query()}) LIMIT 0"
        )
        ddl = con.execute(
            "SELECT sql FROM duckdb_tables() WHERE table_name = ?", [name]
        ).fetchone()[0]
        return " COLLATE " in ddl.upper()
    finally:
        _drop_table_and_registered_aliases(con, name)


class _CanonicalIntermediates:
    """Own folder-preparation intermediates through export and manifest creation."""

    def __init__(self, con: DuckDBPyConnection):
        self.con = con
        self._temporary = TemporaryDirectory(prefix="ukam-canonical-intermediates-")
        self.directory = Path(self._temporary.name)
        self._files: dict[str, list[Path]] = {}
        self._native: set[str] = set()
        self._views: set[str] = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        cleanup_error = None
        try:
            for name in [*self._views, *self._files, *self._native]:
                try:
                    self.drop(name)
                except Exception as error:
                    cleanup_error = error
        finally:
            self._temporary.cleanup()
        if cleanup_error is not None and exc is None:
            raise cleanup_error

    def drop(self, name: str) -> None:
        _drop_table_and_registered_aliases(self.con, name)
        if self._files.pop(name, None) is not None:
            shutil.rmtree(self.directory / name)
        self._native.discard(name)
        self._views.discard(name)

    def retain_view(self, name: str) -> None:
        self._views.add(name)

    def write(
        self,
        relation: DuckDBPyRelation,
        name: str,
        *,
        append: bool = False,
        partition_by: str | None = None,
        order_by: str | None = None,
    ) -> DuckDBPyRelation:
        if not all(_parquet_preserves_type(kind) for kind in relation.types):
            if name in self._files:
                raise TypeError("Cannot append incompatible types to Parquet storage")
            if append:
                relation.insert_into(name)
            else:
                relation.create(name)
                self._native.add(name)
            return _relation_from_registered_alias(self.con, name)
        if append and name not in self._files:
            raise ValueError("Cannot append to an unowned Parquet intermediate")
        if append and (
            relation.columns != _relation_from_registered_alias(self.con, name).columns
            or relation.types != _relation_from_registered_alias(self.con, name).types
        ):
            raise TypeError("Intermediate append schema differs")
        directory = self.directory / name
        directory.mkdir(exist_ok=append)
        previous = self._files.get(name, [])
        destination = (
            directory if partition_by else directory / f"{len(previous)}.parquet"
        )
        sql = relation.sql_query()
        if order_by:
            sql = f"SELECT * FROM ({sql}) ORDER BY {_quote_identifier(order_by)}"
        partition = (
            f", PARTITION_BY ({_quote_identifier(partition_by)}), "
            "WRITE_PARTITION_COLUMNS TRUE"
            if partition_by
            else ""
        )
        escaped = str(destination).replace("'", "''")
        try:
            self.con.execute(
                f"COPY ({sql}) TO '{escaped}' "
                f"(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 122880{partition})"
            )
            paths = (
                sorted(directory.rglob("*.parquet")) if partition_by else [destination]
            )
            if not paths:
                # Partitioned COPY emits no file for empty input.
                directory.rmdir()
                return self.write(relation, name, order_by=order_by)
            expected = dict(zip(relation.columns, relation.types, strict=True))
            for path in paths:
                stored = self.con.read_parquet(str(path), hive_partitioning=False)
                actual = dict(zip(stored.columns, stored.types, strict=True))
                if actual != expected or len(stored.columns) != len(relation.columns):
                    raise TypeError(
                        "Intermediate Parquet schema did not round-trip exactly"
                    )
            files = [*previous, *paths]
            stored = self.con.read_parquet(
                [str(path) for path in files], hive_partitioning=False
            ).project(", ".join(map(_quote_identifier, relation.columns)))
            self.con.execute(
                f"CREATE {'OR REPLACE ' if append else ''}TEMPORARY VIEW "
                f"{_quote_identifier(name)} AS {stored.sql_query()}"
            )
            self._files[name] = files
            return _relation_from_registered_alias(self.con, name)
        except BaseException:
            if append:
                destination.unlink(missing_ok=True)
            else:
                shutil.rmtree(directory, ignore_errors=True)
            raise
