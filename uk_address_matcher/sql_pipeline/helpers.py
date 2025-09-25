import logging
import random
import re
import string

import duckdb

logger = logging.getLogger("uk_address_matcher")


def _emit_debug(msg: str) -> None:
    """Emit debug output via logger if configured, else stdout.

    Many users won't configure logging in quick scripts, so when debug/pretty-print
    is enabled we print to stdout to ensure visibility.
    """
    if logger.handlers and logger.isEnabledFor(logging.DEBUG):
        logger.debug(msg)
    else:
        print(msg)


def _uid(n: int = 6) -> str:
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(n)
    )


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower())


def _pretty_sql(sql: str) -> str:
    # Hook for pretty printers if desired
    return sql


def _duckdb_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()
    return result[0] > 0
