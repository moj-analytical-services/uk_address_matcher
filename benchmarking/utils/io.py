from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import duckdb

_PRIVATE_CONFIG_PATH = Path(__file__).resolve().parent.parent / ".config.json"


def _private_config_path() -> Path:
    return _PRIVATE_CONFIG_PATH


@lru_cache(maxsize=1)
def load_private_config() -> Dict[str, str]:
    """Return key/value pairs from the user's private JSON config."""

    path = _private_config_path()
    if not path.exists():
        return {}

    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):  # pragma: no cover - defensive guard
        raise ValueError(
            f"Private config at {path} must be a JSON object of key/value pairs."
        )

    return {str(key): str(value) for key, value in data.items()}


def apply_env_from_private_config() -> None:
    """Populate missing environment variables from the private config file."""

    for key, value in load_private_config().items():
        os.environ.setdefault(key, value)


def get_env_setting(name: str, *, default: Optional[str] = None) -> str:
    """Return an environment variable, loading the private config if required."""

    apply_env_from_private_config()

    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(
            f"Missing required configuration value '{name}'. Set it as an environment "
            "variable or add it to the private config JSON file."
        )
    return value


def load_duckdb_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure DuckDB has httpfs/S3 support configured with boto3 credentials.

    The helper installs and loads the `httpfs` extension, then checks for
    credentials available via boto3. When credentials are missing we still
    ensure the extension is available, but callers are warned that S3 access
    may fail.
    """

    already_loaded = (
        con.execute(
            "SELECT 1 FROM duckdb_extensions() WHERE extension_name = 'httpfs' AND loaded"
        ).fetchone()
        is not None
    )
    if already_loaded:
        return

    con.execute("INSTALL httpfs; LOAD httpfs;")

    try:  # pragma: no cover - optional dependency
        import boto3  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:  # pragma: no cover - env specific
        raise RuntimeError(
            "boto3 is required to configure S3 access. Install boto3 or set up "
            "DuckDB credentials manually before running this script."
        ) from exc

    session = boto3.Session()
    credentials = session.get_credentials()

    if not credentials:
        print(
            "Warning: No AWS credentials available from boto3 session; S3 access may fail."
        )
        return

    frozen = credentials.get_frozen_credentials()
    region = session.region_name or os.getenv("AWS_REGION") or "eu-west-1"
    con.execute(f"SET s3_region='{region}';")

    access_key = frozen.access_key.replace("'", "''") if frozen.access_key else ""
    secret_key = frozen.secret_key.replace("'", "''") if frozen.secret_key else ""
    token = frozen.token.replace("'", "''") if frozen.token else ""

    if access_key:
        con.execute(f"SET s3_access_key_id='{access_key}';")
    if secret_key:
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
    if token:
        con.execute(f"SET s3_session_token='{token}';")


@lru_cache(maxsize=None)
def setup_connection() -> duckdb.DuckDBPyConnection:
    """Initialise DuckDB connection with required extensions for benchmarking."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")
    return con


__all__ = [
    "apply_env_from_private_config",
    "get_env_setting",
    "load_duckdb_httpfs",
    "load_private_config",
    "setup_connection",
]
