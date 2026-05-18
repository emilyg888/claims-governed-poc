"""Snowflake connection helpers for pipeline modules."""

from __future__ import annotations

import yaml
import os
from contextlib import contextmanager
from typing import Any, Generator

import snowflake.connector
from snowflake.connector import SnowflakeConnection


def _required_env(name: str) -> str:
    """Read required environment variable and fail early if missing."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_yaml_config() -> dict[str, Any]:
    """Load default Snowflake connection values from local dev config."""
    with open("config/env.dev.yaml", "r") as f:
        return yaml.safe_load(f)["snowflake"]


def connection_params() -> dict[str, Any]:
    """Build connector kwargs.

    Precedence is:
    1) Environment variable (best for secrets/runtime overrides)
    2) config/env.dev.yaml fallback for non-secret defaults
    """
    yaml_config = load_yaml_config()

    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", yaml_config["account"]),
        "user": os.getenv("SNOWFLAKE_USER", yaml_config["user"]),
        # Keep password env-only so it never needs to be committed in YAML.
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", yaml_config["role"]),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", yaml_config["warehouse"]),
        "database": os.getenv("SNOWFLAKE_DATABASE", yaml_config["database"]),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", yaml_config["schema_gold"]),
    }


@contextmanager
def get_connection() -> Generator[SnowflakeConnection, None, None]:
    """Open Snowflake connection with commit/rollback handling."""
    conn = snowflake.connector.connect(autocommit=False, **connection_params())
    try:
        yield conn
        # If no exception happened in caller code, persist the transaction.
        conn.commit()
    except Exception:
        # Roll back partial changes so each run is all-or-nothing.
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_scalar(
    conn: SnowflakeConnection,
    sql: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """Execute query and return first column of first row."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row else None


class SnowflakeClient:
    """Small convenience wrapper used by control/evidence modules."""

    def __init__(self) -> None:
        self._conn: SnowflakeConnection | None = None

    def connect(self) -> None:
        """Open a connection when needed."""
        if self._conn is None:
            self._conn = snowflake.connector.connect(
                autocommit=False,
                **connection_params(),
            )

    def close(self) -> None:
        """Close the active connection if it exists."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SnowflakeClient":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._conn is None:
            return
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self.close()

    @property
    def connection(self) -> SnowflakeConnection:
        """Return an open connection."""
        self.connect()
        assert self._conn is not None
        return self._conn

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> None:
        """Execute one statement."""
        with self.connection.cursor() as cur:
            cur.execute(sql, params)

    def query_one(
        self,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> tuple[Any, ...] | None:
        """Execute query and fetch one row."""
        with self.connection.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
