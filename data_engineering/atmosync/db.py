"""PostgreSQL connection helpers for AtmoSync."""

from __future__ import annotations

import psycopg

from data_engineering.atmosync.config import DatabaseConfig, load_database_config


def get_connection(config: DatabaseConfig | None = None) -> psycopg.Connection:
    """Open a PostgreSQL connection using the provided or environment config."""
    cfg = config or load_database_config()
    return psycopg.connect(**cfg.connect_kwargs())
