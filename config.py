"""Database configuration helpers for AtmoSync."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CSV_PATH = PROJECT_ROOT / "sensor_readings.csv"


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL connection settings loaded from environment variables."""

    host: str
    port: int
    dbname: str
    user: str
    password: str | None = None
    dsn: str | None = None

    def connect_kwargs(self) -> dict[str, Any]:
        """Return keyword arguments suitable for psycopg.connect()."""
        if self.dsn:
            return {"conninfo": self.dsn}
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs


def load_database_config(env_path: Path | None = None) -> DatabaseConfig:
    """Load database settings from .env (if present) and process environment."""
    target_env = env_path or (PROJECT_ROOT / ".env")
    if target_env.exists():
        load_dotenv(dotenv_path=target_env, override=False)

    dsn = os.getenv("DATABASE_URL") or None
    port_raw = os.getenv("PGPORT", "5432")
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError(f"Invalid PGPORT value: {port_raw!r}") from exc

    return DatabaseConfig(
        host=os.getenv("PGHOST", "127.0.0.1"),
        port=port,
        dbname=os.getenv("PGDATABASE", "atmosync_db"),
        user=os.getenv("PGUSER", "atmosync_user"),
        password=os.getenv("PGPASSWORD") or None,
        dsn=dsn,
    )
