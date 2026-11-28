"""Apache Superset configuration for AtmoSync (single-container local setup)."""

from __future__ import annotations

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Persistent SQLite metadata DB stored on dedicated Docker volume atmosync_superset_home.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

ROW_LIMIT = 5000
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = False
TALISMAN_ENABLED = False
