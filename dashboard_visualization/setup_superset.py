"""Safe host-side bootstrap and verification script for AtmoSync Apache Superset."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import textwrap
import urllib.request
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTAINER = "atmosync-superset"
MARTS_TABLES = (
    "dim_containers",
    "fct_sensor_readings",
    "mart_container_health_summary",
)


def run_step(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(cmd, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed (exit={result.returncode}): {' '.join(cmd)}")
    return result


def check_health(url: str = "http://127.0.0.1:8088/health") -> None:
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = resp.read().decode("utf-8", errors="replace").strip()
        if resp.status != 200:
            raise RuntimeError(f"Unexpected health status {resp.status}: {body}")
        print(f"SUPERSET_HEALTH_OK: status={resp.status} body={body!r}")


def test_pg_from_container(pg_host: str, env: dict[str, str | None]) -> bool:
    payload = json.dumps(
        {
            "host": pg_host,
            "port": int(env.get("PGPORT") or "5432"),
            "dbname": env["PGDATABASE"],
            "user": env["PGUSER"],
            "password": env["PGPASSWORD"],
        }
    )
    probe_code = textwrap.dedent(
        """
        import json, sys, psycopg2
        cfg = json.load(sys.stdin)
        conn = psycopg2.connect(**cfg, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT current_database(), COUNT(*) FROM marts.fct_sensor_readings")
        print("PG_CONNECT_OK:", cur.fetchone())
        conn.close()
        """
    )
    res = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "python", "-c", probe_code],
        input=payload,
        text=True,
        check=False,
    )
    return res.returncode == 0


def register_database_and_datasets(pg_host: str, env: dict[str, str | None]) -> None:
    payload = json.dumps(
        {
            "host": pg_host,
            "port": int(env.get("PGPORT") or "5432"),
            "dbname": env["PGDATABASE"],
            "user": env["PGUSER"],
            "password": env["PGPASSWORD"],
            "tables": list(MARTS_TABLES),
        }
    )
    reg_code = textwrap.dedent(
        """
        import json, sys
        from sqlalchemy import text
        from sqlalchemy.engine import URL
        from superset.app import create_app

        cfg = json.load(sys.stdin)
        app = create_app()

        with app.app_context():
            from superset import db
            from superset.models.core import Database
            from superset.connectors.sqla.models import SqlaTable

            uri = URL.create(
                drivername="postgresql+psycopg2",
                username=cfg["user"],
                password=cfg["password"],
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["dbname"],
            ).render_as_string(hide_password=False)

            database = db.session.query(Database).filter_by(database_name="AtmoSync PostgreSQL").first()
            if database is None:
                database = Database(database_name="AtmoSync PostgreSQL", expose_in_sqllab=True)
                db.session.add(database)
            database.set_sqlalchemy_uri(uri)
            database.expose_in_sqllab = True
            database.allow_ctas = False
            database.allow_cvas = False
            database.allow_dml = False
            db.session.commit()

            with database.get_sqla_engine() as engine:
                with engine.connect() as conn:
                    for tbl in cfg["tables"]:
                        cnt = conn.execute(text(f"SELECT COUNT(*) FROM marts.{tbl}")).scalar_one()
                        print(f"SELECT_OK marts.{tbl}: {cnt} rows")

            for tbl in cfg["tables"]:
                ds = (
                    db.session.query(SqlaTable)
                    .filter_by(database_id=database.id, schema="marts", table_name=tbl)
                    .first()
                )
                if ds is None:
                    ds = SqlaTable(database=database, schema="marts", table_name=tbl)
                    db.session.add(ds)
                    db.session.commit()
                ds.fetch_metadata()
                db.session.commit()
                print(f"DATASET_REGISTERED marts.{tbl} (id={ds.id}, cols={len(ds.columns)})")
        """
    )
    res = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "python", "-c", reg_code],
        input=payload,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError("Failed to register PostgreSQL database/datasets in Superset")


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize AtmoSync Superset metadata and datasets.")
    parser.add_argument("--pg-host", default="host.docker.internal", help="PostgreSQL host reachable from Superset")
    parser.add_argument("--verify-only", action="store_true", help="Only verify health and existing registered datasets")
    args = parser.parse_args()

    env = dotenv_values(PROJECT_ROOT / ".env")

    if not args.verify_only:
        print("=== 1. superset db upgrade ===")
        run_step(["docker", "exec", CONTAINER, "superset", "db", "upgrade"])

        print("=== 2. create-admin (idempotent) ===")
        admin_res = subprocess.run(
            [
                "docker",
                "exec",
                CONTAINER,
                "superset",
                "fab",
                "create-admin",
                "--username",
                env["SUPERSET_ADMIN_USERNAME"] or "admin",
                "--firstname",
                env["SUPERSET_ADMIN_FIRSTNAME"] or "AtmoSync",
                "--lastname",
                env["SUPERSET_ADMIN_LASTNAME"] or "Admin",
                "--email",
                env["SUPERSET_ADMIN_EMAIL"] or "admin@atmosync.local",
                "--password",
                env["SUPERSET_ADMIN_PASSWORD"] or "",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        print(admin_res.stdout.strip())
        if admin_res.returncode != 0 and "already exists" not in (admin_res.stdout + admin_res.stderr).lower():
            print(admin_res.stderr, file=sys.stderr)
            return admin_res.returncode

        print("=== 3. superset init ===")
        run_step(["docker", "exec", CONTAINER, "superset", "init"])

    print("=== 4. Superset /health ===")
    check_health()

    print(f"=== 5. Test PostgreSQL connectivity from container ({args.pg_host}) ===")
    if not test_pg_from_container(args.pg_host, env):
        print(f"ERROR: PostgreSQL connectivity failed from {CONTAINER} to {args.pg_host}", file=sys.stderr)
        return 1

    if not args.verify_only:
        print("=== 6. Register AtmoSync PostgreSQL & marts datasets ===")
        register_database_and_datasets(args.pg_host, env)
    else:
        verify_code = textwrap.dedent(
            """
            from sqlalchemy import text
            from superset.app import create_app

            app = create_app()
            with app.app_context():
                from superset import db
                from superset.models.core import Database
                from superset.connectors.sqla.models import SqlaTable

                database = db.session.query(Database).filter_by(database_name="AtmoSync PostgreSQL").first()
                assert database is not None, "Missing AtmoSync PostgreSQL database"
                admin_user = app.appbuilder.sm.find_user(username="admin")
                assert admin_user is not None, "Missing admin user after restart"
                datasets = db.session.query(SqlaTable).filter_by(database_id=database.id, schema="marts").all()
                print("PERSISTED_ADMIN:", admin_user.username)
                print("PERSISTED_DATASETS:", sorted(d.table_name for d in datasets))
                with database.get_sqla_engine() as engine:
                    with engine.connect() as conn:
                        for tbl in sorted(d.table_name for d in datasets):
                            cnt = conn.execute(text(f"SELECT COUNT(*) FROM marts.{tbl}")).scalar_one()
                            print(f"VERIFY_SELECT_OK marts.{tbl}: {cnt} rows")
            """
        )
        run_step(["docker", "exec", CONTAINER, "python", "-c", verify_code])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
