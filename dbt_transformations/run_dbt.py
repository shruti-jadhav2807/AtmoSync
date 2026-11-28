"""Safe dbt runner that loads .env into the subprocess environment without exposing secrets."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

DBT_PROJECT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DBT_PROJECT_DIR.parent


def main(argv: list[str] | None = None) -> int:
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print("Usage: python dbt_transformations/run_dbt.py <dbt-subcommand> [args...]", file=sys.stderr)
        return 2

    dbt_exe = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if not dbt_exe.exists():
        print(f"dbt executable not found at {dbt_exe}", file=sys.stderr)
        return 1

    cmd = [str(dbt_exe), *args]
    if "--project-dir" not in args:
        cmd.extend(["--project-dir", str(DBT_PROJECT_DIR)])
    if "--profiles-dir" not in args:
        cmd.extend(["--profiles-dir", str(DBT_PROJECT_DIR)])

    completed = subprocess.run(cmd, env=os.environ.copy(), check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
