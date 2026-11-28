"""Pytest suite for AtmoSync Phase 1 CSV validation and PostgreSQL ingestion."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from data_engineering.atmosync.config import DEFAULT_CSV_PATH, load_database_config
from data_engineering.atmosync.db import get_connection
from data_engineering.atmosync.ingest import (
    CsvValidationError,
    ingest_csv,
    insert_records,
    parse_and_validate_csv,
)
from data_engineering.atmosync.validate import run_validation


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path


def _db_connection_or_skip() -> psycopg.Connection:
    cfg = load_database_config()
    try:
        return get_connection(cfg)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL database unavailable for integration test: {exc}")


def test_csv_structure_validation_rejects_invalid_headers_and_columns(tmp_path: Path) -> None:
    bad_header = _write_csv(
        tmp_path / "bad_header.csv",
        """
        timestamp_utc,container_id,product,route,temperature_c,humidity_pct
        2026-11-23T17:00:00Z,C001,avocado,Pune-Mumbai,4.72,61.51
        """,
    )
    with pytest.raises(CsvValidationError, match="Invalid CSV header"):
        parse_and_validate_csv(bad_header)

    extra_col = _write_csv(
        tmp_path / "extra_col.csv",
        """
        timestamp_utc,container_id,product,route,temperature_c,humidity_pct,vibration_g
        2026-11-23T17:00:00Z,C001,avocado,Pune-Mumbai,4.72,61.51,2.45,extra
        """,
    )
    with pytest.raises(CsvValidationError, match="unexpected extra column"):
        parse_and_validate_csv(extra_col)


def test_csv_data_type_validation_rejects_bad_types_and_ranges(tmp_path: Path) -> None:
    bad_rows = _write_csv(
        tmp_path / "bad_types.csv",
        """
        timestamp_utc,container_id,product,route,temperature_c,humidity_pct,vibration_g
        2026-11-23T17:00:00,C001,avocado,Pune-Mumbai,4.72,61.51,2.45
        2026-11-23T17:00:00Z,C001,avocado,Pune-Mumbai,not_a_num,61.51,2.45
        2026-11-23T17:00:00Z,C001,avocado,Pune-Mumbai,4.72,105.00,2.45
        2026-11-23T17:00:00Z,C001,avocado,Pune-Mumbai,4.72,61.51,-0.10
        """,
    )
    with pytest.raises(CsvValidationError) as exc_info:
        parse_and_validate_csv(bad_rows)

    assert len(exc_info.value.errors) == 4


def test_real_csv_parses_all_240_records_as_utc() -> None:
    records = parse_and_validate_csv(DEFAULT_CSV_PATH)
    assert len(records) == 240
    assert all(r.timestamp_utc.utcoffset().total_seconds() == 0 for r in records)
    assert records[0].container_id == "C001"
    assert isinstance(records[0].temperature_c, Decimal)


def test_duplicate_prevention_and_rolled_back_ingestion(tmp_path: Path) -> None:
    sample_csv = _write_csv(
        tmp_path / "sample.csv",
        """
        timestamp_utc,container_id,product,route,temperature_c,humidity_pct,vibration_g
        2099-01-01T00:00:00Z,T001,avocado,Test-Route,4.50,65.00,1.20
        2099-01-01T00:00:00Z,T001,avocado,Test-Route,4.50,65.00,1.20
        2099-01-01T01:00:00+00:00,T001,avocado,Test-Route,4.80,66.00,1.30
        """,
    )
    records = parse_and_validate_csv(sample_csv)
    assert len(records) == 3

    conn = _db_connection_or_skip()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT test_ingest;")
            inserted, skipped = insert_records(conn, records)
            assert inserted == 2
            assert skipped == 1

            inserted_again, skipped_again = insert_records(conn, records)
            assert inserted_again == 0
            assert skipped_again == 3
            conn.rollback()
    finally:
        conn.close()


def test_rerun_real_csv_ingestion_is_idempotent() -> None:
    conn = _db_connection_or_skip()
    try:
        before = run_validation(conn=conn)
        result = ingest_csv(csv_path=DEFAULT_CSV_PATH, conn=conn)
        after = run_validation(conn=conn)

        assert before.total_records == 240
        assert result.total_rows == 240
        assert result.valid_rows == 240
        assert result.inserted_rows == 0
        assert result.skipped_duplicates == 240
        assert after.total_records == before.total_records == 240
        assert after.duplicate_groups == 0
        assert all(count == 0 for count in after.null_counts.values())
    finally:
        conn.close()
