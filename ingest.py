"""CSV validation and idempotent PostgreSQL ingestion for AtmoSync."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Sequence

import psycopg
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_engineering.atmosync.config import DEFAULT_CSV_PATH, DatabaseConfig
from data_engineering.atmosync.db import get_connection

EXPECTED_COLUMNS: tuple[str, ...] = (
    "timestamp_utc",
    "container_id",
    "product",
    "route",
    "temperature_c",
    "humidity_pct",
    "vibration_g",
)

INSERT_SQL = """
INSERT INTO public.sensor_readings (
    timestamp_utc,
    container_id,
    product,
    route,
    temperature_c,
    humidity_pct,
    vibration_g
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (container_id, timestamp_utc) DO NOTHING
RETURNING reading_id;
"""


class CsvValidationError(ValueError):
    """Raised when the CSV header or one or more rows fail validation."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = list(errors)
        preview = "; ".join(self.errors[:10])
        suffix = f" (+{len(self.errors) - 10} more)" if len(self.errors) > 10 else ""
        super().__init__(f"CSV validation failed with {len(self.errors)} error(s): {preview}{suffix}")


class SensorReadingRecord(BaseModel):
    """Validated representation of a single sensor reading CSV row."""

    model_config = ConfigDict(str_strip_whitespace=True)

    timestamp_utc: datetime
    container_id: str = Field(min_length=1, max_length=10)
    product: str = Field(min_length=1, max_length=50)
    route: str = Field(min_length=1, max_length=100)
    temperature_c: Decimal
    humidity_pct: Decimal
    vibration_g: Decimal

    @field_validator("timestamp_utc", mode="before")
    @classmethod
    def parse_utc_timestamp(cls, value: object) -> datetime:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                raise ValueError("timestamp_utc cannot be empty")
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            try:
                dt = datetime.fromisoformat(text)
            except ValueError as exc:
                raise ValueError(f"invalid ISO-8601 timestamp: {value!r}") from exc
        else:
            raise ValueError(f"timestamp_utc must be a string or datetime, got {type(value).__name__}")

        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError(f"timestamp_utc must include timezone offset or 'Z': {value!r}")
        return dt.astimezone(timezone.utc)

    @field_validator("temperature_c", "humidity_pct", "vibration_g", mode="before")
    @classmethod
    def parse_numeric(cls, value: object) -> Decimal:
        if isinstance(value, Decimal):
            dec = value
        elif isinstance(value, (int, float)):
            dec = Decimal(str(value))
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                raise ValueError("numeric field cannot be empty")
            try:
                dec = Decimal(text)
            except InvalidOperation as exc:
                raise ValueError(f"invalid decimal value: {value!r}") from exc
        else:
            raise ValueError(f"expected numeric string, got {type(value).__name__}")

        if not dec.is_finite():
            raise ValueError(f"numeric value must be finite: {value!r}")
        quantized = dec.quantize(Decimal("0.01"))
        if abs(quantized) >= Decimal("10000"):
            raise ValueError(f"value {quantized} exceeds numeric(6,2) precision")
        return quantized

    @field_validator("humidity_pct")
    @classmethod
    def validate_humidity_range(cls, value: Decimal) -> Decimal:
        if value < Decimal("0") or value > Decimal("100"):
            raise ValueError(f"humidity_pct must be between 0 and 100, got {value}")
        return value

    @field_validator("vibration_g")
    @classmethod
    def validate_vibration_range(cls, value: Decimal) -> Decimal:
        if value < Decimal("0"):
            raise ValueError(f"vibration_g must be non-negative, got {value}")
        return value


@dataclass(frozen=True)
class IngestionResult:
    """Summary metrics from validating and ingesting a sensor readings CSV."""

    total_rows: int
    valid_rows: int
    inserted_rows: int
    skipped_duplicates: int
    invalid_rows: int


def parse_and_validate_csv(csv_path: Path) -> list[SensorReadingRecord]:
    """Read and validate all rows from a sensor readings CSV file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    errors: list[str] = []
    records: list[SensorReadingRecord] = []

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise CsvValidationError(["CSV file is empty or missing header row"])

        header = tuple(col.strip() for col in reader.fieldnames)
        if header != EXPECTED_COLUMNS:
            raise CsvValidationError(
                [f"Invalid CSV header: expected {list(EXPECTED_COLUMNS)}, got {list(header)}"]
            )

        for line_number, raw_row in enumerate(reader, start=2):
            if None in raw_row:
                errors.append(f"Row {line_number}: unexpected extra column(s) {raw_row[None]!r}")
                continue
            if any(v is None for v in raw_row.values()):
                errors.append(f"Row {line_number}: missing column(s) in row {raw_row!r}")
                continue

            try:
                records.append(SensorReadingRecord.model_validate(raw_row))
            except Exception as exc:  # noqa: BLE001 - aggregate all row errors clearly
                errors.append(f"Row {line_number}: {exc}")

    if errors:
        raise CsvValidationError(errors)

    return records


def insert_records(
    conn: psycopg.Connection,
    records: Iterable[SensorReadingRecord],
) -> tuple[int, int]:
    """Insert validated records idempotently using ON CONFLICT DO NOTHING."""
    record_list = list(records)
    if not record_list:
        return 0, 0

    inserted = 0
    with conn.transaction():
        with conn.cursor() as cur:
            for rec in record_list:
                cur.execute(
                    INSERT_SQL,
                    (
                        rec.timestamp_utc,
                        rec.container_id,
                        rec.product,
                        rec.route,
                        rec.temperature_c,
                        rec.humidity_pct,
                        rec.vibration_g,
                    ),
                )
                if cur.fetchone() is not None:
                    inserted += 1

    skipped = len(record_list) - inserted
    return inserted, skipped


def ingest_csv(
    csv_path: Path = DEFAULT_CSV_PATH,
    db_config: DatabaseConfig | None = None,
    conn: psycopg.Connection | None = None,
) -> IngestionResult:
    """Validate CSV records and load them idempotently into public.sensor_readings."""
    records = parse_and_validate_csv(csv_path)

    if conn is not None:
        inserted, skipped = insert_records(conn, records)
    else:
        with get_connection(db_config) as managed_conn:
            inserted, skipped = insert_records(managed_conn, records)

    return IngestionResult(
        total_rows=len(records),
        valid_rows=len(records),
        inserted_rows=inserted,
        skipped_duplicates=skipped,
        invalid_rows=0,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate and ingest AtmoSync sensor readings CSV.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"Path to sensor readings CSV (default: {DEFAULT_CSV_PATH.name})",
    )
    args = parser.parse_args(argv)

    result = ingest_csv(csv_path=args.csv)
    print(
        f"Ingestion complete: total={result.total_rows}, valid={result.valid_rows}, "
        f"inserted={result.inserted_rows}, skipped_duplicates={result.skipped_duplicates}, "
        f"invalid={result.invalid_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
