"""Post-ingestion PostgreSQL data quality validation for AtmoSync."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg

from data_engineering.atmosync.config import DatabaseConfig
from data_engineering.atmosync.db import get_connection


@dataclass(frozen=True)
class ContainerSummary:
    container_id: str
    product: str | None
    route: str | None
    count: int


@dataclass(frozen=True)
class MetricRange:
    min_value: Decimal | None
    max_value: Decimal | None
    avg_value: Decimal | None


@dataclass(frozen=True)
class ValidationReport:
    total_records: int
    records_per_container: list[ContainerSummary]
    min_timestamp: datetime | None
    max_timestamp: datetime | None
    null_counts: dict[str, int]
    duplicate_groups: int
    temperature_range: MetricRange
    humidity_range: MetricRange
    vibration_range: MetricRange

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_records": self.total_records,
            "records_per_container": [
                {
                    "container_id": c.container_id,
                    "product": c.product,
                    "route": c.route,
                    "count": c.count,
                }
                for c in self.records_per_container
            ],
            "min_timestamp": self.min_timestamp.isoformat() if self.min_timestamp else None,
            "max_timestamp": self.max_timestamp.isoformat() if self.max_timestamp else None,
            "null_counts": self.null_counts,
            "duplicate_groups": self.duplicate_groups,
            "temperature_range": self.temperature_range,
            "humidity_range": self.humidity_range,
            "vibration_range": self.vibration_range,
        }


def run_validation(
    db_config: DatabaseConfig | None = None,
    conn: psycopg.Connection | None = None,
) -> ValidationReport:
    """Query public.sensor_readings and return a full data-quality summary."""
    own_conn = conn is None
    active_conn = conn if conn is not None else get_connection(db_config)

    try:
        with active_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*),
                    MIN(timestamp_utc),
                    MAX(timestamp_utc),
                    COUNT(*) FILTER (WHERE timestamp_utc IS NULL),
                    COUNT(*) FILTER (WHERE container_id IS NULL),
                    COUNT(*) FILTER (WHERE product IS NULL),
                    COUNT(*) FILTER (WHERE route IS NULL),
                    COUNT(*) FILTER (WHERE temperature_c IS NULL),
                    COUNT(*) FILTER (WHERE humidity_pct IS NULL),
                    COUNT(*) FILTER (WHERE vibration_g IS NULL),
                    MIN(temperature_c), MAX(temperature_c), ROUND(AVG(temperature_c), 2),
                    MIN(humidity_pct), MAX(humidity_pct), ROUND(AVG(humidity_pct), 2),
                    MIN(vibration_g), MAX(vibration_g), ROUND(AVG(vibration_g), 2)
                FROM public.sensor_readings;
                """
            )
            row = cur.fetchone()
            assert row is not None

            cur.execute(
                """
                SELECT container_id, product, route, COUNT(*)
                FROM public.sensor_readings
                GROUP BY container_id, product, route
                ORDER BY container_id;
                """
            )
            container_rows = [
                ContainerSummary(
                    container_id=r[0],
                    product=r[1],
                    route=r[2],
                    count=r[3],
                )
                for r in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT container_id, timestamp_utc
                    FROM public.sensor_readings
                    GROUP BY container_id, timestamp_utc
                    HAVING COUNT(*) > 1
                ) AS dups;
                """
            )
            dup_row = cur.fetchone()
            duplicate_groups = int(dup_row[0]) if dup_row else 0

        null_counts = {
            "timestamp_utc": int(row[3]),
            "container_id": int(row[4]),
            "product": int(row[5]),
            "route": int(row[6]),
            "temperature_c": int(row[7]),
            "humidity_pct": int(row[8]),
            "vibration_g": int(row[9]),
        }

        return ValidationReport(
            total_records=int(row[0]),
            records_per_container=container_rows,
            min_timestamp=row[1],
            max_timestamp=row[2],
            null_counts=null_counts,
            duplicate_groups=duplicate_groups,
            temperature_range=MetricRange(row[10], row[11], row[12]),
            humidity_range=MetricRange(row[13], row[14], row[15]),
            vibration_range=MetricRange(row[16], row[17], row[18]),
        )
    finally:
        if own_conn:
            active_conn.close()


def format_report(report: ValidationReport) -> str:
    """Format a ValidationReport for terminal output."""
    lines = [
        "=== AtmoSync Database Validation Report ===",
        f"Total records: {report.total_records}",
        f"Timestamp range (UTC): {report.min_timestamp} -> {report.max_timestamp}",
        f"Duplicate (container_id, timestamp_utc) groups: {report.duplicate_groups}",
        f"NULL counts: {report.null_counts}",
        f"Temperature (C): min={report.temperature_range.min_value}, max={report.temperature_range.max_value}, avg={report.temperature_range.avg_value}",
        f"Humidity (%):    min={report.humidity_range.min_value}, max={report.humidity_range.max_value}, avg={report.humidity_range.avg_value}",
        f"Vibration (g):   min={report.vibration_range.min_value}, max={report.vibration_range.max_value}, avg={report.vibration_range.avg_value}",
        "Records per container:",
    ]
    for c in report.records_per_container:
        lines.append(f"  - {c.container_id} ({c.product}, {c.route}): {c.count}")
    return "\n".join(lines)


def main() -> int:
    report = run_validation()
    print(format_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
