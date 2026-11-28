"""Independent Kafka sensor producer for AtmoSync."""

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from confluent_kafka import Producer

from data_engineering.atmosync.config import DEFAULT_CSV_PATH
from data_engineering.atmosync.ingest import SensorReadingRecord, parse_and_validate_csv
from data_engineering.atmosync.kafka_config import KafkaConfig, load_kafka_config
from data_engineering.atmosync.kafka_topics import ensure_topics

LIVE_CONTAINERS: tuple[tuple[str, str, str, float, float, float], ...] = (
    ("C001", "avocado", "Pune-Mumbai", 5.0, 65.0, 2.1),
    ("C002", "tomato", "Nashik-Delhi", 7.0, 72.0, 2.8),
    ("C003", "mango", "Mumbai-Bengaluru", 6.0, 68.0, 2.4),
    ("C004", "spinach", "Pune-Hyderabad", 4.0, 75.0, 1.9),
    ("C005", "banana", "Nagpur-Chennai", 5.5, 66.0, 2.3),
)


@dataclass(frozen=True)
class ProduceResult:
    """Summary metrics from publishing sensor records to Kafka."""

    total_sent: int
    acknowledged: int
    failed: int
    partitions_used: tuple[int, ...]


def serialize_record(record: SensorReadingRecord) -> bytes:
    """Serialize a validated SensorReadingRecord as UTF-8 JSON bytes."""
    payload = {
        "timestamp_utc": record.timestamp_utc.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "container_id": record.container_id,
        "product": record.product,
        "route": record.route,
        "temperature_c": float(record.temperature_c),
        "humidity_pct": float(record.humidity_pct),
        "vibration_g": float(record.vibration_g),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def generate_live_batch(
    timestamp_utc: datetime | None = None,
    rng: random.Random | None = None,
) -> list[SensorReadingRecord]:
    """Generate one synthetic reading per container for live streaming mode."""
    ts = (timestamp_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    rand = rng or random.Random()
    batch: list[SensorReadingRecord] = []
    for container_id, product, route, base_temp, base_hum, base_vib in LIVE_CONTAINERS:
        batch.append(
            SensorReadingRecord(
                timestamp_utc=ts,
                container_id=container_id,
                product=product,
                route=route,
                temperature_c=round(base_temp + rand.uniform(-0.8, 0.8), 2),
                humidity_pct=round(min(100.0, max(0.0, base_hum + rand.uniform(-5.0, 5.0))), 2),
                vibration_g=round(max(0.0, base_vib + rand.uniform(-0.5, 0.8)), 2),
            )
        )
    return batch


def build_producer(config: KafkaConfig) -> Producer:
    """Create an idempotent confluent_kafka Producer."""
    return Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def publish_records(
    records: Iterable[SensorReadingRecord],
    config: KafkaConfig | None = None,
    producer: Producer | None = None,
    topic: str | None = None,
) -> ProduceResult:
    """Publish validated sensor records to Kafka keyed by container_id."""
    cfg = config or load_kafka_config()
    target_topic = topic or cfg.topic
    own_producer = producer is None
    active_producer = producer or build_producer(cfg)

    acked = 0
    failed = 0
    errors: list[str] = []
    partitions: set[int] = set()
    total = 0

    def on_delivery(err: object, msg: object) -> None:
        nonlocal acked, failed
        if err is not None:
            failed += 1
            errors.append(str(err))
        else:
            acked += 1
            partitions.add(msg.partition())  # type: ignore[attr-defined]

    for record in records:
        key_bytes = record.container_id.encode("utf-8")
        value_bytes = serialize_record(record)
        while True:
            try:
                active_producer.produce(
                    topic=target_topic,
                    key=key_bytes,
                    value=value_bytes,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                active_producer.poll(0.1)
        total += 1
        active_producer.poll(0)

    remaining = active_producer.flush(timeout=30)
    if remaining > 0:
        raise RuntimeError(f"{remaining} Kafka message(s) were not flushed within timeout")
    if failed > 0 or errors:
        raise RuntimeError(f"Kafka delivery failed for {failed} message(s): {errors[:5]}")

    if own_producer:
        active_producer.poll(0)

    return ProduceResult(
        total_sent=total,
        acknowledged=acked,
        failed=failed,
        partitions_used=tuple(sorted(partitions)),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish AtmoSync sensor readings to Apache Kafka.")
    parser.add_argument(
        "--mode",
        choices=("csv", "live"),
        default="csv",
        help="Replay records from CSV (safe to rerun) or generate live readings (default: csv)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"CSV path for --mode csv (default: {DEFAULT_CSV_PATH.name})",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max records in csv mode")
    parser.add_argument("--ticks", type=int, default=1, help="Number of batches in live mode (default: 1)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between live ticks (default: 1.0)")
    parser.add_argument("--ensure-topics", action="store_true", help="Create Kafka topics if missing before sending")
    args = parser.parse_args(argv)

    cfg = load_kafka_config()
    if args.ensure_topics:
        ensure_topics(cfg)

    if args.mode == "csv":
        records = parse_and_validate_csv(args.csv)
        if args.limit is not None:
            records = records[: args.limit]
        result = publish_records(records, config=cfg)
        print(
            f"Produced {result.acknowledged}/{result.total_sent} CSV messages to topic {cfg.topic!r} "
            f"(partitions={list(result.partitions_used)})"
        )
        return 0

    total_ack = 0
    for tick in range(args.ticks):
        batch = generate_live_batch()
        result = publish_records(batch, config=cfg)
        total_ack += result.acknowledged
        if tick + 1 < args.ticks and args.interval > 0:
            time.sleep(args.interval)
    print(f"Produced {total_ack} live messages across {args.ticks} tick(s) to topic {cfg.topic!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
