"""Independent Kafka-to-PostgreSQL consumer for AtmoSync with DLQ and manual commits."""

from __future__ import annotations

import argparse
import base64
import json
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

import psycopg
from confluent_kafka import Consumer, KafkaError, Producer

from data_engineering.atmosync.config import DatabaseConfig
from data_engineering.atmosync.db import get_connection
from data_engineering.atmosync.ingest import SensorReadingRecord, insert_records
from data_engineering.atmosync.kafka_config import KafkaConfig, load_kafka_config
from data_engineering.atmosync.kafka_topics import ensure_topics

InsertFn = Callable[[psycopg.Connection, Sequence[SensorReadingRecord]], tuple[int, int]]


@dataclass(frozen=True)
class ConsumerStats:
    """Execution metrics for a consumer batch or run."""

    consumed_messages: int
    valid_messages: int
    inserted_rows: int
    skipped_duplicates: int
    dlq_messages: int
    committed_batches: int


def decode_and_validate_message(raw_value: bytes | None) -> tuple[SensorReadingRecord | None, str | None]:
    """Decode a Kafka payload as JSON and validate against SensorReadingRecord."""
    if raw_value is None:
        return None, "Empty (None) Kafka message value"
    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, f"Invalid UTF-8 payload: {exc}"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"Malformed JSON payload: {exc}"

    if not isinstance(payload, dict):
        return None, f"JSON payload must be an object, got {type(payload).__name__}"

    try:
        record = SensorReadingRecord.model_validate(payload)
    except Exception as exc:  # noqa: BLE001 - capture full Pydantic validation reason
        return None, f"Schema validation error: {exc}"

    return record, None


def build_dlq_payload(msg: Any, error_reason: str) -> bytes:
    """Create a structured DLQ JSON envelope preserving original metadata and payload."""
    raw_val = msg.value()
    if raw_val is None:
        encoded_payload: str | None = None
        payload_encoding = "none"
    else:
        try:
            encoded_payload = raw_val.decode("utf-8")
            payload_encoding = "utf-8"
        except UnicodeDecodeError:
            encoded_payload = base64.b64encode(raw_val).decode("ascii")
            payload_encoding = "base64"

    raw_key = msg.key()
    key_str = raw_key.decode("utf-8", errors="replace") if raw_key is not None else None

    envelope = {
        "source_topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": key_str,
        "error": error_reason,
        "payload_encoding": payload_encoding,
        "raw_payload": encoded_payload,
        "failed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return json.dumps(envelope, separators=(",", ":")).encode("utf-8")


def send_to_dlq(
    dlq_producer: Producer,
    dlq_topic: str,
    messages_with_errors: Sequence[tuple[Any, str]],
) -> int:
    """Publish invalid messages to the DLQ topic and flush before committing offsets."""
    if not messages_with_errors:
        return 0

    failed = 0
    errors: list[str] = []

    def on_delivery(err: object, _msg: object) -> None:
        nonlocal failed
        if err is not None:
            failed += 1
            errors.append(str(err))

    for msg, reason in messages_with_errors:
        dlq_producer.produce(
            topic=dlq_topic,
            key=msg.key(),
            value=build_dlq_payload(msg, reason),
            on_delivery=on_delivery,
        )
        dlq_producer.poll(0)

    remaining = dlq_producer.flush(timeout=15)
    if remaining > 0 or failed > 0:
        raise RuntimeError(f"Failed to flush DLQ messages (remaining={remaining}, failed={failed}, errors={errors[:3]})")
    return len(messages_with_errors)


def process_message_batch(
    messages: Sequence[Any],
    consumer: Any,
    conn: psycopg.Connection,
    dlq_producer: Producer,
    dlq_topic: str,
    insert_fn: InsertFn = insert_records,
) -> ConsumerStats:
    """Validate a batch, persist valid records, route invalid ones to DLQ, then commit offsets."""
    if not messages:
        return ConsumerStats(0, 0, 0, 0, 0, 0)

    valid_records: list[SensorReadingRecord] = []
    invalid_items: list[tuple[Any, str]] = []

    for msg in messages:
        record, err = decode_and_validate_message(msg.value())
        if record is not None:
            valid_records.append(record)
        else:
            invalid_items.append((msg, err or "Unknown validation error"))

    dlq_count = send_to_dlq(dlq_producer, dlq_topic, invalid_items)

    inserted = 0
    skipped = 0
    if valid_records:
        inserted, skipped = insert_fn(conn, valid_records)

    # Commit offsets strictly AFTER both PostgreSQL writes and DLQ flushes succeed.
    consumer.commit(asynchronous=False)

    return ConsumerStats(
        consumed_messages=len(messages),
        valid_messages=len(valid_records),
        inserted_rows=inserted,
        skipped_duplicates=skipped,
        dlq_messages=dlq_count,
        committed_batches=1,
    )


def build_consumer(config: KafkaConfig, auto_offset_reset: str = "earliest") -> Consumer:
    """Create a confluent_kafka Consumer with manual offset commits."""
    return Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": auto_offset_reset,
        }
    )


def run_consumer(
    kafka_config: KafkaConfig | None = None,
    db_config: DatabaseConfig | None = None,
    conn: psycopg.Connection | None = None,
    batch_size: int = 50,
    poll_timeout: float = 1.0,
    max_messages: int | None = None,
    idle_timeout_sec: float | None = None,
) -> ConsumerStats:
    """Consume sensor readings from Kafka and persist them idempotently to PostgreSQL."""
    kcfg = kafka_config or load_kafka_config()
    consumer = build_consumer(kcfg)
    dlq_producer = Producer({"bootstrap.servers": kcfg.bootstrap_servers, "acks": "all"})

    own_conn = conn is None
    active_conn = conn if conn is not None else get_connection(db_config)

    stop_requested = False

    def handle_signal(_signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    total_consumed = 0
    total_valid = 0
    total_inserted = 0
    total_skipped = 0
    total_dlq = 0
    total_batches = 0
    pending: list[Any] = []
    assigned = False
    started_at = datetime.now(timezone.utc)
    last_message_time = started_at

    def on_assign(_consumer: Any, _partitions: Any) -> None:
        nonlocal assigned, last_message_time
        assigned = True
        last_message_time = datetime.now(timezone.utc)

    try:
        consumer.subscribe([kcfg.topic], on_assign=on_assign)

        while not stop_requested:
            if max_messages is not None and total_consumed + len(pending) >= max_messages:
                break

            msg = consumer.poll(timeout=poll_timeout)
            now = datetime.now(timezone.utc)

            if msg is None:
                if pending:
                    stats = process_message_batch(
                        pending, consumer, active_conn, dlq_producer, kcfg.dlq_topic
                    )
                    total_consumed += stats.consumed_messages
                    total_valid += stats.valid_messages
                    total_inserted += stats.inserted_rows
                    total_skipped += stats.skipped_duplicates
                    total_dlq += stats.dlq_messages
                    total_batches += stats.committed_batches
                    pending.clear()
                if idle_timeout_sec is not None:
                    if assigned and (now - last_message_time).total_seconds() >= idle_timeout_sec:
                        break
                    if not assigned and (now - started_at).total_seconds() >= max(idle_timeout_sec * 5, 30.0):
                        break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consumer error: {msg.error()}")

            last_message_time = now
            pending.append(msg)

            if len(pending) >= batch_size or (
                max_messages is not None and total_consumed + len(pending) >= max_messages
            ):
                stats = process_message_batch(
                    pending, consumer, active_conn, dlq_producer, kcfg.dlq_topic
                )
                total_consumed += stats.consumed_messages
                total_valid += stats.valid_messages
                total_inserted += stats.inserted_rows
                total_skipped += stats.skipped_duplicates
                total_dlq += stats.dlq_messages
                total_batches += stats.committed_batches
                pending.clear()

        if pending:
            stats = process_message_batch(
                pending, consumer, active_conn, dlq_producer, kcfg.dlq_topic
            )
            total_consumed += stats.consumed_messages
            total_valid += stats.valid_messages
            total_inserted += stats.inserted_rows
            total_skipped += stats.skipped_duplicates
            total_dlq += stats.dlq_messages
            total_batches += stats.committed_batches
            pending.clear()

        return ConsumerStats(
            consumed_messages=total_consumed,
            valid_messages=total_valid,
            inserted_rows=total_inserted,
            skipped_duplicates=total_skipped,
            dlq_messages=total_dlq,
            committed_batches=total_batches,
        )
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        consumer.close()
        if own_conn:
            active_conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consume AtmoSync sensor readings from Kafka into PostgreSQL.")
    parser.add_argument("--batch-size", type=int, default=50, help="Commit batch size (default: 50)")
    parser.add_argument("--poll-timeout", type=float, default=1.0, help="Poll timeout seconds (default: 1.0)")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after N messages")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="Stop after N idle seconds with no new messages (useful for bounded runs)",
    )
    parser.add_argument("--ensure-topics", action="store_true", help="Create Kafka topics if missing before subscribing")
    args = parser.parse_args(argv)

    kcfg = load_kafka_config()
    if args.ensure_topics:
        ensure_topics(kcfg)

    stats = run_consumer(
        kafka_config=kcfg,
        batch_size=args.batch_size,
        poll_timeout=args.poll_timeout,
        max_messages=args.max_messages,
        idle_timeout_sec=args.idle_timeout,
    )
    print(
        f"Consumer finished: consumed={stats.consumed_messages}, valid={stats.valid_messages}, "
        f"inserted={stats.inserted_rows}, skipped_duplicates={stats.skipped_duplicates}, "
        f"dlq={stats.dlq_messages}, committed_batches={stats.committed_batches}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
