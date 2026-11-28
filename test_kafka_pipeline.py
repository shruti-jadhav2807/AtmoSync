"""Unit and integration tests for AtmoSync Phase 2 Apache Kafka pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from data_engineering.atmosync.config import DEFAULT_CSV_PATH, load_database_config
from data_engineering.atmosync.consumer import (
    decode_and_validate_message,
    process_message_batch,
    run_consumer,
)
from data_engineering.atmosync.db import get_connection
from data_engineering.atmosync.ingest import SensorReadingRecord, parse_and_validate_csv
from data_engineering.atmosync.kafka_config import KafkaConfig, load_kafka_config
from data_engineering.atmosync.kafka_topics import ensure_topics
from data_engineering.atmosync.producer import generate_live_batch, publish_records, serialize_record
from data_engineering.atmosync.validate import run_validation


class _FakeKafkaMessage:
    def __init__(
        self,
        value: bytes | None,
        key: bytes | None = b"C001",
        topic: str = "sensor.readings",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


def _require_kafka(cfg: KafkaConfig) -> None:
    try:
        admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
        admin.list_topics(timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Kafka broker unavailable at {cfg.bootstrap_servers}: {exc}")


def _require_db() -> psycopg.Connection:
    try:
        return get_connection(load_database_config())
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")


def test_producer_serialization_and_container_key() -> None:
    record = SensorReadingRecord(
        timestamp_utc="2026-11-23T17:00:00Z",
        container_id="C002",
        product="tomato",
        route="Nashik-Delhi",
        temperature_c="7.12",
        humidity_pct="70.97",
        vibration_g="3.57",
    )
    raw = serialize_record(record)
    decoded = json.loads(raw.decode("utf-8"))
    assert decoded["timestamp_utc"] == "2026-11-23T17:00:00Z"
    assert decoded["container_id"] == "C002"
    assert decoded["temperature_c"] == 7.12

    fake_producer = MagicMock()
    delivered: list[tuple[bytes, bytes]] = []

    def fake_produce(topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        delivered.append((key, value))
        msg = MagicMock()
        msg.partition.return_value = 2
        on_delivery(None, msg)

    fake_producer.produce.side_effect = fake_produce
    fake_producer.flush.return_value = 0

    result = publish_records([record], config=load_kafka_config(), producer=fake_producer)
    assert result.total_sent == 1
    assert result.acknowledged == 1
    assert result.partitions_used == (2,)
    assert delivered[0][0] == b"C002"


def test_decode_and_validate_handles_valid_and_invalid_payloads() -> None:
    valid_bytes = json.dumps(
        {
            "timestamp_utc": "2026-11-25T16:00:00Z",
            "container_id": "C001",
            "product": "avocado",
            "route": "Pune-Mumbai",
            "temperature_c": 5.38,
            "humidity_pct": 65.05,
            "vibration_g": 1.87,
        }
    ).encode("utf-8")
    rec, err = decode_and_validate_message(valid_bytes)
    assert err is None
    assert rec is not None
    assert rec.temperature_c == Decimal("5.38")

    rec_bad_json, err_bad_json = decode_and_validate_message(b"{not-json")
    assert rec_bad_json is None
    assert err_bad_json is not None and "Malformed JSON" in err_bad_json

    rec_bad_schema, err_bad_schema = decode_and_validate_message(
        json.dumps({"container_id": "C001", "humidity_pct": 140}).encode("utf-8")
    )
    assert rec_bad_schema is None
    assert err_bad_schema is not None and "Schema validation error" in err_bad_schema


def test_consumer_routes_invalid_messages_to_dlq_and_commits_after_db() -> None:
    valid_payload = serialize_record(generate_live_batch(datetime(2026, 11, 28, 0, 0, tzinfo=timezone.utc))[0])
    messages = [
        _FakeKafkaMessage(valid_payload, key=b"C001", offset=1),
        _FakeKafkaMessage(b"not-valid-json", key=b"C001", offset=2),
        _FakeKafkaMessage(
            json.dumps(
                {
                    "timestamp_utc": "2026-11-28T00:00:00Z",
                    "container_id": "C002",
                    "product": "tomato",
                    "route": "Nashik-Delhi",
                    "temperature_c": 8.0,
                    "humidity_pct": 150.0,
                    "vibration_g": 2.0,
                }
            ).encode("utf-8"),
            key=b"C002",
            offset=3,
        ),
    ]

    mock_consumer = MagicMock()
    mock_conn = MagicMock()
    mock_dlq = MagicMock()
    dlq_sent: list[dict[str, Any]] = []

    def fake_dlq_produce(topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        dlq_sent.append({"topic": topic, "key": key, "envelope": json.loads(value.decode("utf-8"))})
        on_delivery(None, MagicMock())

    mock_dlq.produce.side_effect = fake_dlq_produce
    mock_dlq.flush.return_value = 0

    inserted_records: list[SensorReadingRecord] = []

    def fake_insert(_conn: Any, recs: Any) -> tuple[int, int]:
        inserted_records.extend(recs)
        return len(recs), 0

    stats = process_message_batch(
        messages=messages,
        consumer=mock_consumer,
        conn=mock_conn,
        dlq_producer=mock_dlq,
        dlq_topic="sensor.readings.dlq",
        insert_fn=fake_insert,
    )

    assert stats.consumed_messages == 3
    assert stats.valid_messages == 1
    assert stats.inserted_rows == 1
    assert stats.dlq_messages == 2
    assert len(inserted_records) == 1
    assert len(dlq_sent) == 2
    assert dlq_sent[0]["envelope"]["raw_payload"] == "not-valid-json"
    mock_consumer.commit.assert_called_once_with(asynchronous=False)


def test_consumer_does_not_commit_offsets_when_postgres_fails() -> None:
    valid_payload = serialize_record(generate_live_batch(datetime(2026, 11, 28, 0, 0, tzinfo=timezone.utc))[0])
    messages = [_FakeKafkaMessage(valid_payload, key=b"C001", offset=10)]

    mock_consumer = MagicMock()
    mock_conn = MagicMock()
    mock_dlq = MagicMock()
    mock_dlq.flush.return_value = 0

    def failing_insert(_conn: Any, _recs: Any) -> tuple[int, int]:
        raise psycopg.OperationalError("simulated DB failure")

    with pytest.raises(psycopg.OperationalError, match="simulated DB failure"):
        process_message_batch(
            messages=messages,
            consumer=mock_consumer,
            conn=mock_conn,
            dlq_producer=mock_dlq,
            dlq_topic="sensor.readings.dlq",
            insert_fn=failing_insert,
        )

    mock_consumer.commit.assert_not_called()


def test_kafka_topics_provisioned() -> None:
    cfg = load_kafka_config()
    _require_kafka(cfg)

    topics = ensure_topics(cfg, num_partitions=5)
    assert topics[cfg.topic] == 5
    assert topics[cfg.dlq_topic] == 1


def test_end_to_end_kafka_csv_replay_preserves_240_rows() -> None:
    base_cfg = load_kafka_config()
    _require_kafka(base_cfg)
    conn = _require_db()

    try:
        ensure_topics(base_cfg, num_partitions=5)
        records = parse_and_validate_csv(DEFAULT_CSV_PATH)
        prod_result = publish_records(records, config=base_cfg)
        assert prod_result.acknowledged == 240
        assert len(prod_result.partitions_used) > 1

        test_cfg = KafkaConfig(
            bootstrap_servers=base_cfg.bootstrap_servers,
            topic=base_cfg.topic,
            dlq_topic=base_cfg.dlq_topic,
            consumer_group=f"atmosync-test-{uuid4().hex[:8]}",
        )
        stats = run_consumer(
            kafka_config=test_cfg,
            conn=conn,
            batch_size=100,
            poll_timeout=0.5,
            idle_timeout_sec=3.0,
        )
        assert stats.consumed_messages >= 240
        assert stats.inserted_rows == 0
        assert stats.dlq_messages == 0

        report = run_validation(conn=conn)
        assert report.total_records == 240
        assert report.duplicate_groups == 0
    finally:
        conn.close()


def test_end_to_end_kafka_dlq_routing_isolated_topic() -> None:
    base_cfg = load_kafka_config()
    _require_kafka(base_cfg)
    conn = _require_db()

    suffix = uuid4().hex[:8]
    test_topic = f"atmosync.test.{suffix}"
    test_dlq = f"atmosync.test.{suffix}.dlq"

    admin = AdminClient({"bootstrap.servers": base_cfg.bootstrap_servers})
    futures = admin.create_topics(
        [
            NewTopic(test_topic, num_partitions=1, replication_factor=1),
            NewTopic(test_dlq, num_partitions=1, replication_factor=1),
        ],
        request_timeout=10,
    )
    for fut in futures.values():
        fut.result()

    try:
        producer = Producer({"bootstrap.servers": base_cfg.bootstrap_servers, "acks": "all"})
        producer.produce(test_topic, key=b"C001", value=b"{invalid-json")
        producer.flush(10)

        test_cfg = KafkaConfig(
            bootstrap_servers=base_cfg.bootstrap_servers,
            topic=test_topic,
            dlq_topic=test_dlq,
            consumer_group=f"atmosync-dlq-test-{suffix}",
        )
        stats = run_consumer(
            kafka_config=test_cfg,
            conn=conn,
            batch_size=10,
            poll_timeout=0.5,
            max_messages=1,
            idle_timeout_sec=3.0,
        )
        assert stats.consumed_messages == 1
        assert stats.valid_messages == 0
        assert stats.dlq_messages == 1

        dlq_consumer = Consumer(
            {
                "bootstrap.servers": base_cfg.bootstrap_servers,
                "group.id": f"atmosync-dlq-reader-{suffix}",
                "auto.offset.reset": "earliest",
            }
        )
        try:
            dlq_consumer.subscribe([test_dlq])
            dlq_msg = None
            for _ in range(10):
                polled = dlq_consumer.poll(0.5)
                if polled is not None and not polled.error():
                    dlq_msg = polled
                    break
            assert dlq_msg is not None
            envelope = json.loads(dlq_msg.value().decode("utf-8"))
            assert envelope["source_topic"] == test_topic
            assert envelope["raw_payload"] == "{invalid-json"
            assert "Malformed JSON" in envelope["error"]
        finally:
            dlq_consumer.close()
    finally:
        admin.delete_topics([test_topic, test_dlq], operation_timeout=10)
        conn.close()
