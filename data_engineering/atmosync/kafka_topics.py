"""Kafka topic provisioning utilities for AtmoSync."""

from __future__ import annotations

import argparse
from typing import Sequence

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from data_engineering.atmosync.kafka_config import KafkaConfig, load_kafka_config


def ensure_topics(
    config: KafkaConfig | None = None,
    num_partitions: int = 5,
    replication_factor: int = 1,
) -> dict[str, int]:
    """Idempotently create primary and DLQ Kafka topics and return partition counts."""
    cfg = config or load_kafka_config()
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})

    metadata = admin.list_topics(timeout=10)
    desired = {
        cfg.topic: num_partitions,
        cfg.dlq_topic: 1,
    }

    to_create = [
        NewTopic(topic_name, num_partitions=parts, replication_factor=replication_factor)
        for topic_name, parts in desired.items()
        if topic_name not in metadata.topics
    ]

    if to_create:
        futures = admin.create_topics(to_create, request_timeout=15)
        for topic_name, future in futures.items():
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - inspect confluent KafkaException code
                err = exc.args[0] if exc.args else None
                if getattr(err, "code", lambda: None)() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise RuntimeError(f"Failed to create Kafka topic {topic_name!r}: {exc}") from exc

    updated_metadata = admin.list_topics(timeout=10)
    return {
        topic_name: len(updated_metadata.topics[topic_name].partitions)
        for topic_name in desired
        if topic_name in updated_metadata.topics
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create AtmoSync Kafka topics if missing.")
    parser.add_argument("--partitions", type=int, default=5, help="Partitions for primary topic (default: 5)")
    args = parser.parse_args(argv)

    cfg = load_kafka_config()
    topics = ensure_topics(cfg, num_partitions=args.partitions)
    print(f"Kafka topics ready on {cfg.bootstrap_servers}: {topics}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
