"""Apache Kafka configuration helpers for AtmoSync."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from data_engineering.atmosync.config import PROJECT_ROOT


@dataclass(frozen=True)
class KafkaConfig:
    """Apache Kafka connection and topic settings."""

    bootstrap_servers: str
    topic: str
    dlq_topic: str
    consumer_group: str


def load_kafka_config(env_path: Path | None = None) -> KafkaConfig:
    """Load Kafka settings from .env (if present) and process environment."""
    target_env = env_path or (PROJECT_ROOT / ".env")
    if target_env.exists():
        load_dotenv(dotenv_path=target_env, override=False)

    return KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic=os.getenv("KAFKA_TOPIC", "sensor.readings"),
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "sensor.readings.dlq"),
        consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "atmosync-pg-consumer"),
    )
