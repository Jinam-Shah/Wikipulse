"""Kafka (Redpanda) plumbing: producer configuration and topic creation."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)

DAY_MS = 24 * 60 * 60 * 1000


def make_producer(bootstrap: str, client_id: str = "wikipulse-producer") -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            # Idempotence implies acks=all and prevents duplicates from internal retries.
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
        }
    )


def ensure_topics(
    bootstrap: str,
    edits_topic: str,
    dlq_topic: str,
    partitions: int = 3,
    timeout_s: float = 15.0,
) -> None:
    """Create topics if missing. Retention: 7 days for edits, 14 for dead letters."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    wanted = [
        NewTopic(edits_topic, partitions, 1, config={"retention.ms": str(7 * DAY_MS)}),
        NewTopic(dlq_topic, 1, 1, config={"retention.ms": str(14 * DAY_MS)}),
    ]
    missing = [t for t in wanted if t.topic not in existing]
    if not missing:
        return
    for topic, fut in admin.create_topics(missing).items():
        try:
            fut.result(timeout=timeout_s)
            log.info("created topic %s", topic)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
