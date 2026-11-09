"""Round trip through a real Redpanda broker (needs `docker compose up -d`).

Run with:  pytest -m integration
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

from conftest import as_json, make_page_change

pytestmark = pytest.mark.integration

confluent_kafka = pytest.importorskip("confluent_kafka")

BOOTSTRAP = os.environ.get("WIKIPULSE_KAFKA_BOOTSTRAP", "localhost:19092")


def consume_all(topic: str, expected: int, timeout_s: float = 20.0) -> list:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"it-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list = []
    import time

    deadline = time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        out.append(msg)
    consumer.close()
    return out


def test_valid_events_and_dead_letters_reach_their_topics(tmp_path):
    from wikipulse.producer.checkpoint import FileCheckpoint
    from wikipulse.producer.kafka_io import ensure_topics, make_producer
    from wikipulse.producer.main import EditProducer
    from wikipulse.producer.normalize import Filters
    from wikipulse.wikimedia.sse import SSEEvent

    run_id = uuid.uuid4().hex[:8]
    edits_topic, dlq_topic = f"it.edits.{run_id}", f"it.dlq.{run_id}"
    ensure_topics(BOOTSTRAP, edits_topic, dlq_topic, partitions=3)
    ensure_topics(BOOTSTRAP, edits_topic, dlq_topic, partitions=3)  # idempotent

    events = [
        SSEEvent(
            data=as_json(
                make_page_change(
                    meta={"id": f"evt-{i}"}, page={"page_id": 100 + i % 3}, revision={"rev_id": i}
                )
            ),
            id=f"sse-{i}",
        )
        for i in range(20)
    ]
    events.insert(5, SSEEvent(data="{not json", id="sse-bad"))

    checkpoint = FileCheckpoint(tmp_path / "cp.json", "https://stream.test")
    producer = EditProducer(
        make_producer(BOOTSTRAP),
        edits_topic=edits_topic,
        dlq_topic=dlq_topic,
        filters=Filters(frozenset({"enwiki"}), frozenset({0})),
        checkpoint=checkpoint,
    )
    stats = producer.run(events)
    assert (stats.produced, stats.dead_letters) == (20, 1)
    assert checkpoint.load() == "sse-19"

    edits = consume_all(edits_topic, 20)
    assert sorted(json.loads(m.value())["event_id"] for m in edits) == sorted(
        f"evt-{i}" for i in range(20)
    )
    # Same page -> same partition (per-page ordering).
    partitions_by_key: dict[bytes, set[int]] = {}
    for m in edits:
        partitions_by_key.setdefault(m.key(), set()).add(m.partition())
    assert all(len(p) == 1 for p in partitions_by_key.values())

    dlq = consume_all(dlq_topic, 1)
    assert len(dlq) == 1
    assert json.loads(dlq[0].value())["reason"] == "invalid_json"
