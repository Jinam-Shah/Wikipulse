"""Kafka -> processor -> Postgres, then a full replay with a new consumer group."""

from __future__ import annotations

import json
import os
import uuid

import pytest

from scenarios import SCENARIO, page_metrics, wiki_metrics

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")
BOOTSTRAP = os.environ.get("WIKIPULSE_KAFKA_BOOTSTRAP", "localhost:19092")


def run_processor(url, topic, dlq_topic, group):
    from confluent_kafka import Consumer

    from wikipulse.processor.main import StreamProcessor
    from wikipulse.processor.store import PostgresStore
    from wikipulse.producer.kafka_io import make_producer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    proc = StreamProcessor(
        consumer,
        lambda: PostgresStore(psycopg.connect(url, autocommit=True)),
        make_producer(BOOTSTRAP),
        dlq_topic=dlq_topic,
        retention_days=10_000,  # synthetic events are dated 2026-11-09
        batch_size=3,
        batch_timeout_s=0.5,
    )
    try:
        totals = proc.run(max_idle_s=8)
        return totals, proc.update_lag()
    finally:
        consumer.close()


def test_end_to_end_and_full_replay(fresh_db_url):
    from wikipulse.producer.kafka_io import ensure_topics, make_producer

    run_id = uuid.uuid4().hex[:8]
    topic, dlq_topic = f"it.edits.{run_id}", f"it.dlq.{run_id}"
    ensure_topics(BOOTSTRAP, topic, dlq_topic, partitions=3)

    producer = make_producer(BOOTSTRAP)
    for ev in SCENARIO:
        producer.produce(topic, key=ev.kafka_key.encode(), value=ev.model_dump_json().encode())
    producer.produce(topic, key=b"enwiki:999", value=b'{"schema_version": 1}')  # invalid
    for ev in SCENARIO[:2]:  # duplicates, as after a producer crash
        producer.produce(topic, key=ev.kafka_key.encode(), value=ev.model_dump_json().encode())
    assert producer.flush(10) == 0

    totals, lag = run_processor(fresh_db_url, topic, dlq_topic, f"g1-{run_id}")
    assert (totals.consumed, totals.inserted, totals.duplicates, totals.dead_letters) == (
        10,
        7,
        2,
        1,
    )
    assert lag == 0  # every offset committed
    first = (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url))
    assert len(first[0]) == 3

    # Replay the whole topic from the beginning with a brand-new consumer group.
    totals2, _ = run_processor(fresh_db_url, topic, dlq_topic, f"g2-{run_id}")
    assert (totals2.inserted, totals2.duplicates) == (0, 9)
    assert (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url)) == first

    # Restarting the *same* group resumes after the committed offsets: nothing to do.
    totals3, _ = run_processor(fresh_db_url, topic, dlq_topic, f"g1-{run_id}")
    assert totals3.consumed == 0

    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"dlq-{run_id}",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([dlq_topic])
    got = []
    for _ in range(40):
        msg = c.poll(0.5)
        if msg is not None and not msg.error():
            got.append(json.loads(msg.value()))
        if len(got) >= 2:
            break
    c.close()
    assert [g["reason"] for g in got] == ["processor_validation", "processor_validation"]
