from __future__ import annotations

import json

import pytest

from conftest import as_json, make_page_change
from wikipulse.producer.checkpoint import FileCheckpoint
from wikipulse.producer.main import DeliveryFailed, EditProducer
from wikipulse.producer.normalize import Filters
from wikipulse.wikimedia.sse import SSEEvent

FILTERS = Filters(wikis=frozenset({"enwiki"}), namespaces=frozenset({0}))


class FakeKafka:
    """Records messages; delivers them (or fails them) on flush, like librdkafka."""

    def __init__(self, fail_topics: set[str] | None = None, stuck: int = 0):
        self.pending: list[dict] = []
        self.delivered: list[dict] = []
        self.fail_topics = fail_topics or set()
        self.stuck = stuck
        self.flushes = 0

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        self.pending.append(
            {"topic": topic, "key": key, "value": value, "headers": headers, "cb": on_delivery}
        )

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=0):
        self.flushes += 1
        for msg in self.pending:
            err = "broker down" if msg["topic"] in self.fail_topics else None
            msg["cb"](err, msg)
            if err is None:
                self.delivered.append(msg)
        self.pending = []
        return self.stuck

    def on(self, topic):
        return [m for m in self.delivered if m["topic"] == topic]


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def sse(i: int, doc) -> SSEEvent:
    return SSEEvent(data=doc if isinstance(doc, str) else as_json(doc), id=f"id-{i}")


def edit(i: int, **kw):
    return make_page_change(
        meta={"id": f"evt-{i}"}, revision={"rev_id": 3000 + i}, page={"page_id": 10 + i % 2}, **kw
    )


def make(kafka, tmp_path, **kw):
    cp = FileCheckpoint(tmp_path / "cp.json", "https://s/x")
    p = EditProducer(
        kafka, edits_topic="edits", dlq_topic="dlq", filters=FILTERS, checkpoint=cp, **kw
    )
    return p, cp


def test_routes_valid_skipped_and_invalid(tmp_path):
    kafka = FakeKafka()
    p, cp = make(kafka, tmp_path)
    events = [
        sse(1, edit(1)),
        sse(2, edit(2, wiki_id="dewiki")),
        sse(3, "{broken"),
        sse(4, edit(4)),
    ]
    stats = p.run(events)

    edits = kafka.on("edits")
    assert [json.loads(m["value"])["event_id"] for m in edits] == ["evt-1", "evt-4"]
    assert edits[0]["key"] == b"enwiki:11"
    assert edits[0]["headers"] == [("schema_version", b"2")]

    dlq = kafka.on("dlq")
    assert len(dlq) == 1
    assert json.loads(dlq[0]["value"])["reason"] == "invalid_json"
    assert dlq[0]["headers"] == [("reason", b"invalid_json")]

    assert (stats.read, stats.produced, stats.dead_letters) == (4, 2, 1)
    assert stats.skipped == {"wiki": 1}
    assert cp.load() == "id-4"  # last event handled, after final flush


def test_checkpoint_advances_only_after_flush(tmp_path):
    kafka, clock = FakeKafka(), FakeClock()
    p, cp = make(kafka, tmp_path, flush_interval_s=10, clock=clock)

    p.handle(sse(1, edit(1)))
    assert cp.load() is None and kafka.delivered == []  # not flushed yet

    p.commit()
    assert cp.load() == "id-1" and len(kafka.delivered) == 1


def test_periodic_flush_during_run(tmp_path):
    kafka, clock = FakeKafka(), FakeClock()
    p, _ = make(kafka, tmp_path, flush_interval_s=1.0, clock=clock)

    def ticking():
        for i in range(5):
            clock.t += 0.6
            yield sse(i, edit(i))

    p.run(ticking())
    assert kafka.flushes >= 3  # periodic flushes + final flush


def test_delivery_failure_does_not_advance_checkpoint(tmp_path):
    kafka = FakeKafka()
    p, cp = make(kafka, tmp_path)
    p.handle(sse(1, edit(1)))
    p.commit()
    assert cp.load() == "id-1"

    kafka.fail_topics = {"edits"}
    p.handle(sse(2, edit(2)))
    with pytest.raises(DeliveryFailed):
        p.commit()
    assert cp.load() == "id-1"  # restart will resend event 2 (at-least-once)


def test_unconfirmed_messages_do_not_advance_checkpoint(tmp_path):
    kafka = FakeKafka(stuck=3)
    p, cp = make(kafka, tmp_path)
    p.handle(sse(1, edit(1)))
    with pytest.raises(DeliveryFailed):
        p.commit()
    assert cp.load() is None


def test_max_events_stops_the_run(tmp_path):
    kafka = FakeKafka()
    p, _ = make(kafka, tmp_path)
    stats = p.run((sse(i, edit(i)) for i in range(100)), max_events=3)
    assert stats.produced == 3 and len(kafka.on("edits")) == 3


def test_max_seconds_stops_the_run(tmp_path):
    kafka, clock = FakeKafka(), FakeClock()
    p, _ = make(kafka, tmp_path, clock=clock)

    def forever():
        i = 0
        while True:
            clock.t += 1
            i += 1
            yield sse(i, edit(i))

    stats = p.run(forever(), max_seconds=5)
    assert stats.read == 5


def test_stream_error_still_flushes_and_checkpoints(tmp_path):
    kafka = FakeKafka()
    p, cp = make(kafka, tmp_path)

    def broken():
        yield sse(1, edit(1))
        raise ConnectionError("gave up")

    with pytest.raises(ConnectionError):
        p.run(broken())
    assert cp.load() == "id-1" and len(kafka.on("edits")) == 1
