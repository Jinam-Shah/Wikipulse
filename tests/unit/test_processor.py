from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from conftest import make_page_change
from wikipulse.processor.main import StreamProcessor
from wikipulse.processor.store import BatchResult
from wikipulse.producer.normalize import Filters, normalize

NOW = datetime(2026, 11, 9, 3, 0, tzinfo=UTC)
FILTERS = Filters(frozenset({"enwiki"}), frozenset({0}))


def edit_json(i: int) -> bytes:
    doc = make_page_change(meta={"id": f"evt-{i}"}, revision={"rev_id": 5000 + i})
    return normalize(json.dumps(doc), FILTERS, now=lambda: NOW).model_dump_json().encode()


class Msg:
    def __init__(self, value: bytes, offset: int = 0):
        self._value, self._offset = value, offset

    def error(self):
        return None

    def value(self):
        return self._value

    def topic(self):
        return "edits"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, log: list[str], batches: list[list[Msg]] | None = None):
        self.log = log
        self.batches = list(batches or [])
        self.commits = 0

    def consume(self, num_messages=1, timeout=1.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=True):
        assert asynchronous is False
        self.commits += 1
        self.log.append("commit")

    def assignment(self):
        return []


class FakeStore:
    def __init__(self, log: list[str], fail_times: int = 0):
        self.log = log
        self.fail_times = fail_times
        self.written: list[str] = []
        self.conn = self
        self.closed = False

    def close(self):
        self.closed = True

    def write_batch(self, events, cutoff):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError("db down")
        self.log.append("db")
        self.written += [e.event_id for e in events]
        return BatchResult(len(events), 0, 0, tuple(events))

    def prune(self, cutoff):
        return 0


class FakeDLQ:
    def __init__(self, log: list[str], unconfirmed: int = 0):
        self.log = log
        self.messages: list[dict] = []
        self.unconfirmed = unconfirmed

    def produce(self, topic, value=None, headers=None):
        self.messages.append({"topic": topic, "value": value, "headers": headers})

    def flush(self, timeout=0):
        self.log.append("dlq_flush")
        return self.unconfirmed


def make(batches=None, store=None, dlq=None, **kw):
    log: list[str] = []
    consumer = FakeConsumer(log, batches)
    store = store or FakeStore(log)
    store.log = log
    dlq = dlq or FakeDLQ(log)
    dlq.log = log
    sleeps: list[float] = []
    proc = StreamProcessor(
        consumer,
        lambda: store,
        dlq,
        dlq_topic="dlq",
        now=lambda: NOW,
        sleep=sleeps.append,
        **kw,
    )
    return proc, consumer, store, dlq, log, sleeps


def test_valid_messages_are_written_then_offsets_committed():
    proc, consumer, store, dlq, log, _ = make()
    result = proc.process_batch([Msg(edit_json(1)), Msg(edit_json(2))])
    assert result.inserted == 2
    assert store.written == ["evt-1", "evt-2"]
    assert log == ["db", "dlq_flush", "commit"]  # DB first, offsets last


def test_invalid_message_goes_to_dlq_and_batch_still_commits():
    proc, consumer, store, dlq, log, _ = make()
    proc.process_batch([Msg(b'{"not": "an edit"}', offset=7), Msg(edit_json(1))])
    assert store.written == ["evt-1"]
    assert len(dlq.messages) == 1
    headers = dict(dlq.messages[0]["headers"])
    assert headers["reason"] == b"processor_validation"
    assert headers["source"] == b"edits/0/7"
    assert consumer.commits == 1 and proc.totals.dead_letters == 1


def test_db_failure_is_retried_with_backoff():
    log: list[str] = []
    proc, consumer, store, dlq, log, sleeps = make(store=FakeStore(log, fail_times=2))
    proc.process_batch([Msg(edit_json(1))])
    assert store.written == ["evt-1"]
    assert sleeps == [1.0, 2.0]
    assert consumer.commits == 1


def test_db_failure_after_all_retries_does_not_commit_offsets():
    log: list[str] = []
    proc, consumer, store, dlq, log, sleeps = make(
        store=FakeStore(log, fail_times=99), max_db_retries=3
    )
    with pytest.raises(ConnectionError):
        proc.process_batch([Msg(edit_json(1))])
    assert consumer.commits == 0  # Kafka will redeliver after restart


def test_unconfirmed_dead_letters_block_the_offset_commit():
    log: list[str] = []
    proc, consumer, *_ = make(dlq=FakeDLQ(log, unconfirmed=1))
    with pytest.raises(RuntimeError):
        proc.process_batch([Msg(b"garbage")])
    assert consumer.commits == 0


def test_run_stops_when_idle_and_counts_everything():
    clock = {"t": 0.0}

    def tick():
        clock["t"] += 1.0
        return clock["t"]

    log: list[str] = []
    store = FakeStore(log)
    proc, consumer, *_ = make(
        batches=[[Msg(edit_json(1))], [Msg(edit_json(2)), Msg(b"bad")]], store=store
    )
    proc.clock = tick
    totals = proc.run(max_idle_s=3)
    assert (totals.consumed, totals.inserted, totals.dead_letters, totals.batches) == (3, 2, 1, 2)
    assert consumer.commits == 2
