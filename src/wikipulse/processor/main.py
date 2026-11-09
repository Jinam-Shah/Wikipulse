"""WikiPulse stream processor: Kafka `wikipulse.edits.v1` -> Postgres.

    python -m wikipulse.processor                       # run until Ctrl+C
    python -m wikipulse.processor --max-idle-seconds 10 # stop once the topic is drained

Guarantees (docs/DECISIONS.md D-013, D-014):
- Offsets are committed only after the batch's Postgres transaction commits
  (at-least-once). Re-delivered messages are ignored by ON CONFLICT (event_id).
- Window metrics are recomputed from stored edits, so replays never double-count.
- Messages that fail validation go to the dead-letter topic with a reason.
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError

from wikipulse.config import Settings, load_settings
from wikipulse.processor import metrics as m
from wikipulse.processor.store import BatchResult, PostgresStore
from wikipulse.producer.schemas import DeadLetter, EditEvent

log = logging.getLogger("wikipulse.processor")


@dataclass
class Totals:
    consumed: int = 0
    inserted: int = 0
    duplicates: int = 0
    too_old: int = 0
    dead_letters: int = 0
    batches: int = 0
    started: float = field(default_factory=time.monotonic)

    def summary(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        return (
            f"consumed={self.consumed} inserted={self.inserted} duplicates={self.duplicates} "
            f"too_old={self.too_old} dead_letters={self.dead_letters} batches={self.batches} "
            f"elapsed={elapsed:.1f}s consumed_per_s={self.consumed / elapsed:.1f}"
        )


def _utcnow() -> datetime:
    return datetime.now(UTC)


class StreamProcessor:
    def __init__(
        self,
        consumer: Any,
        store_factory: Callable[[], PostgresStore],
        dlq_producer: Any,
        *,
        dlq_topic: str,
        retention_days: int = 7,
        batch_size: int = 500,
        batch_timeout_s: float = 1.0,
        max_db_retries: int = 5,
        prune_every_s: float = 600.0,
        lag_every_s: float = 5.0,
        now: Callable[[], datetime] = _utcnow,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.consumer = consumer
        self.store_factory = store_factory
        self.store = store_factory()
        self.dlq = dlq_producer
        self.dlq_topic = dlq_topic
        self.retention = timedelta(days=retention_days)
        self.batch_size = batch_size
        self.batch_timeout_s = batch_timeout_s
        self.max_db_retries = max_db_retries
        self.prune_every_s = prune_every_s
        self.lag_every_s = lag_every_s
        self.now = now
        self.clock = clock
        self.sleep = sleep
        self.totals = Totals()
        self._last_prune = float("-inf")
        self._last_lag = float("-inf")

    # ----------------------------------------------------------------- parsing

    def _parse(self, msgs: Sequence[Any]) -> list[EditEvent]:
        events: list[EditEvent] = []
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if getattr(err, "code", lambda: None)() == _partition_eof():
                    continue
                raise RuntimeError(f"Kafka error: {err}")
            raw = msg.value() or b""
            try:
                events.append(EditEvent.model_validate_json(raw))
            except ValidationError as exc:
                self._dead_letter(raw, str(exc), msg)
        return events

    def _dead_letter(self, raw: bytes, error: str, msg: Any) -> None:
        dl = DeadLetter(
            reason="processor_validation",
            error=error[:2000],
            raw=raw.decode("utf-8", "replace")[:100_000],
            sse_id=None,
            received_at=self.now(),
        )
        self.dlq.produce(
            self.dlq_topic,
            value=dl.model_dump_json().encode(),
            headers=[
                ("reason", b"processor_validation"),
                ("source", f"{msg.topic()}/{msg.partition()}/{msg.offset()}".encode()),
            ],
        )
        self.totals.dead_letters += 1
        m.MESSAGES.labels("dead_letter").inc()
        log.warning("dead letter at %s/%s/%s", msg.topic(), msg.partition(), msg.offset())

    # ------------------------------------------------------------------ writing

    def _write_with_retry(self, events: list[EditEvent]) -> BatchResult:
        delay = 1.0
        for attempt in range(1, self.max_db_retries + 1):
            try:
                return self.store.write_batch(events, cutoff=self.now() - self.retention)
            except Exception as exc:  # psycopg errors, lost connection, ...
                m.BATCH_FAILURES.inc()
                if attempt == self.max_db_retries:
                    raise
                log.warning("batch write failed (attempt %d): %s; retrying", attempt, exc)
                self.sleep(delay)
                delay = min(delay * 2, 30)
                try:
                    self.store.conn.close()
                except Exception:
                    pass
                self.store = self.store_factory()
        raise AssertionError("unreachable")

    def process_batch(self, msgs: Sequence[Any]) -> BatchResult:
        started = self.clock()
        events = self._parse(msgs)
        result = self._write_with_retry(events) if events else BatchResult(0, 0, 0)
        # Order matters: DB committed -> dead letters delivered -> offsets committed.
        if self.dlq.flush(30) > 0:
            raise RuntimeError("dead-letter messages not confirmed; offsets not committed")
        self.consumer.commit(asynchronous=False)

        t = self.totals
        t.consumed += len(msgs)
        t.inserted += result.inserted
        t.duplicates += result.duplicates
        t.too_old += result.too_old
        t.batches += 1
        m.MESSAGES.labels("inserted").inc(result.inserted)
        m.MESSAGES.labels("duplicate").inc(result.duplicates)
        m.MESSAGES.labels("too_old").inc(result.too_old)
        m.BATCHES.inc()
        m.BATCH_SECONDS.observe(self.clock() - started)
        committed_at = self.now()
        m.LAST_COMMIT.set(committed_at.timestamp())
        for ev in result.inserted_events:
            m.E2E_LATENCY.observe(max((committed_at - ev.ts).total_seconds(), 0.0))
        return result

    # --------------------------------------------------------------- upkeep

    def update_lag(self) -> int:
        total = 0
        for tp in self.consumer.assignment():
            _low, high = self.consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            committed = self.consumer.committed([tp], timeout=5)[0].offset
            lag = high - committed if committed >= 0 else high
            m.CONSUMER_LAG.labels(str(tp.partition)).set(max(lag, 0))
            total += max(lag, 0)
        return total

    def maybe_upkeep(self) -> None:
        now = self.clock()
        if now - self._last_lag >= self.lag_every_s:
            self._last_lag = now
            try:
                self.update_lag()
            except Exception as exc:  # metrics must never stop processing
                log.debug("lag update failed: %s", exc)
        if now - self._last_prune >= self.prune_every_s:
            self._last_prune = now
            deleted = self.store.prune(self.now() - self.retention)
            m.PRUNED.inc(deleted)
            if deleted:
                log.info("pruned %d edits older than %s", deleted, self.retention)

    def run(
        self,
        *,
        max_seconds: float | None = None,
        max_idle_s: float | None = None,
        should_stop: Callable[[], bool] = lambda: False,
        stats_every_s: float = 30.0,
    ) -> Totals:
        start = last_msg = last_stats = self.clock()
        while not should_stop():
            msgs = self.consumer.consume(num_messages=self.batch_size, timeout=self.batch_timeout_s)
            now = self.clock()
            if msgs:
                self.process_batch(msgs)
                last_msg = now
            self.maybe_upkeep()
            if now - last_stats >= stats_every_s:
                log.info("stats %s", self.totals.summary())
                last_stats = now
            if max_seconds is not None and now - start >= max_seconds:
                break
            if max_idle_s is not None and now - last_msg >= max_idle_s:
                log.info("no messages for %.0fs, stopping", max_idle_s)
                break
        log.info("final stats %s", self.totals.summary())
        return self.totals


def _partition_eof() -> int:
    from confluent_kafka import KafkaError

    return KafkaError._PARTITION_EOF


def make_consumer(settings: Settings, group_id: str | None = None):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": group_id or settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wikipulse-processor", description=__doc__.splitlines()[0])
    p.add_argument("--max-seconds", type=float, help="stop after this many seconds")
    p.add_argument(
        "--max-idle-seconds", type=float, help="stop after this many seconds with no messages"
    )
    p.add_argument("--group", help="consumer group (default from WIKIPULSE_CONSUMER_GROUP)")
    p.add_argument("--no-migrate", action="store_true", help="don't apply DB migrations on start")
    p.add_argument("--no-metrics-server", action="store_true", help="don't serve /metrics")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = settings or load_settings()

    from prometheus_client import start_http_server

    from wikipulse.db import connect
    from wikipulse.db.__main__ import upgrade
    from wikipulse.producer.kafka_io import ensure_topics, make_producer

    if not args.no_migrate:
        upgrade(settings.database_url)
    ensure_topics(
        settings.kafka_bootstrap,
        settings.edits_topic,
        settings.dlq_topic,
        settings.topic_partitions,
    )
    if not args.no_metrics_server:
        start_http_server(settings.processor_metrics_port)
        log.info("metrics on http://localhost:%d/metrics", settings.processor_metrics_port)

    consumer = make_consumer(settings, args.group)
    consumer.subscribe([settings.edits_topic])
    stopping = {"flag": False}

    def _stop(signum: int, _frame: Any) -> None:
        log.info("signal %s received, finishing current batch", signum)
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    processor = StreamProcessor(
        consumer,
        lambda: PostgresStore(connect(settings.database_url, autocommit=True)),
        make_producer(settings.kafka_bootstrap, client_id="wikipulse-processor-dlq"),
        dlq_topic=settings.dlq_topic,
        retention_days=settings.retention_days,
        batch_size=settings.batch_size,
        batch_timeout_s=settings.batch_timeout_s,
    )
    try:
        processor.run(
            max_seconds=args.max_seconds,
            max_idle_s=args.max_idle_seconds,
            should_stop=lambda: stopping["flag"],
        )
    finally:
        consumer.close()  # leaves the group cleanly; offsets were already committed
    return 0


def cli() -> int:
    return main()
