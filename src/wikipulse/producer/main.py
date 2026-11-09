"""WikiPulse producer: Wikimedia EventStreams -> Kafka.

    python -m wikipulse.producer                     # run until Ctrl+C
    python -m wikipulse.producer --max-seconds 60    # bounded run (benchmarks, hosted mode)
    python -m wikipulse.producer --since 2026-11-09T00:00:00Z --reset-checkpoint

Guarantees (see docs/DECISIONS.md):
- Valid edits go to the edits topic keyed by `<wiki>:<page_id>`.
- Malformed events go to the dead-letter topic with a reason; they never stop the stream.
- The resume checkpoint is written only after Kafka confirms every message before it.
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from wikipulse.config import Settings, load_settings
from wikipulse.producer.checkpoint import FileCheckpoint
from wikipulse.producer.normalize import Filters, Skipped, normalize
from wikipulse.producer.schemas import DeadLetter, EditEvent
from wikipulse.wikimedia.sse import SSEClient, SSEEvent

log = logging.getLogger("wikipulse.producer")


class KafkaProducerLike(Protocol):
    def produce(self, topic: str, value: Any = ..., key: Any = ..., **kwargs: Any) -> None: ...
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...


class DeliveryFailed(RuntimeError):
    """Kafka did not confirm some messages; the checkpoint was NOT advanced."""


@dataclass
class Stats:
    read: int = 0
    produced: int = 0
    dead_letters: int = 0
    skipped: Counter[str] = field(default_factory=Counter)
    started: float = field(default_factory=time.monotonic)

    def summary(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        dlq_rate = self.dead_letters / self.read if self.read else 0.0
        return (
            f"read={self.read} produced={self.produced} dead_letters={self.dead_letters} "
            f"dlq_rate={dlq_rate:.4%} skipped={dict(self.skipped)} "
            f"elapsed={elapsed:.1f}s produced_per_s={self.produced / elapsed:.2f}"
        )


class EditProducer:
    def __init__(
        self,
        producer: KafkaProducerLike,
        *,
        edits_topic: str,
        dlq_topic: str,
        filters: Filters,
        checkpoint: FileCheckpoint | None = None,
        flush_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.producer = producer
        self.edits_topic = edits_topic
        self.dlq_topic = dlq_topic
        self.filters = filters
        self.checkpoint = checkpoint
        self.flush_interval_s = flush_interval_s
        self.clock = clock
        self.stats = Stats()
        self.last_event_id: str | None = None
        self._failures: list[str] = []
        self._last_flush = clock()

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._failures.append(str(err))

    def handle(self, ev: SSEEvent) -> EditEvent | Skipped | DeadLetter:
        self.stats.read += 1
        result = normalize(ev.data, self.filters, sse_id=ev.id)
        if isinstance(result, EditEvent):
            self.producer.produce(
                self.edits_topic,
                key=result.kafka_key.encode(),
                value=result.model_dump_json().encode(),
                headers=[("schema_version", str(result.schema_version).encode())],
                on_delivery=self._on_delivery,
            )
            self.stats.produced += 1
        elif isinstance(result, DeadLetter):
            self.producer.produce(
                self.dlq_topic,
                value=result.model_dump_json().encode(),
                headers=[("reason", result.reason.encode())],
                on_delivery=self._on_delivery,
            )
            self.stats.dead_letters += 1
            log.warning("dead letter (%s): %s", result.reason, result.error[:200])
        else:
            self.stats.skipped[result.reason] += 1
        if ev.id:
            self.last_event_id = ev.id
        self.producer.poll(0)  # serve delivery callbacks
        return result

    def commit(self, timeout_s: float = 30.0) -> None:
        """Flush Kafka, then (only if everything was confirmed) save the checkpoint."""
        remaining = self.producer.flush(timeout_s)
        self._last_flush = self.clock()
        if remaining or self._failures:
            failures, self._failures = self._failures, []
            raise DeliveryFailed(
                f"{remaining} message(s) unconfirmed, {len(failures)} failed: {failures[:3]}"
            )
        if self.checkpoint:
            self.checkpoint.save(self.last_event_id)

    def run(
        self,
        events: Iterable[SSEEvent],
        *,
        max_events: int | None = None,
        max_seconds: float | None = None,
        should_stop: Callable[[], bool] = lambda: False,
        stats_every_s: float = 30.0,
    ) -> Stats:
        start = self.clock()
        last_stats = start
        try:
            for ev in events:
                self.handle(ev)
                now = self.clock()
                if now - self._last_flush >= self.flush_interval_s:
                    self.commit()
                if now - last_stats >= stats_every_s:
                    log.info("stats %s", self.stats.summary())
                    last_stats = now
                if max_events is not None and self.stats.produced >= max_events:
                    break
                if max_seconds is not None and now - start >= max_seconds:
                    break
                if should_stop():
                    break
        finally:
            self.commit()
        log.info("final stats %s", self.stats.summary())
        return self.stats


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wikipulse-producer", description=__doc__.splitlines()[0])
    p.add_argument("--max-events", type=int, help="stop after producing this many edits")
    p.add_argument("--max-seconds", type=float, help="stop after this many seconds")
    p.add_argument(
        "--since",
        help="start from this time (ISO 8601) when there is no checkpoint; "
        "Wikimedia only keeps a limited history",
    )
    p.add_argument("--reset-checkpoint", action="store_true", help="ignore the saved resume point")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = settings or load_settings()

    # Imported here so unit tests of the pure parts don't need a Kafka client.
    from wikipulse.producer.kafka_io import ensure_topics, make_producer

    ensure_topics(
        settings.kafka_bootstrap,
        settings.edits_topic,
        settings.dlq_topic,
        settings.topic_partitions,
    )
    checkpoint = FileCheckpoint(settings.checkpoint_path, settings.stream_url)
    resume_id = None if args.reset_checkpoint else checkpoint.load()
    params = {"since": args.since} if args.since and not resume_id else {}
    log.info(
        "starting: wikis=%s namespaces=%s resume=%s since=%s user_agent=%r",
        settings.wikis,
        settings.namespaces,
        bool(resume_id),
        params.get("since"),
        settings.user_agent,
    )

    source = SSEClient(
        settings.stream_url, settings.user_agent, last_event_id=resume_id, params=params
    )
    stopping = {"flag": False}

    def _stop(signum: int, _frame: Any) -> None:
        log.info("signal %s received, finishing up", signum)
        stopping["flag"] = True
        source.stop()

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    producer = EditProducer(
        make_producer(settings.kafka_bootstrap),
        edits_topic=settings.edits_topic,
        dlq_topic=settings.dlq_topic,
        filters=Filters(frozenset(settings.wikis), frozenset(settings.namespaces)),
        checkpoint=checkpoint,
        flush_interval_s=settings.flush_interval_s,
    )
    producer.run(
        source.events(),
        max_events=args.max_events,
        max_seconds=args.max_seconds,
        should_stop=lambda: stopping["flag"],
    )
    log.info("reconnects=%d stopped_at=%s", source.reconnects, datetime.now(UTC).isoformat())
    return 0


def cli() -> int:
    return main()
