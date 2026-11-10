"""Central place for settings read from the environment (.env locally, secrets in CI/hosting)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from wikipulse import PROJECT_URL, __version__

DEFAULT_CONTACT = "https://github.com/jinam-shah/wikipulse/issues"
DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1"


def _csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    database_url: str
    contact: str
    # --- producer (Phase 1) ---
    stream_url: str = DEFAULT_STREAM_URL
    wikis: tuple[str, ...] = ("enwiki",)
    namespaces: tuple[int, ...] = (0,)
    edits_topic: str = "wikipulse.edits.v1"
    dlq_topic: str = "wikipulse.edits.dlq.v1"
    topic_partitions: int = 3
    checkpoint_path: Path = field(default_factory=lambda: Path("data/checkpoints/producer.json"))
    flush_interval_s: float = 1.0
    producer_metrics_port: int = 9107
    # --- stream processor (Phase 2) ---
    consumer_group: str = "wikipulse-processor"
    batch_size: int = 500
    batch_timeout_s: float = 1.0
    retention_days: int = 7
    processor_metrics_port: int = 9108
    radar_metrics_port: int = 9109
    scorer_metrics_port: int = 9110
    # --- lakehouse (Phase 3) ---
    lake_dir: Path = field(default_factory=lambda: Path("data/lake"))

    @property
    def user_agent(self) -> str:
        """User-Agent sent to every Wikimedia endpoint (see docs/DATA_SOURCES.md)."""
        return f"WikiPulse/{__version__} ({PROJECT_URL}; {self.contact})"


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        kafka_bootstrap=env("WIKIPULSE_KAFKA_BOOTSTRAP", "localhost:19092"),
        database_url=env(
            "WIKIPULSE_DATABASE_URL",
            "postgresql://wikipulse:wikipulse@localhost:5432/wikipulse",
        ),
        contact=env("WIKIPULSE_CONTACT", DEFAULT_CONTACT),
        stream_url=env("WIKIPULSE_STREAM_URL", DEFAULT_STREAM_URL),
        wikis=_csv(env("WIKIPULSE_WIKIS", "enwiki")),
        namespaces=tuple(int(n) for n in _csv(env("WIKIPULSE_NAMESPACES", "0"))),
        edits_topic=env("WIKIPULSE_EDITS_TOPIC", "wikipulse.edits.v1"),
        dlq_topic=env("WIKIPULSE_DLQ_TOPIC", "wikipulse.edits.dlq.v1"),
        topic_partitions=int(env("WIKIPULSE_TOPIC_PARTITIONS", "3")),
        checkpoint_path=Path(env("WIKIPULSE_CHECKPOINT_PATH", "data/checkpoints/producer.json")),
        flush_interval_s=float(env("WIKIPULSE_FLUSH_INTERVAL_S", "1.0")),
        producer_metrics_port=int(env("WIKIPULSE_PRODUCER_METRICS_PORT", "9107")),
        consumer_group=env("WIKIPULSE_CONSUMER_GROUP", "wikipulse-processor"),
        batch_size=int(env("WIKIPULSE_BATCH_SIZE", "500")),
        batch_timeout_s=float(env("WIKIPULSE_BATCH_TIMEOUT_S", "1.0")),
        retention_days=int(env("WIKIPULSE_RETENTION_DAYS", "7")),
        processor_metrics_port=int(env("WIKIPULSE_PROCESSOR_METRICS_PORT", "9108")),
        radar_metrics_port=int(env("WIKIPULSE_RADAR_METRICS_PORT", "9109")),
        scorer_metrics_port=int(env("WIKIPULSE_SCORER_METRICS_PORT", "9110")),
        lake_dir=Path(env("WIKIPULSE_LAKE_DIR", "data/lake")),
    )
