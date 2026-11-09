"""Prometheus metrics for the stream processor (scraped from :9108/metrics by default)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter(
    "wikipulse_processor_messages_total", "Kafka messages consumed", ["outcome"]
)  # outcome: inserted | duplicate | too_old | dead_letter
BATCHES = Counter("wikipulse_processor_batches_total", "Batches committed")
BATCH_FAILURES = Counter(
    "wikipulse_processor_batch_failures_total", "Batch attempts that failed and were retried"
)
BATCH_SECONDS = Histogram(
    "wikipulse_processor_batch_seconds",
    "Time to write one batch to Postgres and commit offsets",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
E2E_LATENCY = Histogram(
    "wikipulse_processor_end_to_end_seconds",
    "Revision saved on Wikipedia -> row committed in Postgres",
    buckets=(0.5, 1, 2, 3, 5, 7.5, 10, 15, 30, 60, 120, 300, 900),
)
CONSUMER_LAG = Gauge(
    "wikipulse_processor_consumer_lag_messages",
    "High watermark minus committed position",
    ["partition"],
)
LAST_COMMIT = Gauge(
    "wikipulse_processor_last_commit_timestamp_seconds", "Unix time of the last successful commit"
)
PRUNED = Counter("wikipulse_processor_pruned_edits_total", "Edits deleted by retention")
