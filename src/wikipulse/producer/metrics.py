"""Prometheus metrics for the producer (scraped from :9107/metrics by default)."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS = Counter(
    "wikipulse_producer_events_total", "Stream events read, by outcome", ["outcome"]
)  # outcome: produced | dead_letter | skipped
RECONNECTS = Gauge("wikipulse_producer_reconnects", "Stream reconnects since start")
INGEST_LATENCY = Histogram(
    "wikipulse_producer_ingest_seconds",
    "Wikimedia event time -> read by the producer",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)
LAST_CHECKPOINT = Gauge(
    "wikipulse_producer_last_checkpoint_timestamp_seconds", "Unix time of the last checkpoint"
)
