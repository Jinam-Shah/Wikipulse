# Metrics

Real, measured numbers only. Each entry has the date, the environment, and the exact
command used. If a number hasn't been measured yet, the cell says "not measured".

## Environments

| Name | Description |
|---|---|
| `cloud-dev` | Linux cloud dev container used while scaffolding (16 GB RAM, Docker 29.9, Compose v5.6). |
| `laptop` | Jinam's Windows machine with Docker Desktop (fill in CPU / RAM when first measured). |

---

## Phase 0 - Local stack resource usage

| Date | Env | Container | Memory | Command |
|---|---|---|---|---|
| 2026-11-06 | cloud-dev | redpanda | 177.7 MiB | `docker stats --no-stream` after `docker compose --profile airflow up -d --wait` |
| 2026-11-06 | cloud-dev | redpanda-console | 39.4 MiB | same |
| 2026-11-06 | cloud-dev | postgres | 51.0 MiB | same |
| 2026-11-06 | cloud-dev | airflow-scheduler | 393.7 MiB | same |
| 2026-11-06 | cloud-dev | airflow-apiserver | 237.1 MiB | same |
| 2026-11-06 | cloud-dev | airflow-dag-processor | not captured (docker stats returned 0B) | same |

Single snapshot at idle, right after start-up; not a load measurement.

## Phase 1 - Producer (EventStreams -> Redpanda)

All runs: env `cloud-dev` (4 vCPU, 15 GB RAM), producer run on the host with Python 3.13
against Redpanda in Docker, filters `WIKIPULSE_WIKIS=enwiki`, `WIKIPULSE_NAMESPACES=0`,
stream `mediawiki.page_change.v1`. The latency analysis script reads every message back
from `wikipulse.edits.v1` and compares timestamps inside each message.

| Metric | Value | Date (UTC) | Command |
|---|---|---|---|
| Stream volume, all wikis | 3,585 events in 180.4 s (19.9 events/s) | 2026-11-09 02:49-02:52 | `python -m wikipulse.producer --max-seconds 180` |
| enwiki article edits kept | 355 in 180.4 s (1.97/s) | same run | same |
| Dead-letter rate | 0 of 3,585 (0.00 %) | same run | same |
| Latency, Wikimedia emits event -> in Kafka (live) | p50 0.16 s, p95 0.46 s, max 0.71 s (n = 355) | same run | same, then read back the topic: `ingested_at - event_dt` per message |
| Latency, revision saved on Wikipedia -> in Kafka (live) | p50 2.48 s, p95 7.03 s (n = 355) | same run | `ingested_at - ts` per message |
| Catch-up throughput (backfill, 30 min of history) | 41,816 events read and validated in 60.1 s (696 events/s); 3,602 enwiki edits produced (59.97/s); 0 dead letters | 2026-11-09 02:52-02:53 | `python -m wikipulse.producer --since <now-30min> --max-seconds 60` |
| Crash recovery | `kill -9` after 45 s, 20 s down, restart: resumed from checkpoint; 186 messages, 1 duplicate, largest gap between consecutive events 2.9 s (no loss) | 2026-11-09 02:47-02:49 | `python -m wikipulse.producer`, `kill -9 <pid>`, then `python -m wikipulse.producer --max-seconds 40` |
| Anonymous share (temp accounts + IPs) | 5.4 % (19 of 355) | live run above | from the `is_anon` field |
| Bot share | 45.6 % (162 of 355) | live run above | from the `is_bot` field |

Notes:
- The catch-up figure is limited by how fast Wikimedia streams history to one connection,
  not by our CPU; it is a single 60-second sample.
- During the crash-recovery catch-up, p95 `event_dt -> Kafka` latency was 18.6 s, because
  backlog events are read late by design.

## Phase 2 - Stream processor (Redpanda -> Postgres)

Env `cloud-dev` (4 vCPU, 15 GB RAM). Postgres 17 and Redpanda in Docker on the same machine.

### Live pipeline, all in containers

`docker compose --profile pipeline up -d` (producer + processor + Prometheus), running
2026-11-09 03:23:34-03:30:04 UTC (6.5 min). Values read from Prometheus with the queries shown.
Latency quantiles are Prometheus histogram estimates (interpolated between buckets).

| Metric | Value | Query / command |
|---|---|---|
| Edits processed | 602 inserted, 0 duplicates, 0 too old | `wikipulse_processor_messages_total` |
| End-to-end latency, revision saved on Wikipedia -> row committed in Postgres | p50 2.93 s, p95 7.44 s, p99 12.98 s; mean 3.47 s (2088.9 s / 602) | `histogram_quantile(q, sum by (le) (wikipulse_processor_end_to_end_seconds_bucket))` |
| Batch write + offset commit | p50 0.005 s, p95 0.010 s | `histogram_quantile(q, sum by (le) (wikipulse_processor_batch_seconds_bucket))` |
| Max consumer lag (live) | 3 messages | `max_over_time(sum(wikipulse_processor_consumer_lag_messages)[7m:5s])` |
| Dead-letter rate (producer + processor) | 0 | `wikipulse_producer_events_total`, `wikipulse_processor_messages_total` |
| Reverts flagged | 9 of 602 (all by edit summary) | `select count(*) ... where revert_kind is not null` |

Note: most of the end-to-end latency is the delay before Wikimedia publishes the event
(Phase 1 measured p50 0.16 s from Wikimedia's event time into Kafka, but 2.48 s from
the revision's save time).

### Throughput and lag under load (backlog drain)

1. Backlog: `python -m wikipulse.producer --since 2026-11-09T01:00:19Z --max-seconds 150`
   into topic `bench.edits.v1` -> **16,545 enwiki edits** (read and validated 188,349
   stream events in 150 s = 1,255 events/s; 0 dead letters; 6 Wikimedia canary events
   skipped).
2. Drain: a fresh processor (new consumer group, fresh database with migrations, batch
   size 500) consumed the whole backlog. Timed from the first to the last batch
   (timing script, 3 runs, each on a fresh database):

| Run | Rows | Seconds | Edits/s |
|---|---|---|---|
| 1 | 16,545 | 4.49 | 3,682 |
| 2 | 16,545 | 4.38 | 3,777 |
| 3 | 16,545 | 4.12 | 4,014 |

- **Consumer lag under load:** started at 16,545 messages and reached 0 in about 4.4 s.
  Live enwiki traffic is about 2 edits/s, so this is roughly 1,800x headroom on this machine.
- The same backlog via the CLI (`python -m wikipulse.processor --group bench-1
  --max-idle-seconds 10`) reported 1,124.5 consumed/s, but that figure includes the
  10 s idle wait before exit, so the table above is the accurate rate.
- **Result of that backlog run:** 16,545 edits -> 14,943 page windows, 153 wiki-minute
  windows; 466 reverts flagged (52 by content hash, 414 by edit summary = 2.8 %).

### Correctness (replay tests)

`pytest -m integration` (2026-11-09, cloud-dev): 10 passed, including full replay with
a new consumer group (0 new rows, identical metrics), the same events shuffled into
batches of 2 (3 random seeds, identical metrics), and duplicates inside a batch.

## Data platform (Phases 3-4)

| Metric | Value | Date | Command |
|---|---|---|---|
| dbt models / tests | not measured | | |

## Breaking News Radar (Phase 5)

| Metric | Value | Date | Command |
|---|---|---|---|
| Trends detected per week | not measured | | |
| Share confirmed by pageviews | not measured | | |
| Lead-time findings | not measured | | |

## Vandalism model (Phase 6)

| Model | Precision | Recall | F1 | PR-AUC | Test window | Date |
|---|---|---|---|---|---|---|
| Logistic regression | not measured | | | | | |
| LightGBM | not measured | | | | | |
| Lift Wing revert-risk (same edits) | not measured | | | | | |

## Tests

| Suite | Count | Date | Command |
|---|---|---|---|
| pytest (Phase 0) | 2 passed | 2026-11-06 | `pytest` (cloud-dev) |
| pytest unit (Phase 1) | 43 passed | 2026-11-09 | `pytest` (cloud-dev) |
| pytest integration (Phase 1) | 1 passed | 2026-11-09 | `pytest -m integration` with the compose stack up (cloud-dev) |
| pytest unit (Phase 2) | 71 passed | 2026-11-09 | `pytest` (cloud-dev) |
| pytest integration (Phase 2) | 10 passed | 2026-11-09 | `pytest -m integration` with the compose stack up (cloud-dev) |
| Playwright | not measured | | |
