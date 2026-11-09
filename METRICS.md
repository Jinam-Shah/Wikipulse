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

## Streaming (Phase 2)

| Metric | Value | Date | Env | Command |
|---|---|---|---|---|
| Stream processor events/second | not measured | | | |
| End-to-end latency (Wikipedia -> DB) | not measured | | | |
| Consumer lag under load | not measured | | | |

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
| Playwright | not measured | | |
