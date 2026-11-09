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

## Phase 3 - Lakehouse (Parquet -> Iceberg -> dbt)

Env `cloud-dev`. Input: **real enwiki article edits** for 2026-11-09 03:00-06:00 UTC, loaded by
`python -m wikipulse.producer --since 2026-11-09T03:00:00Z --max-seconds 240` and
`python -m wikipulse.processor --max-idle-seconds 8` (16,458 edits into Postgres).

| Metric | Value | Command |
|---|---|---|
| Rows per hour (03, 04, 05 UTC) | 5,226 / 5,233 / 4,343 = 14,802 | `python -m wikipulse.lake run --start 2026-11-09T03:00Z --end 2026-11-09T06:00Z` |
| Export + bronze/silver/gold, 3 hours, fresh lake | 2.42 s wall | same, under `time` |
| Same 3 hours rebuilt (idempotent) | 2.30 s wall; row counts unchanged (bronze/silver 14,802, gold.page_hourly 11,996, gold.wiki_hourly 3) | same command again; `python -m wikipulse.lake status` |
| `dbt build` | 9 models (6 tables, 3 views) + 40 data tests, all pass; 2.49 s (dbt) / 5.58 s wall | `python -m wikipulse.lake dbt build` |
| Source freshness | 3 of 3 sources pass | `python -m wikipulse.lake dbt source freshness` |
| On-disk size | raw Parquet 1.7 MB; Iceberg warehouse 7.2 MB (two loads, old snapshots not yet expired); DuckDB marts 5.1 MB | `du -sh data/lake/*` |

What the marts showed for those 3 real hours (`marts.mart_wiki_daily`, `marts.mart_editor_types_daily`):

| Fact | Value |
|---|---|
| Edits / new pages / distinct pages / distinct editors | 14,802 / 224 / 11,629 / 2,412 |
| Bot share / anonymous share | 38.2 % / 8.1 % |
| Share of edits that *are* reverts | 2.76 % (registered 4.61 %, anonymous 1.67 %, bots 0.39 %; see D-021) |
| Top 5 pages by edits | each had 1-3 distinct editors in their busiest hour, so high edit counts alone don't signal news (this is why trends require several distinct editors) |

Correctness: `pytest -m integration tests/integration/test_lake.py`: 6 passed (export matches
Postgres; idempotent rebuild; late event updates its hour; time travel; **gold totals equal
the processor's `page_metrics_5min` totals hour by hour**; `dbt build` passes).

## Phase 4 - Orchestration (Airflow 3.4, LocalExecutor)

Env `cloud-dev`, `docker compose --profile airflow up -d`, image `wikipulse-airflow:dev`
(Dockerfile.airflow), Postgres holding 8 hours of synthetic `samplewiki` edits
(`python -m wikipulse.devtools.sample --hours 8`).

| Metric | Value | Date (UTC) | Command |
|---|---|---|---|
| DAGs loaded / import errors | 3 / 0 | 2026-11-09 17:59 | `airflow dags list`, `airflow dags list-import-errors` |
| **Scheduled run started by the scheduler** | `scheduled__2026-11-09T18:00:00+00:00`, state **success**, 30.9 s (18:00:39.8 -> 18:01:10.7); next run 19:00 | 2026-11-09 18:00 | `airflow dags unpause wikipulse_hourly_lake`, then `airflow dags list-runs wikipulse_hourly_lake` |
| Manual end-to-end run of `wikipulse_hourly_lake` | success, 18.4 s (4 tasks; dbt 49/49 pass; 5/5 quality checks pass) | 2026-11-09 17:59 | `airflow dags test wikipulse_hourly_lake` |
| `wikipulse_weekly_retrain` | success: snapshot 900 rows (Iceberg snapshot 3329344356418085965); `train_model` skipped (Phase 6) | 2026-11-09 18:00 | `airflow dags test wikipulse_weekly_retrain` |
| Shared lake, dev container <-> Airflow, both as uid 50000 | Airflow build -> dev read + rebuild 4 hours -> Airflow build again: all succeed | 2026-11-09 18:05 | see D-024, D-028 |
| Memory at idle | scheduler 523 MiB, api-server 296 MiB, dag-processor 194 MiB (Airflow total about 1.0 GiB) | 2026-11-09 18:06 | `docker stats --no-stream` |
| Airflow image size | 4.22 GB (Airflow base + WikiPulse venv) | 2026-11-09 | `docker images wikipulse-airflow:dev` |

Not yet measured: a week of unattended scheduled runs (success rate, duration trend) on the laptop.

## Phase 5 - Breaking News Radar

Env `cloud-dev`. Input: 26 h of **real enwiki article edits** replayed from EventStreams
(2026-11-08 18:00 UTC to 2026-11-09 20:15 UTC), `python -m wikipulse.producer --since 2026-11-08T18:00:00Z`
+ `python -m wikipulse.processor` -> **132,751 edits**, 0 dead letters, 0 duplicates.

| Metric | Value | Command |
|---|---|---|
| Replay throughput (producer, history) | 1,502,781 stream events read in 600 s (2,505/s) while catching up | producer log |
| Radar: 26 h of windows (about 315) | 7.7 s wall | `time python -m wikipulse.radar --rebuild --once --no-pageviews` |
| Trends found, final rules (D-029 + D-032) | **8 in 26 h**; 6 news, 2 vandalism false positives (manual review) | `select ... from trends` |
| Trends found, before the revert gate | 20 in 26 h; 12 started in revert-heavy windows | same, before D-032 |
| Lead time, event time known | Russell Brand sentencing **+4 to +8 min**; NASCAR result **12 min before** first results article | docs/LEAD_TIME.md |
| Lead time vs first news report (death) | Dževad Prekazi **+95 min** (Wikipedia itself updated after 97 min) | docs/LEAD_TIME.md |
| Share confirmed by page views | **not measured**: Pageviews API returned 429 from the cloud IP | run `python -m wikipulse.radar --once` from a home connection |
| Pageview publication delay | hourly dump for 16:00-17:00 UTC published 18:51 UTC (about 2 h after the hour) | `dumps.wikimedia.org/other/pageviews/2026/2026-11/` listing |
| EventStreams history available | >= 10 days (`since=` 2026-10-30 returned events from then) | `curl ...?since=` |

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
| pytest unit (Phase 3) | 78 passed | 2026-11-09 | `pytest` (cloud-dev) |
| pytest integration (Phase 3) | 16 passed | 2026-11-09 | `pytest -m integration` (cloud-dev) |
| dbt models / data tests (Phase 3) | 9 / 40 (+3 source freshness checks) | 2026-11-09 | `python -m wikipulse.lake dbt build` |
| pytest unit (Phase 4) | 91 passed | 2026-11-09 | `pytest` (cloud-dev) |
| pytest integration (Phase 4) | 20 passed | 2026-11-09 | `pytest -m integration` (cloud-dev) |
| pytest unit (Phase 5) | 118 passed | 2026-11-09 | `pytest` (cloud-dev) |
| pytest integration (Phase 5) | 23 passed | 2026-11-09 | `pytest -m integration` (cloud-dev) |
| Playwright | not measured | | |
