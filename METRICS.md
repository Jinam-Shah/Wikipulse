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

## Streaming (Phases 1-2)

| Metric | Value | Date | Env | Command |
|---|---|---|---|---|
| Events/second processed | not measured | | | |
| End-to-end latency (Wikipedia -> DB) | not measured | | | |
| Consumer lag under load | not measured | | | |
| Dead-letter rate | not measured | | | |

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
| pytest | 2 passed | 2026-11-06 | `pytest` (cloud-dev) |
| Playwright | not measured | | |
