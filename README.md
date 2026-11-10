# WikiPulse

**Real-time Wikipedia intelligence.** WikiPulse watches every edit to Wikipedia as it happens,
spots breaking news from sudden edit spikes, explains in plain English what changed and why,
detects edit wars and likely vandalism, and alerts users when a topic they follow starts moving.

> **Status:** Phase 6 - every edit gets a **Vandalism Risk Score** (LightGBM, evaluated on
> newer data than it was trained on and compared with Wikimedia's own Lift Wing model:
> [results](#vandalism-risk-score-phase-6)). Phase 5's **Breaking News Radar** flags pages whose edit activity
> suddenly spikes among several editors, confirms them against Wikipedia page views, and
> has a measured lead time against real news events ([docs/LEAD_TIME.md](docs/LEAD_TIME.md)).
> Underneath: Kafka -> Postgres 5-minute windows -> hourly Airflow lakehouse (Iceberg + dbt)
> with data-quality checks.
> Features arrive phase by phase - see [the build plan](#build-plan).

## Architecture

```mermaid
flowchart LR
  ES[Wikimedia EventStreams] --> PR[Producer: Python]
  PR --> K[(Kafka topics on Redpanda)]
  K --> SP[Stream processor: Python]
  SP --> DLQ[(Dead-letter topic)]
  SP --> SCORE[Live vandalism scoring]
  SP --> PG[(PostgreSQL / Neon)]
  SP --> RAW[Hourly Parquet files]
  RAW --> ICE[Iceberg bronze/silver/gold]
  RAW --> DBX[Databricks Free Edition: Delta Lake]
  ICE --> DBT[dbt + DuckDB marts]
  AF[Airflow] --> DBT
  AF --> TRAIN[Weekly retraining + MLflow]
  PG --> API[FastAPI]
  API --> WM[Wikimedia REST / Action / Pageviews / Lift Wing]
  API --> GEM[Gemini free tier]
  WEB[Next.js on Vercel] --> API
  GHA[GitHub Actions schedule] --> API
```

## Where things run

| Mode | What runs | Where | Status |
|---|---|---|---|
| **Full streaming mode** | Redpanda, producer, stream processor, Airflow, Prometheus, Grafana | Your machine, `docker compose` | Everything except Grafana works |
| **Hosted mode** | GitHub Actions every 15 min samples the stream, processes it with the same code, writes to Neon; app on Vercel, API on Render | Free cloud tiers | Not built yet (Phase 13) |

Hosted mode samples a few minutes of the stream per run, so its trend detection is less
complete than full streaming mode. Today, **nothing runs in the cloud except CI.**

## Run it locally

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (on Windows, WSL 2 backend). Give it at least 4 GB RAM if you'll start Airflow.
- Python 3.12
- Git

### Start the stack

Windows PowerShell:

```powershell
cd C:\Projects\wikipulse
Copy-Item .env.example .env
docker compose up -d --wait
python scripts\smoke_test.py
```

macOS / Linux:

```bash
cp .env.example .env
docker compose up -d --wait
python scripts/smoke_test.py
```

Add Airflow (heavier, optional until Phase 4):

```bash
docker compose --profile airflow up -d --wait
python scripts/smoke_test.py --airflow
```

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8088 |
| Kafka API (from your machine) | `localhost:19092` |
| Postgres | `postgresql://wikipulse:wikipulse@localhost:5432/wikipulse` |
| Airflow UI (profile `airflow`) | http://localhost:8081 |
| Prometheus (profile `pipeline`) | http://localhost:9090 |

Stop everything: `docker compose --profile airflow down` (add `-v` to delete the data volumes).

### Stream live Wikipedia edits into Kafka (Phase 1)

Option A - run the producer on your machine (easiest to watch and stop with Ctrl+C):

```powershell
.venv\Scripts\Activate.ps1
python -m wikipulse.producer --max-seconds 120
```

Option B - run it as a container next to Redpanda (keeps running in the background):

```bash
docker compose --profile pipeline up -d --build
docker logs -f wikipulse-producer
```

Then open Redpanda Console (http://localhost:8088) -> Topics -> `wikipulse.edits.v1`,
or print a few messages with
`docker exec wikipulse-redpanda rpk topic consume wikipulse.edits.v1 -n 3`.

Useful flags: `--max-events N`, `--max-seconds S`, `--since 2026-11-09T00:00:00Z`
(start from a past time when there is no checkpoint), `--reset-checkpoint`.
The resume point is stored in `data/checkpoints/producer.json` (host) or the
`producer-state` volume (container).

| Topic | Contents |
|---|---|
| `wikipulse.edits.v1` | One validated edit per message, key `<wiki>:<page_id>` (schema: `EditEvent` in `src/wikipulse/producer/schemas.py`) |
| `wikipulse.edits.dlq.v1` | Events that failed validation, with the reason and the raw payload |

### Turn the stream into activity windows in Postgres (Phase 2)

Run the whole pipeline in containers (producer + processor + Prometheus):

```bash
docker compose --profile pipeline up -d --build
docker logs -f wikipulse-processor
```

Or run the processor directly on your machine (in a second terminal, next to the producer):

```powershell
python -m wikipulse.processor
```

It applies the database migrations on start, then writes:

| Table | One row per |
|---|---|
| `edits` | edit (last 7 days; `WIKIPULSE_RETENTION_DAYS`) |
| `page_metrics_5min` | page and 5-minute window: edits, distinct editors, bot/anon edits, reverts, bytes changed |
| `wiki_metrics_1min` | wiki and minute: edits, new pages, bot/anon edits, reverts, distinct pages/editors |

The most active pages right now:

```bash
docker exec -it wikipulse-postgres psql -U wikipulse -c "select page_title, window_start, edits, distinct_editors, reverts from page_metrics_5min order by window_start desc, edits desc limit 10"
```

Metrics: Prometheus at http://localhost:9090 (try `wikipulse_processor_consumer_lag_messages`
or `rate(wikipulse_processor_messages_total[1m])`). Raw metrics: http://localhost:9107/metrics
(producer) and http://localhost:9108/metrics (processor).

Database migrations by hand: `python -m wikipulse.db upgrade` / `current` / `downgrade -1`.

### Build the lakehouse and dbt marts (Phase 3)

> **Windows:** if `import pyarrow.parquet` fails with "An Application Control policy has
> blocked this file", run these commands in the Linux dev container instead (D-023): put
> `docker compose run --rm dev` in front, e.g.
> `docker compose run --rm dev python -m wikipulse.lake run --hours 3`.
> First time: `docker compose --profile tools build dev`.

Once Postgres has edits (from the producer + processor, or synthetic data):

```powershell
python -m wikipulse.devtools.sample --hours 6   # optional: synthetic data (wiki "samplewiki")
python -m wikipulse.lake run --hours 3          # last 3 finished hours -> Parquet -> Iceberg
python -m wikipulse.lake status                 # tables, row counts, snapshots
python -m wikipulse.lake dbt build              # marts + 40 data tests
python -m wikipulse.lake dbt docs generate --static   # docs site: dbt/target/static_index.html
```

| Layer | Where | Contents |
|---|---|---|
| Raw | `data/lake/raw/edits/date=.../hour=.../edits.parquet` | one file per finished hour, exported from Postgres |
| Bronze (Iceberg) | `bronze.edits` | raw rows + load metadata |
| Silver (Iceberg) | `silver.edits` | deduplicated, typed, derived columns (editor type, account age, bytes changed, ...) |
| Gold (Iceberg) | `gold.page_hourly`, `gold.wiki_hourly` | hourly aggregates |
| Marts (dbt + DuckDB) | `data/lake/analytics.duckdb`, schema `marts` | `mart_wiki_daily`, `mart_top_pages_daily`, `mart_editor_types_daily`, `mart_hourly_profile`, `dim_pages`, `fct_edits` |

Re-running an hour replaces it (no duplicates); every load is an Iceberg snapshot you can
time-travel to. **dbt docs:** https://jinam-shah.github.io/Wikipulse/ (built on synthetic
sample data by GitHub Actions; see "Known limitations").

### Breaking News Radar (Phase 5)

The radar runs with the pipeline (`docker compose --profile pipeline up -d --build`) or
on its own: `python -m wikipulse.radar` (Windows: `docker compose run --rm dev python -m wikipulse.radar`).
Every 5-minute window it compares each page's edits with that page's normal level
(6-hour EWMA) and opens a **trend** when the jump is large (z >= 4) **and** at least 3
different people made 5+ edits (mostly not bots). Trends end after 15 quiet minutes.
Page views confirm them a few hours later, once Wikimedia publishes hourly views.

```bash
docker exec -it wikipulse-postgres psql -U wikipulse -c "select started_at, page_title, peak_edits, distinct_editors, round(peak_zscore::numeric,1) as z, views_status from trends order by started_at desc limit 15"
```

Replay history (EventStreams keeps about 10 days) and re-run the radar on it:

```bash
python -m wikipulse.producer --since 2026-11-08T18:00:00Z --reset-checkpoint --max-seconds 900
python -m wikipulse.processor --max-idle-seconds 30
python -m wikipulse.radar --rebuild --once --no-pageviews
```

### Vandalism Risk Score (Phase 6)

Each edit gets `risk_score`: the model's estimated probability that the edit will be
reverted within 6 hours. **It's an automated estimate, not an accusation.**

```bash
# Train (time-based split, Lift Wing comparison, MLflow). Windows: prefix with
# "docker compose run --rm dev" (scikit-learn/LightGBM need the Linux container there).
python -m wikipulse.ml.train
# Score new edits continuously (also runs in the pipeline as the `scorer` container)
python -m wikipulse.ml.score
# MLflow UI on http://localhost:5000
docker compose --profile tools up -d mlflow
```

Lift Wing scores for the comparison come from Wikimedia's revert-risk stream
(`liftwing` container, or `python -m wikipulse.ml.liftwing_stream`). Results are in
[METRICS.md](METRICS.md#phase-6---vandalism-risk-score). How labels and features are
built, and what they can't capture: D-033 to D-037 in [docs/DECISIONS.md](docs/DECISIONS.md).

High-risk edits right now:

```bash
docker exec -it wikipulse-postgres psql -U wikipulse -c "select e.ts, e.page_title, round(e.risk_score::numeric,2) risk, e.comment from edits e, model_runs m where m.is_current and e.risk_score >= m.threshold order by e.ts desc limit 10"
```

### Schedule it with Airflow (Phase 4)

```bash
docker compose --profile airflow up -d --build --wait
```

Open http://localhost:8081, then **unpause** the DAGs (toggle on the left):

| DAG | Schedule (UTC) | Tasks |
|---|---|---|
| `wikipulse_hourly_lake` | every hour | `export_and_build_lake` (last 3 hours) -> `dbt_build` -> `dbt_source_freshness` + `data_quality_checks` |
| `wikipulse_weekly_retrain` | Sundays 03:17 | `snapshot_training_data` (7 days of silver) -> `train_model` (vandalism model; skipped if there isn't enough data yet) |

Run one immediately instead of waiting: **Trigger** in the UI, or
`docker compose exec airflow-scheduler airflow dags test wikipulse_hourly_lake`.

Data-quality results (stream freshness, hourly volume, lake vs Postgres, revert rate):

```bash
docker exec -it wikipulse-postgres psql -U wikipulse -c "select run_at, target_hour, check_name, wiki, status, message from data_quality_checks order by id desc limit 10"
```

Airflow and the dev container share the same lake (Docker volume `lake-data`), so
`docker compose run --rm dev python -m wikipulse.lake status` shows what Airflow built.
Code edits in `src/` and `dbt/` are used by the next task run; rebuild the image
(`--build`) only when `pyproject.toml` changes.

### Python development

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1        # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest                      # unit tests
pytest -m integration       # needs `docker compose up -d` first
```

Or the same inside the Linux dev container (works even where Windows blocks native DLLs):

```bash
docker compose --profile tools build dev
docker compose run --rm dev pytest
docker compose run --rm dev pytest -m integration
```

## Project docs

- [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) - every Wikimedia endpoint, its limits, and how we follow the rules
- [`docs/DECISIONS.md`](docs/DECISIONS.md) - design decisions and why
- [`METRICS.md`](METRICS.md) - measured numbers only, with date, environment and command
- [`docs/LEAD_TIME.md`](docs/LEAD_TIME.md) - how early the radar flags real news events
- [dbt docs](https://jinam-shah.github.io/Wikipulse/) - models, columns, tests and lineage

## Build plan

| Phase | Work | Status |
|---|---|---|
| 0 | Repo setup, docker-compose, CI, docs | Done |
| 1 | Producer: validation, dead-letter topic, reconnect/resume | Done |
| 2 | Stream processor: windowed page metrics, idempotent upserts | Done |
| 3 | Parquet, Iceberg, dbt marts | Done |
| 4 | Airflow DAGs | Done |
| 5 | Breaking News Radar + pageview confirmation | Done |
| 6 | Vandalism model vs Lift Wing | Done |
| 7 | Databricks / Delta Lake notebook | |
| 8 | Edit War Detector, What Changed | |
| 9 | AI explanations, Story Clusters | |
| 10 | FastAPI backend + public API | |
| 11 | Next.js frontend | |
| 12 | Alerts and Daily Pulse | |
| 13 | Hosted mode, deployment, final README | |

## Wikimedia attribution and responsible use

- Data comes from the Wikimedia Foundation's public APIs and EventStreams. Wikipedia content is
  licensed [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/); WikiPulse links back
  to the source article or diff wherever it shows text derived from it.
- WikiPulse identifies itself with a descriptive User-Agent and stays within Wikimedia's
  [rate limits](https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits).
- Vandalism scores are automated probability estimates, not accusations. Edit war views describe
  edit patterns on pages, not people.
- WikiPulse is an independent project and is not affiliated with or endorsed by the Wikimedia Foundation.

## Known limitations

- Revert detection and the model's labels use exact content-hash reverts (D-015, D-034):
  partial or manual rewrites aren't counted, and good-faith edits reverted in content
  disputes count as "reverted". The model has no diff-text features (D-035).
- The producer uses `mediawiki.page_change.v1` rather than `recentchange`
  (see [D-007](docs/DECISIONS.md)).
- The published dbt docs are built on synthetic data in CI, because GitHub Actions has no
  access to your local stream; the models and tests are identical.
- Data-quality failures show in Airflow and the `data_quality_checks` table; email/push
  alerts arrive in Phase 12.
- If the producer is down for longer than Wikimedia's stream history, the missed edits
  can't be recovered from EventStreams.

## License

Code: MIT (see [LICENSE](LICENSE)). Wikipedia content shown by the app: CC BY-SA 4.0.
