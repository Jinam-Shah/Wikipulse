# WikiPulse

**Real-time Wikipedia intelligence.** WikiPulse watches every edit to Wikipedia as it happens,
spots breaking news from sudden edit spikes, explains in plain English what changed and why,
detects edit wars and likely vandalism, and alerts users when a topic they follow starts moving.

> **Status:** Phase 2 - live Wikipedia edits flow into Kafka and on into Postgres as
> per-page 5-minute and per-wiki 1-minute activity windows. Replays never double-count,
> and Prometheus tracks throughput, lag, latency and the dead-letter rate.
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

## Project docs

- [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) - every Wikimedia endpoint, its limits, and how we follow the rules
- [`docs/DECISIONS.md`](docs/DECISIONS.md) - design decisions and why
- [`METRICS.md`](METRICS.md) - measured numbers only, with date, environment and command

## Build plan

| Phase | Work | Status |
|---|---|---|
| 0 | Repo setup, docker-compose, CI, docs | Done |
| 1 | Producer: validation, dead-letter topic, reconnect/resume | Done |
| 2 | Stream processor: windowed page metrics, idempotent upserts | Done |
| 3 | Parquet, Iceberg, dbt marts | |
| 4 | Airflow DAGs | |
| 5 | Breaking News Radar + pageview confirmation | |
| 6 | Vandalism model vs Lift Wing | |
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

- Revert detection uses content hashes and edit summaries only (D-015); Wikimedia's own
  revert tags are added in Phase 6.
- The producer uses `mediawiki.page_change.v1` rather than `recentchange`
  (see [D-007](docs/DECISIONS.md)).
- If the producer is down for longer than Wikimedia's stream history, the missed edits
  can't be recovered from EventStreams.

## License

Code: MIT (see [LICENSE](LICENSE)). Wikipedia content shown by the app: CC BY-SA 4.0.
