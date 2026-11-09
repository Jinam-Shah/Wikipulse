# WikiPulse

**Real-time Wikipedia intelligence.** WikiPulse watches every edit to Wikipedia as it happens,
spots breaking news from sudden edit spikes, explains in plain English what changed and why,
detects edit wars and likely vandalism, and alerts users when a topic they follow starts moving.

> **Status:** Phase 0 - project skeleton. The local stack (Redpanda, Redpanda Console,
> Postgres, Airflow) starts and passes a smoke test; CI runs lint, tests and a compose smoke test.
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
| **Full streaming mode** | Redpanda, producer, stream processor, Airflow, Prometheus, Grafana | Your machine, `docker compose` | Infrastructure only (Phase 0) |
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

Stop everything: `docker compose --profile airflow down` (add `-v` to delete the data volumes).

### Python development

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1        # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest
```

## Project docs

- [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) - every Wikimedia endpoint, its limits, and how we follow the rules
- [`docs/DECISIONS.md`](docs/DECISIONS.md) - design decisions and why
- [`METRICS.md`](METRICS.md) - measured numbers only, with date, environment and command

## Build plan

| Phase | Work | Status |
|---|---|---|
| 0 | Repo setup, docker-compose, CI, docs | Done |
| 1 | Producer: validation, dead-letter topic, reconnect/resume | |
| 2 | Stream processor: windowed page metrics, idempotent upserts | |
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

- Phase 0: no application features yet.

## License

Code: MIT (see [LICENSE](LICENSE)). Wikipedia content shown by the app: CC BY-SA 4.0.
