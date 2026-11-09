# Decisions

Each significant choice: the problem, the options, what we chose, and why.
Newest at the bottom. Format: `D-<number> (<date>, Phase <n>)`.

---

## D-001 (2026-11-06, Phase 0) - One Postgres container for app data and Airflow metadata

- **Problem:** Airflow needs a metadata database, and the app needs Postgres too.
- **Options:** (a) two Postgres containers; (b) one container, two databases.
- **Choice:** (b). The container creates `wikipulse` by default and an init script
  (`infra/postgres/init/01-create-databases.sql`) creates `airflow`.
- **Why:** saves RAM on a laptop; the databases stay logically separate, so app tables
  never mix with Airflow's. In hosted mode the app database is Neon, which is unaffected.

## D-002 (2026-11-06, Phase 0) - Airflow is opt-in (compose profile) and uses LocalExecutor

- **Problem:** The official Airflow compose file uses CeleryExecutor + Redis + workers and
  recommends at least 4 GB RAM for Airflow alone. Phases 1-3 don't need Airflow at all.
- **Options:** (a) official Celery setup always on; (b) LocalExecutor, always on;
  (c) LocalExecutor behind a `profiles: ["airflow"]` compose profile.
- **Choice:** (c). `docker compose up -d` starts only Redpanda, Console and Postgres;
  `docker compose --profile airflow up -d` adds Airflow (api-server, scheduler,
  dag-processor). No Redis, no Celery workers, no triggerer yet.
- **Why:** WikiPulse's DAGs are a few hourly/weekly jobs; LocalExecutor runs them fine
  on one machine. Measured on 2026-11-06 the Airflow containers used roughly 630 MiB
  together (see METRICS.md). A triggerer can be added if a DAG ever uses deferrable operators.

## D-003 (2026-11-06, Phase 0) - Airflow SimpleAuthManager with "all admins" locally

- **Problem:** Airflow 3 needs an auth manager; FAB adds users/passwords to manage.
- **Choice:** `SimpleAuthManager` with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`.
- **Why:** Airflow only runs on localhost in full streaming mode and is never deployed.
  **This must not be exposed publicly.**

## D-004 (2026-11-06, Phase 0) - Redpanda in dev-container mode with two listeners

- **Problem:** Containers and the laptop need to reach the broker at different addresses.
- **Choice:** an internal listener `redpanda:9092` for containers and an external one
  `localhost:19092` for scripts run on the host. `--mode=dev-container`, 1 core, 1 GB.
- **Why:** this is Redpanda's documented single-node quickstart pattern; dev-container mode
  relaxes fsync settings that only matter for production durability.

## D-005 (2026-11-06, Phase 0) - Python 3.12, src layout, Ruff, pytest

- **Choice:** Python 3.12 (container images and CI), `src/wikipulse` package,
  Ruff for lint + formatting, pytest.
- **Why:** 3.12 is supported by every library in the planned stack (confluent-kafka,
  LightGBM, PyIceberg, dbt, Airflow image tag `3.4.0-python3.12`). Ruff replaces
  flake8/isort/black with one fast tool.

## D-006 (2026-11-06, Phase 0) - Wikimedia contact info comes from the environment

- **Problem:** Wikimedia requires contact info in the User-Agent, but personal emails
  shouldn't be hard-coded in a public repo.
- **Choice:** `WIKIPULSE_CONTACT` env var; default is the repo's GitHub issues URL
  (the policy accepts "an email or full URL").
