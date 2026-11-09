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

## D-007 (2026-11-09, Phase 1) - Main stream is `mediawiki.page_change.v1`, not `recentchange`

- **Problem:** The spec names `recentchange` as the main stream. Inspecting 500 live
  `recentchange` events showed it has **no `page_id`** (needed for the `edits` and
  `page_metrics_5min` keys) and no reliable "anonymous" flag now that Wikimedia uses
  temporary accounts instead of showing IP addresses.
- **Options:** (a) `recentchange` + look up page IDs through the API (extra calls, rate
  limits); (b) `recentchange` keyed by title (titles change on page moves);
  (c) `mediawiki.page_change.v1`.
- **Choice:** (c).
- **Why:** each `page_change` event carries `page_id`, `performer.is_temp`, `is_bot`,
  `groups`, the editor's `edit_count` and `registration_dt`, the new and previous
  revision sizes, and `rev_sha1` (a content hash). The editor fields are strong vandalism
  features; matching `rev_sha1` values detect exact reverts for the edit war detector.
  All of it comes free, with no extra API calls.
- **Trade-offs:** `page_change` has no `log`/`categorize` events (we don't need them).
  Any fields we later need from `recentchange` can be added by a second normalizer.

## D-008 (2026-11-09, Phase 1) - Own small SSE client on top of httpx

- **Options:** `requests-sseclient`/`httpx-sse`/`aiohttp-sse-client`, or about 80 lines of our own.
- **Choice:** our own `wikipulse.wikimedia.sse` (parser + reconnecting client).
- **Why:** reconnect behaviour is the part that matters (backoff with jitter, honouring
  `Retry-After` on 429, not retrying 4xx configuration errors, resuming with
  `Last-Event-ID`), and owning it makes each behaviour unit-testable with
  `httpx.MockTransport`. httpx is also what later phases use for Wikimedia REST calls.

## D-009 (2026-11-09, Phase 1) - Resume checkpoint saved only after Kafka confirms delivery

- **Problem:** restart without losing events, and without trusting events Kafka never stored.
- **Options:** (a) save the SSE ID as soon as it's read; (b) store it in Kafka headers and
  read the topic tail on start; (c) save it to a file right after a successful `flush()`.
- **Choice:** (c), flushing every `WIKIPULSE_FLUSH_INTERVAL_S` (default 1 s). If any
  message fails or stays unconfirmed, the checkpoint is not advanced and the producer exits.
- **Why:** (a) can lose events in a crash; (b) is fiddly with several partitions.
  (c) gives **at-least-once**: after a crash, up to about one flush interval of events is
  re-sent. Downstream writes upsert on `event_id`, so duplicates are harmless. Measured: a
  `kill -9` + 20 s outage produced exactly 1 duplicate and no gap (METRICS.md).
- **Limit:** Wikimedia keeps only a limited history, so a producer that stays down for
  a long time cannot fully catch up.

## D-010 (2026-11-09, Phase 1) - Topics, keys, partitions, retention, serialization

- `wikipulse.edits.v1`: 3 partitions, key `<wiki>:<page_id>`, so all edits to one page
  land in one partition, in order (the Phase 2 window aggregation relies on this). 7-day retention.
- `wikipulse.edits.dlq.v1`: 1 partition, 14-day retention. Value = reason
  (`invalid_json`, `schema_validation`, `unexpected_error`), the error text, the raw
  payload (truncated to 100 KB) and the SSE ID. A `reason` header allows filtering
  without parsing.
- Values are JSON with a `schema_version` field and header. Avro/Protobuf with the
  Redpanda schema registry is a possible upgrade later; JSON keeps Phase 1 simple and
  readable in Redpanda Console.
- Producer: `enable.idempotence=true` (implies `acks=all`), lz4 compression, `linger.ms=20`.
- The `.v1` suffix lets a breaking schema change go to a new topic instead of breaking consumers.

## D-011 (2026-11-09, Phase 1) - What is filtered vs dead-lettered

- **Filtered (counted, not stored):** other wikis, non-article namespaces (configurable
  via `WIKIPULSE_WIKIS` / `WIKIPULSE_NAMESPACES`), change kinds other than
  `edit`/`create` (delete, move, undelete, visibility), and Wikimedia `canary` test events.
- **Dead-lettered:** only events we *wanted* but couldn't use: invalid JSON, or a tracked
  event missing a required field or with the wrong type.
- **Why:** filtering runs on the raw dict *before* validation, so a schema quirk on a wiki
  we don't track never inflates our dead-letter rate. Unknown extra fields are ignored,
  so Wikimedia adding fields never breaks us.

## D-012 (2026-11-09, Phase 1) - "Anonymous" means temporary account or IP editor

- Wikimedia now gives logged-out editors **temporary accounts** (usernames starting
  with `~`, `performer.is_temp = true`) instead of showing their IP address.
  `is_anon = is_temp OR (username present AND no user_id)`; the second part covers
  wikis that still show IPs. In a 180-second enwiki sample, 5.4 % of edits were anonymous.
- Suppressed usernames (hidden by Wikipedia admins) arrive with no `user_text`; we
  keep the edit with `user_name = null` and never try to recover the name.

## D-013 (2026-11-09, Phase 2) - Plain confluent-kafka consumer, metrics recomputed in Postgres

- **Problem:** turn the edit stream into per-page 5-minute windows (and per-wiki 1-minute
  windows for the dashboard) with at-least-once delivery and **no double counting on replay**.
- **Options:** (a) Quix Streams (windowing + RocksDB state, its own checkpointing);
  (b) plain consumer, windows kept in Python memory, flushed as increments;
  (c) plain consumer; store raw edits keyed by `event_id`, then *recompute* each
  affected window from the stored edits in SQL and upsert it.
- **Choice:** (c).
- **Why:**
  - Idempotent by construction: a window row is a pure function of the `edits` table,
    and an edit is stored once (`ON CONFLICT (event_id) DO NOTHING`). Replays,
    duplicates, out-of-order and late events all give the same numbers. The integration
    tests check exactly this (replay, shuffled order, small batches).
  - One Postgres transaction per batch, then the Kafka offset commit, so the "commit
    offsets only after a successful DB write" rule holds with no extra state store.
    Quix keeps its own state and commit schedule, which would put a second source of
    truth beside Postgres.
  - The same code works in hosted mode (Phase 13), where a GitHub Actions run drains
    the topic and exits (`--max-idle-seconds`).
  - Cost: recomputing a window reads that page's edits in the window (a handful of
    rows, indexed on `(wiki, page_id, ts)`). Measured throughput is in METRICS.md.
- Windows use **event time** (`ts`, when Wikipedia saved the revision), via
  `date_bin('5 minutes', ts, '2000-01-01')`.

## D-014 (2026-11-09, Phase 2) - Retention, late events, schema migrations

- Raw `edits` are kept for `WIKIPULSE_RETENTION_DAYS` (default 7) and pruned by the
  processor every 10 minutes. Window tables are kept, because they are small.
- Events older than the retention cutoff are **dropped and counted** (`too_old`): their
  windows may already be pruned, and recomputing them would undercount.
- Schema is owned by **Alembic** from the start (the spec names it for the backend).
  Migrations are hand-written SQL, applied with `python -m wikipulse.db upgrade`;
  the processor applies them on start, which is safe to repeat.
- **Neon free tier (to verify in Phase 13):** a full 7 days of enwiki article edits
  (about 2 edits/s, so about 1.2 million rows) is likely more than Neon's free storage.
  Hosted mode only samples the stream, so it stores far fewer rows; if needed, hosted
  mode will use a shorter retention.

## D-015 (2026-11-09, Phase 2) - Revert signals (for now)

- `revert_kind = 'sha1'`: the edit's content hash equals an earlier revision's hash on the
  same page (an exact revert). Checked over every stored edit of the pages in each batch,
  so it is correct whatever order events arrive in.
- `revert_kind = 'comment'`: the edit summary starts with a revert phrase ("Reverted",
  "Undid revision", "Restored revision", "rv", "rvv", ...; see `processor/revert.py`).
- **Known gaps:** partial or manual reverts with ordinary summaries are missed; the sha1
  check only sees revisions within our retention window; a summary starting with "Revert"
  that isn't really a revert is counted. Phase 6 adds Wikimedia's own revert tags
  (`mw-reverted`, `mw-undo`, `mw-rollback`, `mw-manual-revert`) from
  `mediawiki.revision-tags-change` for model labels.

## D-016 (2026-11-09, Phase 2) - Observability: Prometheus now, Grafana in Phase 13

- Producer (`:9107/metrics`) and processor (`:9108/metrics`) expose Prometheus metrics:
  events by outcome (so the dead-letter rate is
  `dead_letter / (produced + dead_letter)`), consumer lag per partition, batch duration,
  end-to-end latency (revision saved -> row committed), last commit time, pruned rows.
- A Prometheus container (`--profile pipeline`, http://localhost:9090) scrapes both,
  whether they run in containers or directly on your machine (`host.docker.internal`).
- The Grafana dashboard is Phase 13 in the spec; Prometheus' own graph page is enough until then.

## D-017 (2026-11-09, Phase 3) - Hourly exports come from Postgres, not straight from Kafka

- **Problem:** the spec's diagram has hourly Parquet files coming out of the stream processing.
- **Options:** (a) a second Kafka consumer group that buffers an hour and writes Parquet;
  (b) export each finished hour from the Postgres `edits` table.
- **Choice:** (b), `python -m wikipulse.lake export|run`.
- **Why:** Postgres already holds exactly-once, deduplicated edits with the revert flags
  the processor computed, so the lake can never disagree with the live app (an
  integration test checks the gold totals against `page_metrics_5min`). An hour is a
  simple `WHERE ts` query, and re-exporting it later picks up late events. Option (a)
  would repeat the dedupe and revert logic.
- **Safety rules:** unfinished hours are refused; hours older than Postgres retention
  are refused (they would overwrite a good file with an empty one). Files are written to
  a temp name and atomically renamed; each hour has a `_manifest.json` with its row count.
- **Layout:** Hive-style `raw/edits/date=YYYY-MM-DD/hour=HH/edits.parquet` (zstd), so
  Spark/Databricks (Phase 7) can read it as a partitioned dataset.

## D-018 (2026-11-09, Phase 3) - Iceberg medallion with PyIceberg + SQLite catalog, hourly overwrite

- bronze = raw + `_export_hour`/`_loaded_at`; silver = deduped by `event_id`, typed, with
  derived columns (`user_type`, `account_age_days`, `comment_length`, `bytes_delta`,
  `is_revert`, `event_hour`, `ingest_lag_seconds`); gold = `page_hourly`, `wiki_hourly`.
- **Idempotency:** each hour is written with `overwrite(df, overwrite_filter=<that hour>)`:
  one atomic snapshot that replaces the hour. Rebuilding never duplicates. Tested,
  including a late event that updates its hour.
- **Partitioning:** Iceberg *hidden* partitioning by `day(ts)`; queries filter on the
  timestamp and Iceberg prunes files. Day rather than hour partitions avoids lots of tiny files.
- **Time travel:** every hourly load is a snapshot; `table.scan(snapshot_id=...)` reads
  the old version (tested). Snapshots older than 7 days are expired after each run.
- **Catalog:** PyIceberg's SQL catalog on a local SQLite file (`data/lake/catalog.db`),
  as the spec asks. The warehouse is a plain absolute path, which PyIceberg 0.12 handles on
  Windows (drive letters) as well as Linux.
- Transformations are SQL run by in-process DuckDB on Arrow tables, which is readable
  and fast (3 real hours, 14,802 edits, in 2.4 s; METRICS.md).
- Requires the `pyiceberg-core` extra (Rust) to write partitioned tables.

## D-019 (2026-11-09, Phase 3) - dbt-duckdb reads Iceberg through its PyIceberg plugin

- **Options:** (a) DuckDB's `iceberg` extension (downloaded at runtime, needs metadata
  file paths); (b) dbt-duckdb's built-in `iceberg` plugin, which loads tables through
  the same PyIceberg catalog.
- **Choice:** (b). Sources declare `meta: {plugin: iceberg}`; no extension downloads, and
  the same catalog config as the Python side. Marts live in `data/lake/analytics.duckdb`.
- `threads: 1`: the plugin loads sources lazily, and parallel loads raced on creating
  the same DuckDB schema. Total build time is a few seconds, so this costs little.
- **Tests (40):** not_null, unique, accepted_values, relationships, value ranges, unique
  key combinations, source freshness (warn after 3 h, error after 26 h), and a
  reconciliation test (daily totals from gold equal a direct count of silver edits).
- **Spec deviation:** Part I says "dbt build against a Postgres service container". Part F
  chooses dbt + DuckDB for analytics, so CI runs `dbt build` on DuckDB over the Iceberg
  tables, which are built from Postgres running as a container in CI.
- **Docs:** `dbt docs generate --static` is published to GitHub Pages by
  `.github/workflows/dbt-docs.yml`, built on synthetic sample data (CI has no real stream).
- `python -m wikipulse.lake dbt <args>` sets absolute paths and runs dbt in-process, so
  it works from any folder on Windows and Linux.

## D-020 (2026-11-09, Phase 3) - Synthetic sample data lives in its own wiki id

- `python -m wikipulse.devtools.sample` writes deterministic synthetic edits for wiki
  `samplewiki` (never `enwiki`) through the processor's own store, so CI, tests and demos
  exercise the real code paths without touching real data or calling Wikimedia.
- Shape (bot/anon shares, reverts, a burst on one page) is modelled on Phase 1 measurements.

## D-021 (2026-11-09, Phase 3) - "is a revert" is not "was reverted"

- `is_revert` / `revert_kind` mark the edit that **does** the reverting. On real data
  (METRICS.md) registered editors have the highest `is_revert` rate (4.6 %) and anonymous
  editors only 1.7 %, because reverting is mostly done by experienced editors.
- The vandalism model (Phase 6) needs the opposite label: "this edit **was** reverted
  later". That will come from Wikimedia's `mw-reverted` tag plus our sha1 matches, by
  linking each revert back to the edits it undid. Not the same column, and not to be confused.

## D-022 (2026-11-09, Phase 3) - Cap pyarrow < 26 and duckdb < 2

- **Problem:** on the Windows dev machine, `import pyarrow.parquet` failed with
  "DLL load failed ... An Application Control policy has blocked this file". Windows
  (Smart App Control or a similar policy) can block brand-new native files that don't yet
  have a reputation, and pyarrow 26.0.1 was 6 days old. Older native packages (confluent-kafka,
  psycopg) loaded fine.
- **Choice:** pin `pyarrow>=20,<26` and `duckdb>=1.4,<2` (resolving to pyarrow 25.0.1 and
  duckdb 1.5.6 at the time of writing), so every machine and CI use the same,
  better-established releases. The code supports both DuckDB 1.x and 2.x APIs; the full
  test suite passes on both (78 unit + 16 integration).
- **Revisit** when the next phase needs a newer feature, or once the newer releases are
  established.

## D-023 (2026-11-09, Phase 3) - A Linux "dev" container for tests, lakehouse and dbt

- **Problem:** on the Windows dev machine, an Application Control policy blocks
  pyarrow's Parquet DLL in *every* version tried (26.0.1 and 25.0.1), so the lakehouse
  and its tests can't run natively. Packages without that DLL (producer, processor) run
  fine on Windows.
- **Options:** (a) turn off Smart App Control (Windows can't turn it back on without a
  reinstall); (b) WSL 2 with a Linux Python; (c) a `dev` service in docker-compose
  (profile `tools`) built from `Dockerfile.dev`.
- **Choice:** (c). Docker Desktop is already required, nothing on Windows needs to change,
  and it's the same Linux environment CI and Airflow (Phase 4) use.
  `docker compose run --rm dev <command>` runs any command with the repo mounted at `/app`
  (edits are picked up without rebuilding). It talks to `redpanda:9092` and `postgres:5432`
  on the compose network.
- The lake directory is a **named volume** (`lake-data`) instead of the Windows bind mount,
  because SQLite (Iceberg catalog) and DuckDB rely on file locking that bind mounts
  from Windows don't guarantee. Copy files out with `docker compose cp` if needed.
- D-022's version caps stay: they cost nothing and keep CI and the container on the
  same releases.

## D-024 (2026-11-09, Phase 4) - Airflow runs WikiPulse code from its own virtualenv

- **Problem:** WikiPulse needs dbt, PyIceberg, DuckDB, pyarrow and SQLAlchemy 2.1; Airflow
  pins its own versions of several of these. Installing both into one environment risks
  breaking Airflow or our code.
- **Options:** (a) `pip install` into Airflow's environment; (b) DockerOperator (needs
  the Docker socket inside Airflow); (c) a separate virtualenv inside a custom Airflow image.
- **Choice:** (c). `Dockerfile.airflow` builds on `apache/airflow:3.4.0-python3.12` and adds
  `/opt/wikipulse/venv`. Tasks are `BashOperator`s that call the existing CLIs through
  `$WIKIPULSE_PYTHON`, so DAG files import only Airflow.
- `./src` and `./dbt` are mounted read-only, so code edits are used by the next task run
  without a rebuild (rebuild only when dependencies change). dbt's build output goes to
  the shared `lake-data` volume (`DBT_TARGET_PATH`, `DBT_LOG_PATH`).
- Airflow and the dev container share the `lake-data` volume and run as the same uid
  (`AIRFLOW_UID`, default 50000), so files either one writes stay writable by the other.
  `airflow-init` and `lake-init` fix ownership at start (volumes from earlier versions
  were root-owned).

## D-025 (2026-11-09, Phase 4) - Hourly DAG: re-process 3 hours, check the settled one

- `wikipulse_hourly_lake` runs `@hourly`. Each run rebuilds the 3 hours ending at
  `data_interval_end` (E). Late edits (the processor's p99 latency is about 13 s, and a restart
  can delay more) are picked up by the next two runs, so an hour is final about 3 hours later.
  Every step is idempotent, so the overlap costs only a few seconds.
- Templates use only `data_interval_end`. In Airflow 3, cron schedules default to
  trigger timetables where start == end, so `data_interval_start` would be misleading.
- `max_active_runs=1` plus a one-slot `lake` pool: never two writers on the Iceberg
  SQLite catalog or the DuckDB file at once.
- Retries: 2 for load/dbt tasks (transient errors); 0 for checks, because a failed check is a finding.

## D-026 (2026-11-09, Phase 4) - Data-quality checks and where alerts go (for now)

| Check | Pass | Warn | Fail |
|---|---|---|---|
| `stream_freshness` (latest `processed_at`) | <= 15 min old | 15-60 min | > 60 min or no data |
| `hourly_volume` (vs median of previous 24 h *with data*) | 0.2x-5x | > 5x (could be big news) | < 0.2x |
| `lake_reconciliation` (gold.wiki_hourly vs Postgres) | equal | - | any difference, or hour missing |
| `revert_rate` | <= 15 % | > 15 % (>= 50 edits) | - |

- Checked hour = E-2h, old enough that late events have settled (checking the hour that
  just ended would flag false mismatches).
- Volume needs 6 earlier hours of history before judging; hours when the pipeline was off
  are absent from the baseline rather than counted as zero.
- **Alerts:** every result is stored in `data_quality_checks` (history for the dashboard),
  printed in the task log, and a `fail` fails the task (red in the Airflow UI). Email and
  push delivery arrive with the alerting system in Phase 12.
- The dead-letter rate isn't checked here: it lives in Kafka/Prometheus, not Postgres. It's
  in the Prometheus metrics (D-016) and will be alerted on with Grafana in Phase 13.

## D-027 (2026-11-09, Phase 4) - Weekly retraining DAG before the model exists

- `wikipulse_weekly_retrain` (Sundays 03:17 UTC) already writes the weekly training
  snapshot: 7 days of `silver.edits` to `training/edits_<date>.parquet`, with a manifest
  holding the Iceberg snapshot id, so each model can be traced to exact data.
- `train_model` runs `python -m wikipulse.ml.train` once it exists (Phase 6); until then it
  exits 99, which Airflow treats as **skipped**, not failed or faked.

## D-028 (2026-11-09, Phase 4) - The lake is mounted at the same path in every container

- **Found while testing Phase 4:** Iceberg metadata (and the SQLite catalog) store
  **absolute** file locations. Airflow mounted the lake at `/opt/wikipulse/data` while the
  dev container used `/app/data`, so tables written by one were unreadable by the other
  ("Failed to open local file /opt/wikipulse/...").
- **Fix:** every container mounts the `lake-data` volume at `/opt/wikipulse/data`
  (`WIKIPULSE_LAKE_DIR=/opt/wikipulse/data/lake`). A lake built natively on a machine
  (`./data/lake`) is a *separate* lake; don't mix the two.
- **Migration:** a `lake-data` volume built by the Phase 3 dev container has `/app/...`
  paths baked in. Delete the volume once (`docker volume rm wikipulse_lake-data`) and let the
  next run rebuild it. The lake can be rebuilt from Postgres for any hour within retention.
