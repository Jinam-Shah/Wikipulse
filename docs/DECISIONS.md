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
