"""Postgres -> Parquet -> Iceberg bronze/silver/gold -> dbt, on synthetic data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")
pytest.importorskip("pyiceberg")

HOURS = 3


@pytest.fixture
def lake(fresh_db_url, tmp_path):
    """A fresh database with 3 finished hours of synthetic edits, and an empty lake."""
    from wikipulse.devtools.sample import generate
    from wikipulse.lake.export import complete_hours
    from wikipulse.lake.paths import LakePaths
    from wikipulse.processor.store import PostgresStore

    now = datetime.now(UTC)
    hours = complete_hours(now, HOURS)
    events = generate(HOURS, hours[-1] + timedelta(hours=1), edits_per_hour=150, seed=3)
    store = PostgresStore(psycopg.connect(fresh_db_url, autocommit=True))
    store.write_batch(events, cutoff=now - timedelta(days=30))
    return {
        "url": fresh_db_url,
        "paths": LakePaths.at(tmp_path / "lake"),
        "hours": hours,
        "now": now,
        "store": store,
        "events": events,
    }


def export(lake, hours=None):
    from wikipulse.lake.export import export_hours

    with psycopg.connect(lake["url"]) as conn:
        return export_hours(
            conn, lake["paths"], hours or lake["hours"], now=lake["now"], retention_days=7
        )


def build(lake, hours=None):
    from wikipulse.lake.iceberg import build_hour, ensure_tables
    from wikipulse.lake.paths import load_catalog

    tables = ensure_tables(load_catalog(lake["paths"]))
    return tables, [build_hour(tables, lake["paths"], h) for h in hours or lake["hours"]]


def pg_hour_counts(url):
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT date_trunc('hour', ts), count(*) FROM edits GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return {h: n for h, n in rows}


def test_export_matches_postgres_and_is_idempotent(lake):
    from wikipulse.lake.export import read_hour

    first = export(lake)
    counts = pg_hour_counts(lake["url"])
    assert [e.rows for e in first] == [counts[h] for h in lake["hours"]]
    assert sum(e.rows for e in first) == len(lake["events"])

    again = export(lake)
    assert [e.rows for e in again] == [e.rows for e in first]
    table = read_hour(lake["paths"], lake["hours"][0])
    assert table.num_rows == first[0].rows
    assert len(set(table.column("event_id").to_pylist())) == table.num_rows


def test_export_refuses_unfinished_and_pruned_hours(lake):
    from wikipulse.lake.export import floor_hour

    with pytest.raises(ValueError, match="not finished"):
        export(lake, [floor_hour(lake["now"])])
    with pytest.raises(ValueError, match="older than Postgres retention"):
        export(lake, [floor_hour(lake["now"] - timedelta(days=8))])


def test_medallion_counts_and_idempotent_rebuild(lake):
    export(lake)
    tables, builds = build(lake)
    total = len(lake["events"])
    assert sum(b.bronze for b in builds) == total
    assert sum(b.silver for b in builds) == total
    assert all(b.wiki_hourly == 1 for b in builds)

    def rows(ident):
        return tables[ident].refresh().scan().to_arrow().num_rows

    before = {ident: rows(ident) for ident in tables}
    snapshots_before = len(tables["silver.edits"].refresh().metadata.snapshots)

    tables, _ = build(lake)  # rebuild everything
    assert {ident: rows(ident) for ident in tables} == before
    assert len(tables["silver.edits"].refresh().metadata.snapshots) > snapshots_before

    spec = tables["silver.edits"].spec()
    assert [f.name for f in spec.fields] == ["ts_day"]  # hidden day partitioning


def test_late_event_updates_its_hour_and_time_travel_shows_the_old_version(lake):
    from wikipulse.devtools.sample import generate

    export(lake)
    tables, _ = build(lake)
    silver = tables["silver.edits"]
    old_snapshot = silver.refresh().current_snapshot().snapshot_id
    old_rows = silver.scan().to_arrow().num_rows

    # Extra edits that "arrived late" for the first hour (new event ids: different seed).
    late = generate(1, lake["hours"][0] + timedelta(hours=1), edits_per_hour=1, seed=99)
    assert late
    lake["store"].write_batch(late, cutoff=lake["now"] - timedelta(days=30))
    export(lake, [lake["hours"][0]])
    tables, _ = build(lake, [lake["hours"][0]])

    silver = tables["silver.edits"].refresh()
    assert silver.scan().to_arrow().num_rows == old_rows + len(late)
    assert silver.scan(snapshot_id=old_snapshot).to_arrow().num_rows == old_rows


def test_gold_reconciles_with_the_stream_processor(lake):
    """Two independent paths must agree: the processor's Postgres windows and the lake."""
    export(lake)
    tables, _ = build(lake)

    gold = tables["gold.page_hourly"].scan().to_arrow().to_pylist()
    lake_totals: dict[datetime, list[int]] = {}
    for r in gold:
        t = lake_totals.setdefault(r["hour_start"], [0, 0, 0])
        t[0] += r["edits"]
        t[1] += r["reverts"]
        t[2] += r["bytes_delta"]

    with psycopg.connect(lake["url"]) as conn:
        pg = conn.execute(
            "SELECT date_trunc('hour', window_start), sum(edits), sum(reverts), sum(bytes_delta) "
            "FROM page_metrics_5min GROUP BY 1"
        ).fetchall()
    pg_totals = {h: [int(e), int(r), int(b)] for h, e, r, b in pg}
    assert lake_totals == pg_totals


def test_dbt_build_passes_on_the_lake(lake, tmp_path, monkeypatch):
    from wikipulse.lake.__main__ import run_dbt

    export(lake)
    build(lake)
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "dbt-logs"))
    code = run_dbt(
        lake["paths"],
        ["build", "--target-path", str(tmp_path / "dbt-target"), "--no-use-colors"],
    )
    assert code == 0

    import duckdb

    # Same settings as dbt's still-open in-process connection (DuckDB requires that).
    con = duckdb.connect(str(lake["paths"].duckdb))
    try:
        edits = con.execute("select sum(edits) from marts.mart_wiki_daily").fetchone()[0]
        top = con.execute("select count(*) from marts.mart_top_pages_daily").fetchone()[0]
    finally:
        con.close()
    assert edits == len(lake["events"])
    assert 0 < top <= 25 * 2  # at most 25 per day; the 3 hours may span midnight
