"""Data-quality runner and training snapshot against real Postgres + Iceberg."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")
pytest.importorskip("pyiceberg")


@pytest.fixture
def loaded(fresh_db_url, tmp_path):
    """8 finished hours of synthetic edits, exported and built into a fresh lake."""
    from wikipulse.devtools.sample import generate
    from wikipulse.lake.export import complete_hours, export_hours
    from wikipulse.lake.iceberg import build_hour, ensure_tables
    from wikipulse.lake.paths import LakePaths, load_catalog
    from wikipulse.processor.store import PostgresStore

    now = datetime.now(UTC)
    hours = complete_hours(now, 8)
    events = generate(8, hours[-1] + timedelta(hours=1), edits_per_hour=120, seed=11)
    store = PostgresStore(psycopg.connect(fresh_db_url, autocommit=True))
    store.write_batch(events, cutoff=now - timedelta(days=30))
    paths = LakePaths.at(tmp_path / "lake")
    with psycopg.connect(fresh_db_url) as conn:
        export_hours(conn, paths, hours, now=now, retention_days=7)
    tables = ensure_tables(load_catalog(paths))
    for h in hours:
        build_hour(tables, paths, h)
    return {"url": fresh_db_url, "paths": paths, "hours": hours, "now": now, "store": store}


def stored_checks(url):
    with psycopg.connect(url) as conn:
        return conn.execute(
            "SELECT check_name, wiki, status, details FROM data_quality_checks ORDER BY id"
        ).fetchall()


def test_quality_checks_pass_on_a_healthy_pipeline(loaded):
    from wikipulse.quality.runner import run

    hour = loaded["hours"][-2]  # "settled" hour, like the DAG uses
    results = run(loaded["url"], loaded["paths"].root, hour, ["samplewiki"], loaded["now"])
    by_name = {(r.name, r.wiki): r for r in results}
    assert by_name[("stream_freshness", None)].status == "pass"
    assert by_name[("lake_reconciliation", "samplewiki")].status == "pass"
    assert by_name[("hourly_volume", "samplewiki")].status == "pass"
    assert by_name[("hourly_volume", "samplewiki")].details["history_hours"] == 6
    assert len(stored_checks(loaded["url"])) == len(results)


def test_reconciliation_fails_when_postgres_got_edits_the_lake_missed(loaded):
    from wikipulse.devtools.sample import generate
    from wikipulse.quality.runner import run

    hour = loaded["hours"][-2]
    late = generate(1, hour + timedelta(hours=1), edits_per_hour=3, seed=77)
    loaded["store"].write_batch(late, cutoff=loaded["now"] - timedelta(days=30))

    results = run(loaded["url"], loaded["paths"].root, hour, [], loaded["now"])
    recon = next(r for r in results if r.name == "lake_reconciliation")
    assert recon.status == "fail" and recon.observed == -len(late)
    rows = stored_checks(loaded["url"])
    assert ("lake_reconciliation", "samplewiki", "fail") in [r[:3] for r in rows]


def test_main_exit_code_reflects_failures(loaded, monkeypatch):
    from wikipulse.quality.runner import main

    monkeypatch.setenv("WIKIPULSE_DATABASE_URL", loaded["url"])
    monkeypatch.setenv("WIKIPULSE_LAKE_DIR", str(loaded["paths"].root))
    monkeypatch.setenv("WIKIPULSE_WIKIS", "samplewiki")
    hour = loaded["hours"][-2].isoformat()
    assert main(["--hour", hour]) == 0
    # An hour the lake never built: reconciliation must fail -> exit code 1.
    with psycopg.connect(loaded["url"], autocommit=True) as conn:
        conn.execute(
            "UPDATE edits SET ts = %s WHERE event_id = (SELECT min(event_id) FROM edits)",
            (loaded["hours"][0] - timedelta(hours=5),),
        )
    assert main(["--hour", (loaded["hours"][0] - timedelta(hours=5)).isoformat()]) == 1


def test_training_snapshot_is_traceable_to_an_iceberg_snapshot(loaded):
    from wikipulse.lake.paths import load_catalog
    from wikipulse.lake.snapshot import training_snapshot

    end = loaded["hours"][-1] + timedelta(hours=1)
    snap = training_snapshot(loaded["paths"], end, days=7)
    silver = load_catalog(loaded["paths"]).load_table("silver.edits")
    assert snap.rows == silver.scan().to_arrow().num_rows
    assert snap.iceberg_snapshot_id == silver.current_snapshot().snapshot_id
    manifest = json.loads(open(snap.path.replace(".parquet", ".json"), encoding="utf-8").read())
    assert manifest["rows"] == snap.rows
    # Time travel: the exact data behind the snapshot is still readable.
    assert silver.scan(snapshot_id=snap.iceberg_snapshot_id).to_arrow().num_rows == snap.rows
