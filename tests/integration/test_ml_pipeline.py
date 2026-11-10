"""Train -> save -> MLflow -> model_runs -> live scoring, on synthetic data in real Postgres."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")
pytest.importorskip("lightgbm")


@pytest.fixture
def env(fresh_db_url, tmp_path, monkeypatch):
    from wikipulse.devtools.sample import SAMPLE_WIKI, generate
    from wikipulse.lake.export import floor_hour
    from wikipulse.processor.store import PostgresStore

    now = datetime.now(UTC)
    events = generate(10, floor_hour(now), edits_per_hour=300, seed=5)
    store = PostgresStore(psycopg.connect(fresh_db_url, autocommit=True))
    store.write_batch(events, cutoff=now - timedelta(days=30))
    # Fake Lift Wing scores for most revisions (the real ones come from EventStreams).
    rng = random.Random(1)
    with psycopg.connect(fresh_db_url, autocommit=True) as conn:
        conn.cursor().executemany(
            "INSERT INTO liftwing_scores VALUES "
            "(%s, %s, 'revertrisk-language-agnostic', '3', %s, %s)",
            [(SAMPLE_WIKI, e.revision_id, rng.random(), e.event_dt) for e in events[::1]
             if rng.random() < 0.9],
        )  # fmt: skip
    monkeypatch.setenv("WIKIPULSE_DATABASE_URL", fresh_db_url)
    monkeypatch.setenv("WIKIPULSE_WIKIS", SAMPLE_WIKI)
    monkeypatch.setenv("WIKIPULSE_LAKE_DIR", str(tmp_path / "data" / "lake"))
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    return {"url": fresh_db_url, "data": tmp_path / "data", "now": now}


def test_train_logs_everything_and_scorer_uses_the_model(env, capsys):
    from wikipulse.ml import score, train

    end = (env["now"] - timedelta(hours=1)).isoformat()
    assert train.main(["--end", end, "--label-window-hours", "2"]) == 0
    report = json.loads(capsys.readouterr().out)

    for model in ("logreg", "lightgbm"):
        m = report["test"][model]
        assert m["n"] > 100 and m["positives"] > 0
        assert 0 <= m["pr_auc"] <= 1 and 0 <= m["f1"] <= 1
    lw = report["same_edits_as_liftwing"]
    assert 0.8 < lw["liftwing_coverage_test"] <= 1.0
    assert {"liftwing", "logreg", "lightgbm"} <= set(lw)
    # The split really is by time: train ends before validation, validation before test.
    split = report["split"]
    assert split["train"][1] <= split["valid"][0] <= split["valid"][1] <= split["test"][0]

    models = env["data"] / "models"
    current = json.loads((models / "current.json").read_text())["version"]
    assert current == report["version"]
    assert (models / current / "model.txt").exists()
    assert (env["data"] / "mlflow" / "mlflow.db").exists()
    with psycopg.connect(env["url"]) as conn:
        version, run_id, is_current = conn.execute(
            "SELECT version, mlflow_run_id, is_current FROM model_runs"
        ).fetchone()
    assert (version, is_current) == (current, True) and run_id

    # Live scoring: recent unscored edits get a probability and the model version.
    assert score.main(["--once", "--lookback-hours", "6", "--no-metrics-server"]) == 0
    with psycopg.connect(env["url"]) as conn:
        scored, versions, lo, hi = conn.execute(
            "SELECT count(*), array_agg(DISTINCT model_version), min(risk_score), max(risk_score) "
            "FROM edits WHERE risk_score IS NOT NULL"
        ).fetchone()
    assert scored > 1000 and versions == [current] and 0 <= lo <= hi <= 1
