"""Live scoring: give every new edit a vandalism risk score.

    python -m wikipulse.ml.score           # loop: every 15 s, score unscored edits
    python -m wikipulse.ml.score --once    # score what's waiting, then exit

Uses the model marked current in <data>/models/current.json (written by training) and the
SAME feature code as training (features.py), with 24 hours of context per batch. The
score is a probability that the edit will be reverted: an automated estimate, never an
accusation (spec Part J). `risk_score >= threshold` puts an edit in the high-risk feed.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import psycopg
from prometheus_client import Counter, Gauge

from wikipulse.ml.data import COLUMNS, _frame, load_context
from wikipulse.ml.features import build_features
from wikipulse.ml.labels import find_reverts

log = logging.getLogger("wikipulse.ml.score")
SCORED = Counter("wikipulse_scorer_edits_total", "Edits scored", ["band"])
LAG = Gauge("wikipulse_scorer_lag_seconds", "Age of the newest scored edit")


@dataclass
class LoadedModel:
    version: str
    threshold: float
    features: list[str]
    booster: object

    def predict(self, X) -> np.ndarray:
        return self.booster.predict(X[self.features])


def load_current(model_dir: Path) -> LoadedModel | None:
    import lightgbm as lgb

    pointer = model_dir / "current.json"
    if not pointer.exists():
        return None
    version = json.loads(pointer.read_text(encoding="utf-8"))["version"]
    meta = json.loads((model_dir / version / "meta.json").read_text(encoding="utf-8"))
    booster = lgb.Booster(model_file=str(model_dir / version / "model.txt"))
    return LoadedModel(version, float(meta["threshold"]), list(meta["features"]), booster)


UNSCORED = f"""
SELECT {", ".join(COLUMNS)} FROM edits
WHERE risk_score IS NULL AND ts >= %s ORDER BY ts LIMIT %s
"""


def score_batch(conn: psycopg.Connection, model: LoadedModel, since: datetime, limit=2000) -> int:
    targets = _frame(conn.execute(UNSCORED, (since, limit)).fetchall())
    if targets.empty:
        return 0
    context = load_context(
        conn,
        targets,
        targets["ts"].min() - timedelta(days=1),
        targets["ts"].max() + timedelta(seconds=1),
    )
    reverts = find_reverts(context, timedelta(days=1))
    X = build_features(context, targets, reverts.reverted_at)
    p = model.predict(X)
    rows = [(float(s), model.version, eid) for s, eid in zip(p, targets["event_id"], strict=True)]
    with conn.transaction():
        conn.cursor().executemany(
            "UPDATE edits SET risk_score = %s, model_version = %s WHERE event_id = %s", rows
        )
    high = int((p >= model.threshold).sum())
    SCORED.labels("high").inc(high)
    SCORED.labels("normal").inc(len(p) - high)
    LAG.set((datetime.now(UTC) - targets["ts"].max().to_pydatetime()).total_seconds())
    return len(p)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m wikipulse.ml.score")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--lookback-hours", type=float, default=6, help="ignore older unscored edits")
    ap.add_argument("--no-metrics-server", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from wikipulse.config import load_settings
    from wikipulse.db.__main__ import upgrade

    settings = load_settings()
    upgrade(settings.database_url)
    if not args.no_metrics_server:
        from prometheus_client import start_http_server

        start_http_server(settings.scorer_metrics_port)
    model_dir = settings.lake_dir.parent / "models"
    model, loaded_at = None, 0.0
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        while True:
            if model is None or time.monotonic() - loaded_at > 300:  # pick up retrained models
                model, loaded_at = load_current(model_dir), time.monotonic()
                if model is None:
                    log.warning(
                        "no trained model in %s yet; run python -m wikipulse.ml.train", model_dir
                    )
            n = 0
            if model is not None:
                since = datetime.now(UTC) - timedelta(hours=args.lookback_hours)
                n = score_batch(conn, model, since)
                if n:
                    log.info("scored %d edits with %s", n, model.version)
            if args.once and n == 0:
                break
            if n == 0:
                time.sleep(15)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
