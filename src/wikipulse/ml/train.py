"""Train and evaluate the vandalism risk model; compare with Lift Wing; log to MLflow.

    python -m wikipulse.ml.train                         # the last 7 days in Postgres
    python -m wikipulse.ml.train --start 2026-11-06T00:00Z --end 2026-11-10T00:00Z

Evaluation is time-based (docs/DECISIONS.md D-036): edits are sorted by time; the oldest
70 % train, the next 10 % pick the decision threshold (max F1) and LightGBM's early
stopping, and the newest 20 % are the held-out test set. Only edits whose label window
has fully passed are used, so "not reverted yet" is never mistaken for "not reverted".
Lift Wing's `revertrisk-language-agnostic` scores are evaluated on exactly the same
test edits (and its threshold is chosen the same way, on the same validation edits).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    precision_recall_fscore_support,
    roc_auc_score,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from wikipulse.ml.features import FEATURES, build_features
from wikipulse.ml.labels import find_reverts

log = logging.getLogger("wikipulse.ml.train")


class InsufficientData(ValueError):
    """Not enough labelled edits to train (e.g. a fresh install). CLI exit code 99."""


@dataclass
class Dataset:
    X: pd.DataFrame
    y: np.ndarray
    meta: pd.DataFrame  # event_id, revision_id, ts, wiki, is_bot, ...


def make_dataset(edits: pd.DataFrame, label_window: timedelta, start, end) -> Dataset:
    """Targets = edits in [start, end) whose label window ended before the data does."""
    if edits.empty:
        raise InsufficientData("no edits in the training window")
    reverts = find_reverts(edits, label_window)
    data_end = edits["ts"].max()
    mask = (edits["ts"] >= start) & (edits["ts"] < end) & (edits["ts"] <= data_end - label_window)
    targets = edits[mask].sort_values(["ts", "event_id"]).reset_index(drop=True)
    if len(targets) < 200:
        raise InsufficientData(f"only {len(targets)} edits with a complete label window")
    X = build_features(edits, targets, reverts.reverted_at)
    y = targets["event_id"].map(reverts.reverted_at).notna().to_numpy(np.int8)
    return Dataset(X, y, targets)


def split_by_time(n: int, train=0.7, valid=0.1):
    a, b = int(n * train), int(n * (train + valid))
    return np.arange(0, a), np.arange(a, b), np.arange(b, n)


def best_threshold(y: np.ndarray, p: np.ndarray) -> float:
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-12, None)
    return float(thr[int(np.nanargmax(f1[:-1]))])


def evaluate(y: np.ndarray, p: np.ndarray, threshold: float) -> dict:
    pred = (p >= threshold).astype(int)
    prec, rec, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    return {
        "precision": round(float(prec), 4),
        "recall": round(float(rec), 4),
        "f1": round(float(f1), 4),
        "pr_auc": round(float(average_precision_score(y, p)), 4),
        "roc_auc": round(float(roc_auc_score(y, p)), 4) if 0 < y.sum() < len(y) else None,
        "threshold": round(float(threshold), 4),
        "n": int(len(y)),
        "positives": int(y.sum()),
        "base_rate": round(float(y.mean()), 4),
    }


def train_models(ds: Dataset, seed: int = 7):
    import lightgbm as lgb

    tr, va, te = split_by_time(len(ds.y))
    X, y = ds.X, ds.y
    lr = make_pipeline(StandardScaler(), LogisticRegression(class_weight="balanced", max_iter=3000))
    lr.fit(X.iloc[tr], y[tr])
    gbm = lgb.LGBMClassifier(
        n_estimators=3000, learning_rate=0.03, num_leaves=63, min_child_samples=50,
        subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
        random_state=seed, verbose=-1,
    )  # fmt: skip
    # LightGBM 4.7 renamed eval_set to eval_X/eval_y; support both.
    import inspect

    if "eval_X" in inspect.signature(gbm.fit).parameters:
        eval_kwargs = {"eval_X": (X.iloc[va],), "eval_y": (y[va],)}
    else:
        eval_kwargs = {"eval_set": [(X.iloc[va], y[va])]}
    gbm.fit(
        X.iloc[tr], y[tr], eval_metric="average_precision",
        callbacks=[lgb.early_stopping(100, verbose=False)], **eval_kwargs,
    )  # fmt: skip
    return {"logreg": lr, "lightgbm": gbm}, (tr, va, te)


def score(model, X: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(X)[:, 1]


def run(edits: pd.DataFrame, liftwing: pd.Series, label_window: timedelta, start, end):
    ds = make_dataset(edits, label_window, start, end)
    if ds.y.sum() < 20:
        raise InsufficientData(
            f"only {int(ds.y.sum())} reverted edits among {len(ds.y)}; need at least 20"
        )
    models, (tr, va, te) = train_models(ds)
    results, thresholds = {}, {}
    for name, m in models.items():
        p_va, p_te = score(m, ds.X.iloc[va]), score(m, ds.X.iloc[te])
        thresholds[name] = best_threshold(ds.y[va], p_va)
        results[name] = evaluate(ds.y[te], p_te, thresholds[name])

    # Lift Wing on the same edits (where it published a score).
    lw = ds.meta["revision_id"].map(liftwing).to_numpy(dtype="float64")
    cov_va, cov_te = va[~np.isnan(lw[va])], te[~np.isnan(lw[te])]
    comparison = {"liftwing_coverage_test": round(float(len(cov_te) / max(len(te), 1)), 4)}
    if len(cov_te) and ds.y[cov_va].sum() and ds.y[cov_te].sum():
        thr_lw = best_threshold(ds.y[cov_va], lw[cov_va])
        comparison["liftwing"] = evaluate(ds.y[cov_te], lw[cov_te], thr_lw)
        for name, m in models.items():
            comparison[name] = evaluate(ds.y[cov_te], score(m, ds.X.iloc[cov_te]), thresholds[name])
    split = {
        "train": [str(ds.meta["ts"].iloc[tr[0]]), str(ds.meta["ts"].iloc[tr[-1]])],
        "valid": [str(ds.meta["ts"].iloc[va[0]]), str(ds.meta["ts"].iloc[va[-1]])],
        "test": [str(ds.meta["ts"].iloc[te[0]]), str(ds.meta["ts"].iloc[te[-1]])],
        "label_window_hours": label_window.total_seconds() / 3600,
    }
    return ds, models, thresholds, results, comparison, split


def time_to_revert(edits: pd.DataFrame, window: timedelta) -> dict:
    """Distribution of revert delays, used to choose the label window (D-034)."""
    rv = find_reverts(edits, window)
    delay = (rv.reverted_at - edits.set_index("event_id")["ts"]).dropna().dt.total_seconds()
    if delay.empty:
        return {}
    q = delay.quantile([0.5, 0.9, 0.95, 0.99])
    return {
        "reverted": int(len(delay)),
        "p50_min": round(q[0.5] / 60, 1), "p90_min": round(q[0.9] / 60, 1),
        "p95_min": round(q[0.95] / 60, 1), "p99_min": round(q[0.99] / 60, 1),
        "within_1h": round(float((delay <= 3600).mean()), 4),
        "within_6h": round(float((delay <= 6 * 3600).mean()), 4),
    }  # fmt: skip


def save_model(model_dir: Path, version: str, gbm, threshold: float, metrics: dict) -> Path:
    out = model_dir / version
    out.mkdir(parents=True, exist_ok=True)
    gbm.booster_.save_model(str(out / "model.txt"))
    meta = {"version": version, "features": FEATURES, "threshold": threshold, "metrics": metrics}
    (out / "meta.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    tmp = model_dir / "current.json.tmp"
    tmp.write_text(json.dumps({"version": version}), encoding="utf-8")
    os.replace(tmp, model_dir / "current.json")
    return out


def log_mlflow(
    tracking_uri: str,
    artifact_root: Path,
    version: str,
    params,
    results,
    comparison,
    split,
    gbm,
    out,
):
    # Containers have no git; tell MLflow not to look for a commit hash (avoids warnings).
    os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")
    os.environ.setdefault("MLFLOW_TRACKING_GIT_DISABLED", "true")
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    name = "wikipulse-vandalism"
    if mlflow.get_experiment_by_name(name) is None:
        # Explicit artifact folder next to the database, at the same absolute path in
        # every container (lesson from D-028), not "./mlruns" of whatever process ran first.
        mlflow.create_experiment(name, artifact_location=artifact_root.as_uri())
    mlflow.set_experiment(name)
    with mlflow.start_run(run_name=version) as r:
        mlflow.log_params({**params, "features": len(FEATURES)})
        for model_name, m in results.items():
            mlflow.log_metrics(
                {
                    f"test_{model_name}_{k}": v
                    for k, v in m.items()
                    if isinstance(v, (int, float)) and v is not None
                }
            )
        for model_name, m in comparison.items():
            if isinstance(m, dict):
                mlflow.log_metrics(
                    {
                        f"same_edits_{model_name}_{k}": v
                        for k, v in m.items()
                        if isinstance(v, (int, float)) and v is not None
                    }
                )
        mlflow.log_dict(split, "split.json")
        mlflow.log_dict(comparison, "liftwing_comparison.json")
        imp = dict(
            zip(FEATURES, gbm.booster_.feature_importance("gain").round(1).tolist(), strict=True)
        )
        mlflow.log_dict(imp, "feature_importance_gain.json")
        mlflow.log_artifacts(str(out), "model")
        return r.info.run_id


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.ml.train")
    p.add_argument("--start", help="first edit time (ISO 8601); default: end - 7 days")
    p.add_argument("--end", "--snapshot-end", dest="end", help="default: now")
    p.add_argument("--label-window-hours", type=float, default=6.0)
    p.add_argument("--no-mlflow", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    import psycopg

    from wikipulse.config import load_settings
    from wikipulse.db.__main__ import upgrade
    from wikipulse.ml.data import load_edits, load_liftwing

    settings = load_settings()
    upgrade(settings.database_url)
    parse = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
    end = parse(args.end) if args.end else datetime.now(UTC)
    start = parse(args.start) if args.start else end - timedelta(days=7)
    window = timedelta(hours=args.label_window_hours)

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        # One extra day before `start` so 24-hour features exist for the first targets.
        edits = load_edits(conn, list(settings.wikis), start - timedelta(days=1), end + window)
        log.info("loaded %d edits", len(edits))
        liftwing = load_liftwing(conn, edits["revision_id"].tolist())
        try:
            delays = time_to_revert(edits, timedelta(days=3)) if not edits.empty else {}
            ds, models, thresholds, results, comparison, split = run(
                edits, liftwing, window, start, end
            )
        except InsufficientData as exc:
            print(f"not training: {exc}")
            return 99  # Airflow's BashOperator treats 99 as "skipped"
        version = datetime.now(UTC).strftime("lgbm-%Y%m%d%H%M")
        params = {
            "start": start.isoformat(), "end": end.isoformat(),
            "label_window_hours": args.label_window_hours,
            "n_targets": len(ds.y), "lightgbm_trees": models["lightgbm"].best_iteration_,
        }  # fmt: skip
        metrics = {
            "test": results,
            "same_edits_as_liftwing": comparison,
            "split": split,
            "time_to_revert": delays,
        }
        model_dir = settings.lake_dir.parent / "models"
        out = save_model(model_dir, version, models["lightgbm"], thresholds["lightgbm"], metrics)
        run_id = None
        if not args.no_mlflow:
            mlflow_dir = (settings.lake_dir.parent / "mlflow").resolve()
            mlflow_dir.mkdir(parents=True, exist_ok=True)
            uri = os.environ.get(
                "MLFLOW_TRACKING_URI", f"sqlite:///{(mlflow_dir / 'mlflow.db').as_posix()}"
            )
            run_id = log_mlflow(
                uri, mlflow_dir / "artifacts", version, params, results, comparison, split,
                models["lightgbm"], out,
            )  # fmt: skip
        with conn.transaction():
            conn.execute("UPDATE model_runs SET is_current = false WHERE is_current")
            conn.execute(
                "INSERT INTO model_runs (version, metrics, params, threshold, mlflow_run_id, "
                "is_current) VALUES (%s, %s, %s, %s, %s, true)",
                (
                    version,
                    json.dumps(metrics, default=str),
                    json.dumps(params, default=str),
                    thresholds["lightgbm"],
                    run_id,
                ),
            )
    print(
        json.dumps(
            {
                "version": version,
                "time_to_revert": delays,
                "test": results,
                "same_edits_as_liftwing": comparison,
                "split": split,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
