"""Weekly model retraining (Sunday 03:17 UTC).

1. snapshot_training_data - the last 7 days of silver edits -> one Parquet file, with a
   manifest naming the Iceberg snapshot it came from (record of the lake at training
   time). Skipped if the lake hasn't been built yet.
2. train_model            - `python -m wikipulse.ml.train` on the last 7 days of edits in
   Postgres (time-based split, Lift Wing comparison, MLflow, new current model; the live
   scorer picks it up within 5 minutes). Skipped (exit code 99), not failed, when there
   isn't enough labelled data yet, e.g. right after a fresh install.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PY = "${WIKIPULSE_PYTHON:-python}"
END = "{{ data_interval_end.isoformat() }}"

TRAIN = f"""
if {PY} -c "import wikipulse.ml.train" 2>/dev/null; then
  exec {PY} -m wikipulse.ml.train --snapshot-end "{END}"
else
  echo "wikipulse.ml.train not installed; skipping."
  exit 99
fi
"""

with DAG(
    dag_id="wikipulse_weekly_retrain",
    description="Weekly training snapshot + vandalism model retraining",
    schedule="17 3 * * 0",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "wikipulse", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["wikipulse", "ml", "phase-4"],
    doc_md=__doc__,
) as dag:
    snapshot_training_data = BashOperator(
        task_id="snapshot_training_data",
        bash_command=f'{PY} -m wikipulse.lake snapshot --days 7 --end "{END}"',
        skip_on_exit_code=99,
    )
    # Training reads Postgres, not the snapshot, so it runs unless the snapshot FAILED.
    train_model = BashOperator(
        task_id="train_model",
        bash_command=TRAIN,
        skip_on_exit_code=99,
        trigger_rule="none_failed",
        execution_timeout=timedelta(hours=1),
    )

    snapshot_training_data >> train_model
