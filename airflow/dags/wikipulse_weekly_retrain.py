"""Weekly model retraining (Sunday 03:17 UTC).

1. snapshot_training_data - the last 7 days of silver edits -> one Parquet file, with a
   manifest naming the Iceberg snapshot it came from (reproducible training data).
2. train_model            - runs `python -m wikipulse.ml.train` once the vandalism model
   exists (Phase 6). Until then the task is *skipped* (exit code 99), not failed.
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
  echo "wikipulse.ml.train not available yet (arrives in Phase 6); skipping."
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
    )
    train_model = BashOperator(task_id="train_model", bash_command=TRAIN, skip_on_exit_code=99)

    snapshot_training_data >> train_model
