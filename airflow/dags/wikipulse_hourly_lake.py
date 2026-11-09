"""Hourly lakehouse load, dbt marts and data-quality checks.

Every hour, for the hours ending at the run's `data_interval_end` (E):

1. export_and_build_lake  - hours [E-3h, E): Postgres -> Parquet -> Iceberg bronze/silver/gold.
   Re-doing the two earlier hours picks up late edits; every step is idempotent.
2. dbt_build              - rebuild marts and run the 40 data tests.
3. dbt_source_freshness   - fail if the Iceberg sources stopped being updated.
4. data_quality_checks    - stream freshness, volume vs recent hours, lake vs Postgres
   reconciliation and revert rate for hour E-2h (old enough to be settled). Results go to
   the `data_quality_checks` table; a failing check fails this task.

All WikiPulse code runs in its own virtualenv (WIKIPULSE_PYTHON) so its dependencies
never clash with Airflow's (docs/DECISIONS.md D-024). Lake-writing tasks share the
one-slot `lake` pool, so two loads never write the Iceberg catalog at once.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PY = "${WIKIPULSE_PYTHON:-python}"
END = "{{ data_interval_end.isoformat() }}"
START = "{{ (data_interval_end - macros.timedelta(hours=3)).isoformat() }}"
CHECK_HOUR = "{{ (data_interval_end - macros.timedelta(hours=2)).isoformat() }}"

with DAG(
    dag_id="wikipulse_hourly_lake",
    description="Postgres -> Parquet -> Iceberg -> dbt, plus data-quality checks",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wikipulse",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["wikipulse", "lakehouse", "phase-4"],
    doc_md=__doc__,
) as dag:
    export_and_build_lake = BashOperator(
        task_id="export_and_build_lake",
        bash_command=f'{PY} -m wikipulse.lake run --start "{START}" --end "{END}"',
        pool="lake",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PY} -m wikipulse.lake dbt build --no-use-colors",
        pool="lake",
    )
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{PY} -m wikipulse.lake dbt source freshness --no-use-colors",
        pool="lake",
        retries=0,
    )
    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=f'{PY} -m wikipulse.quality --hour "{CHECK_HOUR}"',
        retries=0,  # a failed check is a finding, not a flaky task
    )

    export_and_build_lake >> dbt_build >> [dbt_source_freshness, data_quality_checks]
