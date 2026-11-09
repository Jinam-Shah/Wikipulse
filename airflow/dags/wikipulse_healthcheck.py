"""Phase 0 placeholder DAG: proves the scheduler picks up DAGs from ./airflow/dags.

Real DAGs (hourly exports, dbt runs, retraining) arrive in Phase 4.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="wikipulse_healthcheck",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["wikipulse", "phase-0"],
)
def wikipulse_healthcheck():
    @task
    def check_redpanda_reachable() -> str:
        host, port = os.environ.get("WIKIPULSE_KAFKA_BOOTSTRAP", "redpanda:9092").split(":")
        with socket.create_connection((host, int(port)), timeout=5):
            pass
        return f"Redpanda reachable at {host}:{port}"

    check_redpanda_reachable()


wikipulse_healthcheck()
