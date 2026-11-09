"""Checks that the local docker compose stack is up. Standard library only.

Usage:
    python scripts/smoke_test.py            # core services
    python scripts/smoke_test.py --airflow  # core services + Airflow
Exit code 0 means every check passed.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import urllib.request


def tcp(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=3):
        pass


def http(url: str, expect: str | None = None) -> None:
    with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - local URLs only
        body = resp.read().decode("utf-8", "replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        if expect and expect not in body:
            raise RuntimeError(f"response did not contain {expect!r}: {body[:200]}")


def load_dotenv(path: str = ".env") -> None:
    """Minimal .env reader so ports match docker compose. Real env vars win."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def port(name: str, default: int) -> int:
    return int(os.environ.get(name) or default)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--airflow", action="store_true", help="also check Airflow")
    p.add_argument("--wait", type=int, default=90, help="seconds to keep retrying")
    args = p.parse_args()
    load_dotenv()
    kafka = port("REDPANDA_KAFKA_PORT", 19092)
    admin = port("REDPANDA_ADMIN_PORT", 19644)
    console = port("REDPANDA_CONSOLE_PORT", 8088)
    pg = port("POSTGRES_PORT", 5432)
    airflow = port("AIRFLOW_PORT", 8081)

    checks = {
        f"Redpanda Kafka API (localhost:{kafka})": lambda: tcp("localhost", kafka),
        "Redpanda Admin API ready": lambda: http(
            f"http://localhost:{admin}/v1/status/ready", expect="ready"
        ),
        f"Redpanda Console (localhost:{console})": lambda: http(f"http://localhost:{console}/"),
        f"PostgreSQL (localhost:{pg})": lambda: tcp("localhost", pg),
    }
    if args.airflow:
        checks[f"Airflow API server (localhost:{airflow})"] = lambda: http(
            f"http://localhost:{airflow}/api/v2/monitor/health", expect="healthy"
        )

    deadline = time.monotonic() + args.wait
    pending = dict(checks)
    errors: dict[str, str] = {}
    while pending and time.monotonic() < deadline:
        for name, check in list(pending.items()):
            try:
                check()
                print(f"PASS  {name}")
                del pending[name]
            except Exception as exc:  # keep retrying until the deadline
                errors[name] = str(exc)
        if pending:
            time.sleep(3)

    for name in pending:
        print(f"FAIL  {name}: {errors.get(name)}")
    return 1 if pending else 0


if __name__ == "__main__":
    sys.exit(main())
