"""Fixtures for tests that need the docker compose stack (Redpanda + Postgres)."""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest

ADMIN_URL = os.environ.get(
    "WIKIPULSE_DATABASE_URL", "postgresql://wikipulse:wikipulse@localhost:5432/wikipulse"
)


@pytest.fixture
def fresh_db_url():
    """A brand-new database with all migrations applied; dropped afterwards."""
    psycopg = pytest.importorskip("psycopg")
    from wikipulse.db.__main__ import upgrade

    name = f"wikipulse_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    parts = urlsplit(ADMIN_URL)
    url = urlunsplit(parts._replace(path=f"/{name}"))
    try:
        upgrade(url)
        yield url
    finally:
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
