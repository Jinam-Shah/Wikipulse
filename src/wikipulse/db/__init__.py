"""Database helpers: one URL format in .env, adapted for each library."""

from __future__ import annotations

import psycopg


def sqlalchemy_url(url: str) -> str:
    """Alembic/SQLAlchemy need the driver in the scheme: postgresql+psycopg://..."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def connect(url: str, **kwargs) -> psycopg.Connection:
    """A plain psycopg connection (the stream processor's hot path uses raw SQL)."""
    return psycopg.connect(url, **kwargs)
