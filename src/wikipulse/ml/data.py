"""Load edits from Postgres into a DataFrame (training and live scoring share this)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import psycopg

COLUMNS = [
    "event_id", "wiki", "change_kind", "page_id", "page_title", "revision_id", "rev_sha1",
    "parent_rev_sha1", "user_name", "is_bot", "is_anon", "user_edit_count",
    "user_registered_at", "comment", "is_minor", "bytes_old", "bytes_new", "ts", "revert_kind",
]  # fmt: skip


def _frame(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=COLUMNS)
    for col in ("ts", "user_registered_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def load_edits(
    conn: psycopg.Connection, wikis: list[str], start: datetime, end: datetime
) -> pd.DataFrame:
    rows = conn.execute(
        f"SELECT {', '.join(COLUMNS)} FROM edits "
        "WHERE wiki = ANY(%s) AND ts >= %s AND ts < %s ORDER BY ts, event_id",
        (wikis, start, end),
    ).fetchall()
    return _frame(rows)


def load_context(
    conn: psycopg.Connection, targets: pd.DataFrame, start: datetime, end: datetime
) -> pd.DataFrame:
    """All edits in [start, end) by the targets' users or on the targets' pages."""
    users = targets["user_name"].dropna().unique().tolist()
    pages = targets["page_id"].unique().tolist()
    wikis = targets["wiki"].unique().tolist()
    rows = conn.execute(
        f"SELECT {', '.join(COLUMNS)} FROM edits "
        "WHERE wiki = ANY(%s) AND ts >= %s AND ts < %s "
        "AND (user_name = ANY(%s) OR page_id = ANY(%s)) ORDER BY ts, event_id",
        (wikis, start, end, users, pages),
    ).fetchall()
    return _frame(rows)


def load_liftwing(conn: psycopg.Connection, revision_ids: list[int]) -> pd.Series:
    rows = conn.execute(
        "SELECT revision_id, prob_revert FROM liftwing_scores "
        "WHERE model_name = 'revertrisk-language-agnostic' AND revision_id = ANY(%s)",
        (revision_ids,),
    ).fetchall()
    return pd.Series({r: p for r, p in rows}, dtype="float64")
