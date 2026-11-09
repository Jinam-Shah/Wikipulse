"""Postgres side of the radar: one closed 5-minute window per transaction.

For window t: load the window's page rows (page_metrics_5min) and every open trend,
advance each of those pages with detector.step(), then write baselines, trends, the
trend timeline and the watermark, all in ONE transaction. A crash mid-window rolls back,
and the window is simply processed again.

Event-time watermark: a window is processed only once the stream itself has moved past
it (latest edit ts >= window end + grace), so a backfill (replaying history) produces
the same trends as live running, and partly-filled windows are never judged.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg

from wikipulse.radar.detector import WINDOW, Baseline, Obs, OpenTrend, Params, step

STATE_KEY = "radar"


@dataclass(frozen=True)
class WindowResult:
    window: datetime
    pages: int
    opened: int
    closed: int


def watermark(conn: psycopg.Connection) -> datetime | None:
    row = conn.execute(
        "SELECT last_window FROM radar_state WHERE key = %s", (STATE_KEY,)
    ).fetchone()
    return row[0] if row else None


def next_windows(
    conn: psycopg.Connection, grace: timedelta, limit: int, since: datetime | None = None
) -> list[datetime]:
    """Closed windows not yet processed, oldest first."""
    last = watermark(conn)
    stream_head = conn.execute("SELECT max(ts) FROM edits").fetchone()[0]
    if stream_head is None:
        return []
    first = last + WINDOW if last else since
    if first is None:
        first = conn.execute("SELECT min(window_start) FROM page_metrics_5min").fetchone()[0]
        if first is None:
            return []
    windows = []
    t = first
    while t + WINDOW + grace <= stream_head and len(windows) < limit:
        windows.append(t)
        t += WINDOW
    return windows


def _load(conn: psycopg.Connection, t: datetime):
    obs = {
        (w, pid): Obs(edits, editors, bots, title, reverts)
        for w, pid, title, edits, editors, bots, reverts in conn.execute(
            """
            SELECT wiki, page_id, page_title, edits, distinct_editors, bot_edits, reverts
            FROM page_metrics_5min WHERE window_start = %s
            """,
            (t,),
        ).fetchall()
    }
    trends = {
        (w, pid): OpenTrend(tid, title, s, pk_at, pk_z, pk_e, ed, tot, quiet, last_act)
        for tid, w, pid, title, s, pk_at, pk_z, pk_e, ed, tot, quiet, last_act in conn.execute(
            """
            SELECT trend_id, wiki, page_id, page_title, started_at, peak_at, peak_zscore,
                   peak_edits, distinct_editors, total_edits, quiet_windows, last_active_window
            FROM trends WHERE status = 'open'
            """
        ).fetchall()
    }
    keys = set(obs) | set(trends)
    baselines: dict = {}
    if keys:
        wikis, pids = zip(*keys, strict=True)
        for w, pid, mean, var, last, seen in conn.execute(
            """
            SELECT b.wiki, b.page_id, b.mean, b.var, b.last_window, b.windows_seen
            FROM page_baselines b
            JOIN unnest(%s::text[], %s::bigint[]) AS k(wiki, page_id) USING (wiki, page_id)
            """,
            (list(wikis), list(pids)),
        ).fetchall():
            baselines[(w, pid)] = Baseline(mean, var, last, seen)
    return obs, trends, baselines


def process_window(conn: psycopg.Connection, t: datetime, p: Params) -> WindowResult:
    opened = closed = 0
    with conn.transaction():
        obs, trends, baselines = _load(conn, t)
        keys = sorted(set(obs) | set(trends))
        base_rows = []
        with conn.cursor() as cur:
            for key in keys:
                wiki, page_id = key
                s = step(baselines.get(key, Baseline()), trends.get(key), obs.get(key), t, p)
                b = s.baseline
                base_rows.append((wiki, page_id, b.mean, b.var, b.last_window, b.windows_seen))
                if s.trend is None:
                    continue
                tr = s.trend
                if s.action == "opened":
                    cur.execute(
                        """
                        INSERT INTO trends (wiki, page_id, page_title, status, started_at, peak_at,
                            peak_zscore, peak_edits, distinct_editors, total_edits,
                            quiet_windows, last_active_window, views_status)
                        VALUES (%s, %s, %s, 'open', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING trend_id
                        """,
                        (
                            wiki,
                            page_id,
                            tr.page_title,
                            tr.started_at,
                            tr.peak_at,
                            tr.peak_zscore,
                            tr.peak_edits,
                            tr.distinct_editors,
                            tr.total_edits,
                            tr.quiet_windows,
                            tr.last_active_window,
                            "pending" if pageviews_project(wiki) else "unavailable",
                        ),
                    )
                    trend_id = cur.fetchone()[0]
                    opened += 1
                else:
                    trend_id = tr.trend_id
                    ended = s.action == "closed"
                    closed += ended
                    cur.execute(
                        """
                        UPDATE trends SET page_title = %s, status = %s, peak_at = %s,
                            peak_zscore = %s, peak_edits = %s, distinct_editors = %s,
                            total_edits = %s, quiet_windows = %s, last_active_window = %s,
                            ended_at = %s, updated_at = now()
                        WHERE trend_id = %s
                        """,
                        (
                            tr.page_title,
                            "ended" if ended else "open",
                            tr.peak_at,
                            tr.peak_zscore,
                            tr.peak_edits,
                            tr.distinct_editors,
                            tr.total_edits,
                            tr.quiet_windows,
                            tr.last_active_window,
                            tr.last_active_window + WINDOW if ended else None,
                            trend_id,
                        ),
                    )
                o = obs.get(key)
                cur.execute(
                    """
                    INSERT INTO trend_windows
                        (trend_id, window_start, edits, distinct_editors, zscore)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """,
                    (trend_id, t, s.edits, o.distinct_editors if o else 0, s.zscore),
                )
            if base_rows:
                cur.executemany(
                    """
                    INSERT INTO page_baselines (wiki, page_id, mean, var, last_window, windows_seen)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (wiki, page_id) DO UPDATE SET mean = EXCLUDED.mean,
                        var = EXCLUDED.var, last_window = EXCLUDED.last_window,
                        windows_seen = EXCLUDED.windows_seen
                    """,
                    base_rows,
                )
            cur.execute(
                """
                INSERT INTO radar_state (key, last_window) VALUES (%s, %s)
                ON CONFLICT (key)
                DO UPDATE SET last_window = EXCLUDED.last_window, updated_at = now()
                """,
                (STATE_KEY, t),
            )
    return WindowResult(t, len(keys), opened, closed)


def prune_baselines(conn: psycopg.Connection, older_than: datetime) -> int:
    """Baselines unseen for days have decayed to ~0, the same as a fresh page."""
    with conn.transaction():
        return conn.execute(
            "DELETE FROM page_baselines WHERE last_window < %s", (older_than,)
        ).rowcount


def reset(conn: psycopg.Connection) -> None:
    """Forget all radar state and trends (for --rebuild)."""
    with conn.transaction():
        conn.execute("TRUNCATE trend_windows, trends, page_baselines, radar_state")


def pageviews_project(wiki: str) -> str | None:
    from wikipulse.radar.pageviews import project_for_wiki

    return project_for_wiki(wiki)


def open_trend_count(conn: psycopg.Connection) -> int:
    return conn.execute("SELECT count(*) FROM trends WHERE status = 'open'").fetchone()[0]


def iter_windows(conn: psycopg.Connection, windows: Iterable[datetime], p: Params):
    for t in windows:
        yield process_window(conn, t, p)
