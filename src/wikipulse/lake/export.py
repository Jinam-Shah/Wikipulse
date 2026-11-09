"""Hourly export: Postgres `edits` -> one Parquet file per hour (the lake's raw zone).

Idempotent: an hour always maps to the same file, written to a temp name and then
atomically replaced, so re-running an hour (for late events) never duplicates data.

Safety: Postgres only keeps `retention_days` of edits. Re-exporting an hour that has
already been pruned would overwrite a good file with an empty one, so such hours are
refused (see `oldest_exportable_hour`).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from wikipulse.lake.paths import LakePaths
from wikipulse.lake.schemas import RAW_COLUMNS, RAW_EDITS

HOUR = timedelta(hours=1)

SELECT_HOUR = f"""
SELECT {", ".join(RAW_COLUMNS)}
FROM edits
WHERE ts >= %(start)s AND ts < %(end)s
ORDER BY ts, event_id
"""


@dataclass(frozen=True)
class HourExport:
    hour: datetime
    rows: int
    path: str


def floor_hour(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def complete_hours(now: datetime, hours_back: int) -> list[datetime]:
    """The last `hours_back` *finished* hours, oldest first (the current hour is excluded)."""
    current = floor_hour(now)
    return [current - HOUR * i for i in range(hours_back, 0, -1)]


def hours_between(start: datetime, end: datetime) -> list[datetime]:
    """Every hour h with start <= h < end (both floored to the hour)."""
    h, stop = floor_hour(start), floor_hour(end)
    out = []
    while h < stop:
        out.append(h)
        h += HOUR
    return out


def oldest_exportable_hour(now: datetime, retention_days: int) -> datetime:
    # One hour of margin so the processor's pruning can't race the export.
    return floor_hour(now - timedelta(days=retention_days)) + HOUR


def fetch_hour(conn: psycopg.Connection, hour: datetime) -> pa.Table:
    with conn.cursor() as cur:
        cur.execute(SELECT_HOUR, {"start": hour, "end": hour + HOUR})
        rows = cur.fetchall()
    columns = list(zip(*rows, strict=True)) if rows else [[] for _ in RAW_COLUMNS]
    return pa.Table.from_arrays(
        [pa.array(col, type=field.type) for col, field in zip(columns, RAW_EDITS, strict=True)],
        schema=RAW_EDITS,
    )


def write_hour(paths: LakePaths, hour: datetime, table: pa.Table) -> HourExport:
    out_dir = paths.raw_hour_dir(hour)
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "edits.parquet"
    tmp = out_dir / "edits.parquet.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, target)  # atomic on Windows and POSIX
    manifest = {
        "hour": hour.isoformat(),
        "rows": table.num_rows,
        "exported_at": datetime.now(UTC).isoformat(),
        "schema_version": 1,
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return HourExport(hour, table.num_rows, str(target))


def read_hour(paths: LakePaths, hour: datetime) -> pa.Table | None:
    target = paths.raw_hour_dir(hour) / "edits.parquet"
    if not target.exists():
        return None
    return pq.read_table(target, schema=RAW_EDITS)


def export_hours(
    conn: psycopg.Connection,
    paths: LakePaths,
    hours: list[datetime],
    *,
    now: datetime,
    retention_days: int,
) -> list[HourExport]:
    oldest = oldest_exportable_hour(now, retention_days)
    current = floor_hour(now)
    results = []
    for hour in hours:
        if hour < oldest:
            raise ValueError(
                f"{hour:%Y-%m-%d %H}:00 is older than Postgres retention "
                f"({retention_days} days); refusing to overwrite its export"
            )
        if hour >= current:
            raise ValueError(f"{hour:%Y-%m-%d %H}:00 has not finished yet")
        results.append(write_hour(paths, hour, fetch_hour(conn, hour)))
    return results
