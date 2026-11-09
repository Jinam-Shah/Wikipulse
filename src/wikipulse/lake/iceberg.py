"""Iceberg medallion tables, built hour by hour with PyIceberg (SQL catalog).

    bronze.edits        raw export, as-is, plus load bookkeeping
    silver.edits        deduplicated, typed, with derived columns (user_type, bytes_delta, ...)
    gold.page_hourly    per page per hour
    gold.wiki_hourly    per wiki per hour

Each hour is written with `overwrite(..., overwrite_filter=<that hour>)`, so rebuilding
an hour replaces it instead of appending duplicates. Tables use Iceberg hidden
partitioning by day, so readers filter on the timestamp and Iceberg prunes files.
Transformations are SQL run by DuckDB directly on Arrow tables.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.expressions import And, BooleanExpression, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from wikipulse.lake.export import HOUR, read_hour
from wikipulse.lake.paths import LakePaths
from wikipulse.lake.schemas import (
    BRONZE_EDITS,
    GOLD_PAGE_HOURLY,
    GOLD_WIKI_HOURLY,
    SILVER_EDITS,
)

log = logging.getLogger(__name__)

TABLES: dict[str, tuple[pa.Schema, str]] = {
    # identifier: (schema, timestamp column used for partitioning and hourly overwrite)
    "bronze.edits": (BRONZE_EDITS, "ts"),
    "silver.edits": (SILVER_EDITS, "ts"),
    "gold.page_hourly": (GOLD_PAGE_HOURLY, "hour_start"),
    "gold.wiki_hourly": (GOLD_WIKI_HOURLY, "hour_start"),
}

SILVER_SQL = """
SELECT
    event_id, wiki, page_id, page_title, namespace, change_kind, revision_id,
    parent_revision_id, user_name,
    CASE WHEN is_bot THEN 'bot' WHEN is_anon THEN 'anon' ELSE 'registered' END AS user_type,
    is_bot, is_anon, user_edit_count,
    CASE WHEN user_registered_at IS NULL THEN NULL
         ELSE CAST(greatest(date_diff('day', user_registered_at, ts), 0) AS INTEGER)
    END AS account_age_days,
    comment,
    CAST(coalesce(length(comment), 0) AS INTEGER) AS comment_length,
    is_minor, bytes_old, bytes_new,
    CAST(coalesce(bytes_new, 0) - coalesce(bytes_old, 0) AS BIGINT) AS bytes_delta,
    revert_kind IS NOT NULL AS is_revert,
    revert_kind,
    ts,
    date_trunc('hour', ts) AS event_hour,
    CAST(epoch(ingested_at) - epoch(ts) AS DOUBLE) AS ingest_lag_seconds,
    $loaded_at AS _loaded_at
FROM bronze
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _loaded_at DESC, processed_at DESC) = 1
"""

PAGE_HOURLY_SQL = """
SELECT
    wiki, page_id,
    arg_max(page_title, ts) AS page_title,
    event_hour AS hour_start,
    count(*) AS edits,
    count(DISTINCT coalesce(user_name, 'hidden:' || event_id)) AS distinct_editors,
    count(*) FILTER (WHERE is_bot) AS bot_edits,
    count(*) FILTER (WHERE is_anon) AS anon_edits,
    count(*) FILTER (WHERE is_revert) AS reverts,
    CAST(sum(bytes_delta) AS BIGINT) AS bytes_delta,
    $loaded_at AS _loaded_at
FROM silver
GROUP BY wiki, page_id, event_hour
"""

WIKI_HOURLY_SQL = """
SELECT
    wiki,
    event_hour AS hour_start,
    count(*) AS edits,
    count(*) FILTER (WHERE change_kind = 'create') AS new_pages,
    count(*) FILTER (WHERE is_bot) AS bot_edits,
    count(*) FILTER (WHERE is_anon) AS anon_edits,
    count(*) FILTER (WHERE is_revert) AS reverts,
    count(DISTINCT page_id) AS distinct_pages,
    count(DISTINCT coalesce(user_name, 'hidden:' || event_id)) AS distinct_editors,
    CAST(sum(bytes_delta) AS BIGINT) AS bytes_delta,
    $loaded_at AS _loaded_at
FROM silver
GROUP BY wiki, event_hour
"""


@dataclass(frozen=True)
class HourBuild:
    hour: datetime
    bronze: int
    silver: int
    page_hourly: int
    wiki_hourly: int


def ensure_tables(catalog) -> dict[str, Table]:
    tables = {}
    for identifier, (schema, ts_col) in TABLES.items():
        namespace = identifier.split(".")[0]
        catalog.create_namespace_if_not_exists(namespace)
        table = catalog.create_table_if_not_exists(identifier, schema=schema)
        if not table.spec().fields:
            with table.update_spec() as spec:
                spec.add_field(ts_col, DayTransform(), f"{ts_col}_day")
            table = catalog.load_table(identifier)
        tables[identifier] = table
    return tables


def hour_filter(column: str, hour: datetime) -> BooleanExpression:
    return And(
        GreaterThanOrEqual(column, hour.isoformat()),
        LessThan(column, (hour + HOUR).isoformat()),
    )


def replace_hour(table: Table, column: str, hour: datetime, data: pa.Table) -> None:
    """Make `data` the complete content of `hour` in `table` (one atomic snapshot)."""
    target = table.schema().as_arrow()
    data = data.select(target.names).cast(target)
    with warnings.catch_warnings():
        # The first load of an hour has nothing to replace; PyIceberg warns about that.
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        if data.num_rows:
            table.overwrite(data, overwrite_filter=hour_filter(column, hour))
        else:
            table.delete(delete_filter=hour_filter(column, hour))


def _sql(con: duckdb.DuckDBPyConnection, query: str, loaded_at: datetime, schema: pa.Schema):
    rel = con.sql(query, params={"loaded_at": loaded_at})
    # DuckDB 2.x calls it to_arrow_table(); 1.x calls it arrow(). Support both.
    fetch = getattr(rel, "to_arrow_table", None) or rel.arrow
    return fetch().select(schema.names).cast(schema)


def build_hour(
    tables: dict[str, Table], paths: LakePaths, hour: datetime, loaded_at: datetime | None = None
) -> HourBuild:
    """Rebuild bronze, silver and gold for one hour from its raw Parquet export."""
    loaded_at = loaded_at or datetime.now(UTC)
    raw = read_hour(paths, hour)
    if raw is None:
        raise FileNotFoundError(f"no raw export for {hour:%Y-%m-%d %H}:00; run the export first")

    bronze = raw.append_column(
        pa.field("_export_hour", BRONZE_EDITS.field("_export_hour").type, nullable=False),
        pa.array([hour] * raw.num_rows, type=BRONZE_EDITS.field("_export_hour").type),
    ).append_column(
        pa.field("_loaded_at", BRONZE_EDITS.field("_loaded_at").type, nullable=False),
        pa.array([loaded_at] * raw.num_rows, type=BRONZE_EDITS.field("_loaded_at").type),
    )
    replace_hour(tables["bronze.edits"], "ts", hour, bronze)

    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.register("bronze", bronze)
        silver = _sql(con, SILVER_SQL, loaded_at, SILVER_EDITS)
        con.register("silver", silver)
        page_hourly = _sql(con, PAGE_HOURLY_SQL, loaded_at, GOLD_PAGE_HOURLY)
        wiki_hourly = _sql(con, WIKI_HOURLY_SQL, loaded_at, GOLD_WIKI_HOURLY)
    finally:
        con.close()

    replace_hour(tables["silver.edits"], "ts", hour, silver)
    replace_hour(tables["gold.page_hourly"], "hour_start", hour, page_hourly)
    replace_hour(tables["gold.wiki_hourly"], "hour_start", hour, wiki_hourly)
    return HourBuild(
        hour, bronze.num_rows, silver.num_rows, page_hourly.num_rows, wiki_hourly.num_rows
    )


def expire_old_snapshots(tables: dict[str, Table], keep_days: int = 7) -> None:
    """Drop snapshots older than `keep_days` (time travel works within that window)."""
    cutoff_ms = int((datetime.now(UTC) - timedelta(days=keep_days)).timestamp() * 1000)
    for identifier, table in tables.items():
        try:
            table.maintenance.expire_snapshots().older_than(
                datetime.fromtimestamp(cutoff_ms / 1000, tz=UTC)
            ).commit()
        except Exception as exc:  # maintenance must never break the hourly load
            log.warning("snapshot expiry skipped for %s: %s", identifier, exc)
