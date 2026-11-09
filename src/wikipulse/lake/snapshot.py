"""Weekly training snapshot: the last N days of silver edits as one Parquet file.

Written to <lake>/training/edits_<YYYY-MM-DD>.parquet (the date the window ends), with a
manifest recording the Iceberg snapshot id it was read from, so a model trained on it
can always be traced back to exact table data (Iceberg time travel).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta

import pyarrow.parquet as pq
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from wikipulse.lake.paths import LakePaths, load_catalog


@dataclass(frozen=True)
class Snapshot:
    path: str
    rows: int
    iceberg_snapshot_id: int | None


def training_snapshot(paths: LakePaths, end: datetime, days: int = 7) -> Snapshot:
    table = load_catalog(paths).load_table("silver.edits")
    start = end - timedelta(days=days)
    current = table.current_snapshot()
    data = table.scan(
        row_filter=And(GreaterThanOrEqual("ts", start.isoformat()), LessThan("ts", end.isoformat()))
    ).to_arrow()
    out_dir = paths.root / "training"
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"edits_{end:%Y-%m-%d}.parquet"
    tmp = target.with_suffix(".parquet.tmp")
    pq.write_table(data, tmp, compression="zstd")
    os.replace(tmp, target)
    manifest = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "rows": data.num_rows,
        "iceberg_table": "silver.edits",
        "iceberg_snapshot_id": current.snapshot_id if current else None,
    }
    target.with_suffix(".json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return Snapshot(str(target), data.num_rows, manifest["iceberg_snapshot_id"])
