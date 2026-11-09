"""Where the lake lives on disk, and the Iceberg catalog that points at it.

Layout under WIKIPULSE_LAKE_DIR (default ./data/lake):
    raw/edits/date=YYYY-MM-DD/hour=HH/edits.parquet   hourly exports (+ _manifest.json)
    warehouse/                                         Iceberg data + metadata files
    catalog.db                                         Iceberg SQL catalog (SQLite)
    analytics.duckdb                                   dbt marts
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

CATALOG_NAME = "wikipulse"


@dataclass(frozen=True)
class LakePaths:
    root: Path

    @classmethod
    def at(cls, root: Path | str) -> LakePaths:
        # Absolute paths everywhere: PyIceberg, SQLite and dbt each resolve relative
        # paths against a different working directory otherwise.
        return cls(Path(root).resolve())

    @property
    def raw_edits(self) -> Path:
        return self.root / "raw" / "edits"

    def raw_hour_dir(self, hour: datetime) -> Path:
        return self.raw_edits / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse"

    @property
    def catalog_db(self) -> Path:
        return self.root / "catalog.db"

    @property
    def catalog_uri(self) -> str:
        # Forward slashes work for SQLAlchemy on Windows too: sqlite:///C:/Projects/...
        return f"sqlite:///{self.catalog_db.as_posix()}"

    @property
    def duckdb(self) -> Path:
        return self.root / "analytics.duckdb"

    def catalog_properties(self) -> dict[str, str]:
        # A plain absolute path (not file://) is what PyIceberg expects for local
        # warehouses, including Windows drive letters.
        return {"type": "sql", "uri": self.catalog_uri, "warehouse": str(self.warehouse)}

    def ensure(self) -> None:
        for d in (self.raw_edits, self.warehouse):
            d.mkdir(parents=True, exist_ok=True)


def load_catalog(paths: LakePaths):
    from pyiceberg.catalog import load_catalog as _load

    paths.ensure()
    return _load(CATALOG_NAME, **paths.catalog_properties())
