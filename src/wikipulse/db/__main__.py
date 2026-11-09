"""Schema migrations (Alembic) without needing an alembic.ini.

python -m wikipulse.db upgrade            # apply all migrations (safe to re-run)
python -m wikipulse.db current            # show the applied revision
python -m wikipulse.db downgrade -1
python -m wikipulse.db revision -m "add trends table"   # new empty migration file
"""

from __future__ import annotations

import argparse
from pathlib import Path

from alembic import command
from alembic.config import Config

from wikipulse.config import load_settings
from wikipulse.db import sqlalchemy_url

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def alembic_config(database_url: str | None = None) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    url = database_url or load_settings().database_url
    # "%" must be escaped for configparser (passwords can contain it).
    cfg.set_main_option("sqlalchemy.url", sqlalchemy_url(url).replace("%", "%%"))
    return cfg


def upgrade(database_url: str | None = None, revision: str = "head") -> None:
    command.upgrade(alembic_config(database_url), revision)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.db")
    sub = p.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upgrade")
    up.add_argument("revision", nargs="?", default="head")
    down = sub.add_parser("downgrade")
    down.add_argument("revision")
    sub.add_parser("current")
    rev = sub.add_parser("revision")
    rev.add_argument("-m", "--message", required=True)
    args = p.parse_args(argv)

    cfg = alembic_config()
    if args.cmd == "upgrade":
        command.upgrade(cfg, args.revision)
    elif args.cmd == "downgrade":
        command.downgrade(cfg, args.revision)
    elif args.cmd == "current":
        command.current(cfg, verbose=True)
    elif args.cmd == "revision":
        command.revision(cfg, message=args.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
