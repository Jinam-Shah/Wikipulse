"""Synthetic edits for tests, CI and demos - clearly separate from real data.

    python -m wikipulse.devtools.sample --hours 6

Writes to the wiki id `samplewiki` (never `enwiki`), straight into Postgres through the
same store the stream processor uses, so window metrics are filled exactly as in
production. Deterministic for a given --seed and end hour.

Shape: Zipf-like page popularity, ~45 % bot / ~6 % anonymous edits (close to the enwiki
shares measured in Phase 1), a few exact reverts (same content hash) and summary
reverts, a handful of page creations, and one burst on a single page.
"""

from __future__ import annotations

import argparse
import random
from datetime import UTC, datetime, timedelta

from wikipulse.lake.export import floor_hour
from wikipulse.producer.schemas import EditEvent

SAMPLE_WIKI = "samplewiki"


def generate(
    hours: int, end: datetime, *, edits_per_hour: int = 300, seed: int = 7
) -> list[EditEvent]:
    rng = random.Random(seed)
    end = floor_hour(end)
    start = end - timedelta(hours=hours)
    pages = list(range(1, 201))
    weights = [1 / (rank**1.1) for rank in pages]
    registered = [f"SampleEditor{i}" for i in range(1, 61)]
    bots = [f"SampleBot{i}" for i in range(1, 4)]
    page_state: dict[int, list[str]] = {}  # page -> history of content hashes
    history_anon: dict[int, bool] = {}  # page -> was the latest edit anonymous?
    sizes: dict[int, int] = {}
    burst_hour = start + timedelta(hours=hours // 2)
    events: list[EditEvent] = []
    n = 0

    for h in range(hours):
        hour = start + timedelta(hours=h)
        count = edits_per_hour + (120 if hour == burst_hour else 0)
        for k in range(count):
            n += 1
            in_burst = hour == burst_hour and k >= edits_per_hour
            page = 7 if in_burst else rng.choices(pages, weights)[0]
            ts = hour + timedelta(seconds=rng.uniform(0, 3599.999))
            roll = rng.random()
            if roll < 0.45:
                user, user_id, is_bot, is_anon = rng.choice(bots), 9000, True, False
            elif roll < 0.51:
                user, user_id, is_bot, is_anon = (
                    f"~2026-{rng.randint(10000, 99999)}-1",
                    0,
                    False,
                    True,
                )
            else:
                idx = rng.randrange(len(registered))
                user, user_id, is_bot, is_anon = registered[idx], 100 + idx, False, False

            history = page_state.setdefault(page, [])
            created = not history and rng.random() < 0.05
            old_size = None if created else sizes.get(page, rng.randint(2_000, 60_000))
            comment = rng.choice(["copyedit", "added source", "update", "fix typo", "", "expand"])
            revert_p = 0.12 if (len(history) >= 2 and history_anon.get(page)) else 0.03
            if len(history) >= 2 and rng.random() < revert_p:
                sha1 = history[-2]  # exact revert to the previous version
                comment = f"Reverted edits by {user} to last version"
                new_size = old_size - rng.randint(-50, 400) if old_size else 1000
            else:
                sha1 = f"s{n:08d}"
                new_size = max((old_size or 0) + rng.randint(-300, 900), 50)
                if rng.random() < 0.01:
                    comment = "rv vandalism"
            parent_sha1 = history[-1] if history else None
            history.append(sha1)
            history_anon[page] = is_anon
            sizes[page] = new_size
            iso = ts.isoformat()
            events.append(
                EditEvent(
                    event_id=f"sample-{seed}-{end:%Y%m%d%H}-{n}",
                    wiki=SAMPLE_WIKI,
                    change_kind="create" if created else "edit",
                    page_id=page,
                    page_title=f"Sample page {page}",
                    namespace=0,
                    revision_id=10_000_000 + n,
                    parent_revision_id=None if created else 10_000_000 + n - 1,
                    rev_sha1=sha1,
                    parent_rev_sha1=parent_sha1,
                    user_name=user,
                    user_id=user_id or None,
                    is_bot=is_bot,
                    is_anon=is_anon,
                    user_edit_count=None if is_anon else rng.randint(1, 50_000),
                    user_registered_at=None
                    if is_anon
                    else ts - timedelta(days=rng.randint(1, 4000)),
                    comment=comment or None,
                    is_minor=rng.random() < 0.2,
                    bytes_old=old_size,
                    bytes_new=new_size,
                    ts=iso,
                    event_dt=(ts + timedelta(seconds=rng.uniform(0.5, 4))).isoformat(),
                    ingested_at=(ts + timedelta(seconds=rng.uniform(1, 6))).isoformat(),
                )
            )
    events.sort(key=lambda e: e.ts)
    return events


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.devtools.sample")
    p.add_argument("--hours", type=int, default=6, help="how many finished hours to fill")
    p.add_argument("--edits-per-hour", type=int, default=300)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args(argv)

    from wikipulse.config import load_settings
    from wikipulse.db import connect
    from wikipulse.db.__main__ import upgrade
    from wikipulse.processor.store import PostgresStore

    settings = load_settings()
    upgrade(settings.database_url)
    events = generate(
        args.hours, datetime.now(UTC), edits_per_hour=args.edits_per_hour, seed=args.seed
    )
    store = PostgresStore(connect(settings.database_url, autocommit=True))
    cutoff = datetime.now(UTC) - timedelta(days=settings.retention_days)
    inserted = 0
    for i in range(0, len(events), 1000):
        inserted += store.write_batch(events[i : i + 1000], cutoff=cutoff).inserted
    print(
        f"{inserted} synthetic edits for wiki '{SAMPLE_WIKI}' "
        f"({len(events) - inserted} already present) covering the last {args.hours} hours"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
