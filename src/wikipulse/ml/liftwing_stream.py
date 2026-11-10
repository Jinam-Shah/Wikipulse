"""Ingest Wikimedia's own revert-risk scores (Lift Wing) into `liftwing_scores`.

    python -m wikipulse.ml.liftwing_stream                          # run forever
    python -m wikipulse.ml.liftwing_stream --since 2026-11-06T00:00:00Z --max-seconds 1200

Source: EventStreams `mediawiki.page_revert_risk_prediction_change.v1`, which carries the
`revertrisk-language-agnostic` prediction for each new revision. Using the stream instead
of calling the Lift Wing API per edit means no rate limits and full coverage
(docs/DECISIONS.md D-033). Same reconnect/resume machinery as the producer; the
checkpoint is saved only after rows are committed (at-least-once, upserts make
repeats harmless).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import psycopg

from wikipulse.producer.checkpoint import FileCheckpoint
from wikipulse.wikimedia.sse import SSEClient

log = logging.getLogger("wikipulse.liftwing")
STREAM = "https://stream.wikimedia.org/v2/stream/mediawiki.page_revert_risk_prediction_change.v1"

UPSERT = """
INSERT INTO liftwing_scores (wiki, revision_id, model_name, model_version, prob_revert, event_dt)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (wiki, revision_id, model_name) DO UPDATE
SET model_version = EXCLUDED.model_version, prob_revert = EXCLUDED.prob_revert,
    event_dt = EXCLUDED.event_dt
"""


def parse(raw: str, wikis: frozenset[str]) -> tuple | None:
    """One SSE payload -> a row, or None if it's not usable / not a tracked wiki."""
    try:
        doc = json.loads(raw)
        if doc.get("wiki_id") not in wikis or doc.get("meta", {}).get("domain") == "canary":
            return None
        pred = doc["predicted_classification"]
        return (
            doc["wiki_id"],
            int(doc["revision"]["rev_id"]),
            str(pred["model_name"]),
            str(pred["model_version"]),
            float(pred["probabilities"]["true"]),
            datetime.fromisoformat(doc["meta"]["dt"].replace("Z", "+00:00")),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.ml.liftwing_stream")
    p.add_argument("--since", help="start time (ISO 8601) when there is no checkpoint")
    p.add_argument("--max-seconds", type=float)
    p.add_argument("--reset-checkpoint", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel("WARNING")

    from wikipulse.config import load_settings
    from wikipulse.db.__main__ import upgrade

    settings = load_settings()
    upgrade(settings.database_url)
    checkpoint = FileCheckpoint(Path(settings.checkpoint_path).with_name("liftwing.json"), STREAM)
    resume = None if args.reset_checkpoint else checkpoint.load()
    params = {"since": args.since} if args.since and not resume else {}
    source = SSEClient(STREAM, settings.user_agent, last_event_id=resume, params=params)
    wikis = frozenset(settings.wikis)

    rows, last_id, total, read = [], None, 0, 0
    started = last_flush = time.monotonic()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:

        def flush():
            nonlocal rows, total
            if rows:
                with conn.transaction():
                    conn.cursor().executemany(UPSERT, rows)
                total += len(rows)
                rows = []
            checkpoint.save(last_id)

        for ev in source.events():
            read += 1
            row = parse(ev.data, wikis)
            if row:
                rows.append(row)
            last_id = ev.id or last_id
            now = time.monotonic()
            if len(rows) >= 500 or now - last_flush >= 2:
                flush()
                last_flush = now
            if args.max_seconds and now - started >= args.max_seconds:
                break
        flush()
    log.info("read=%d stored=%d in %.0fs", read, total, time.monotonic() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
