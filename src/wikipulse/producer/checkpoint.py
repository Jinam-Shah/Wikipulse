"""Persists the last EventStreams event ID that Kafka has confirmed.

Saved only after a successful producer flush, so after a crash we resume from an
event that is definitely in Kafka. Some events after it may be sent twice
(at-least-once); downstream upserts keyed on event_id make that harmless.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path


class FileCheckpoint:
    def __init__(self, path: Path, stream_url: str):
        self.path = Path(path)
        self.stream_url = stream_url

    def load(self) -> str | None:
        try:
            doc = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError):
            return None  # corrupt checkpoint: start from "now" rather than crash
        if doc.get("stream_url") != self.stream_url:
            return None  # IDs from a different stream are meaningless here
        return doc.get("last_event_id") or None

    def save(self, last_event_id: str | None) -> None:
        if not last_event_id:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        doc = {
            "stream_url": self.stream_url,
            "last_event_id": last_event_id,
            "saved_at": datetime.now(UTC).isoformat(),
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(doc), encoding="utf-8")
        os.replace(tmp, self.path)  # atomic on Windows and POSIX
