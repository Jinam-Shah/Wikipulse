"""A small Server-Sent Events client for Wikimedia EventStreams.

Why not a library: the protocol is tiny, and owning it lets us test reconnects,
Retry-After handling and Last-Event-ID resume precisely (see docs/DECISIONS.md D-008).

Spec: https://html.spec.whatwg.org/multipage/server-sent-events.html
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEEvent:
    data: str
    event: str = "message"
    id: str | None = None
    retry_ms: int | None = None


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Turn raw SSE lines (without newlines) into events.

    - Lines starting with ':' are comments (keep-alives) and are ignored.
    - Multiple 'data:' lines are joined with '\\n'.
    - A blank line dispatches the event. Events with no data are dropped.
    """
    data: list[str] = []
    event = "message"
    event_id: str | None = None
    retry: int | None = None

    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield SSEEvent(data="\n".join(data), event=event, id=event_id, retry_ms=retry)
            data, event, event_id, retry = [], "message", None, None
            continue
        if line.startswith(":"):
            continue
        name, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if name == "data":
            data.append(value)
        elif name == "event":
            event = value or "message"
        elif name == "id":
            if "\0" not in value:
                event_id = value
        elif name == "retry" and value.isdigit():
            retry = int(value)
    # An incomplete trailing event (no blank line) is discarded, as the spec requires.


class StreamUnavailable(Exception):
    """The server answered, but not with a usable stream (4xx/5xx)."""

    def __init__(self, status: int, retry_after: float | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if value and value.strip().isdigit():
        return float(value.strip())
    return None


class SSEClient:
    """Reads an EventStreams URL forever, reconnecting and resuming as needed.

    `last_event_id` is updated as events are read, so a reconnect inside one process
    resumes exactly where it left off. Persisting it across restarts is the caller's job
    (the producer saves it only after Kafka confirms delivery).
    """

    def __init__(
        self,
        url: str,
        user_agent: str,
        *,
        last_event_id: str | None = None,
        params: dict[str, str] | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
        read_timeout_s: float = 60.0,
        max_reconnects: int | None = None,
    ):
        self.url = url
        self.user_agent = user_agent
        self.last_event_id = last_event_id
        self.params = params or {}
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(10.0, read=read_timeout_s), follow_redirects=True
        )
        self._sleep = sleep
        self._initial_backoff = initial_backoff_s
        self._max_backoff = max_backoff_s
        self._max_reconnects = max_reconnects
        self.reconnects = 0
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": self.user_agent, "Accept": "text/event-stream"}
        if self.last_event_id:
            headers["Last-Event-ID"] = self.last_event_id
        return headers

    def _request_params(self) -> dict[str, str]:
        # `since` only applies to the very first connection; after that we resume by ID.
        if self.last_event_id:
            return {k: v for k, v in self.params.items() if k != "since"}
        return self.params

    def events(self) -> Iterator[SSEEvent]:
        backoff = self._initial_backoff
        while not self._stopped:
            wait: float | None = None
            try:
                with self._client.stream(
                    "GET", self.url, headers=self._headers(), params=self._request_params()
                ) as resp:
                    if resp.status_code != 200:
                        raise StreamUnavailable(resp.status_code, _retry_after_seconds(resp))
                    log.info("connected to %s (resume=%s)", self.url, bool(self.last_event_id))
                    for ev in parse_sse(resp.iter_lines()):
                        if ev.id:
                            self.last_event_id = ev.id
                        backoff = self._initial_backoff  # healthy stream: reset backoff
                        yield ev
                        if self._stopped:
                            return
                log.info("stream closed by server; reconnecting")
            except StreamUnavailable as exc:
                log.warning("stream unavailable: %s", exc)
                wait = exc.retry_after
                if exc.status in (400, 401, 403, 404):
                    raise  # configuration problem: retrying won't help
            except httpx.HTTPError as exc:
                log.warning("stream connection error: %s: %s", type(exc).__name__, exc)

            self.reconnects += 1
            if self._max_reconnects is not None and self.reconnects > self._max_reconnects:
                raise ConnectionError(f"gave up after {self._max_reconnects} reconnects")
            delay = wait if wait is not None else backoff * (0.5 + random.random() / 2)
            log.info("reconnect #%d in %.1fs", self.reconnects, delay)
            self._sleep(delay)
            backoff = min(backoff * 2, self._max_backoff)
