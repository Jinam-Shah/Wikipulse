from __future__ import annotations

import httpx
import pytest

from wikipulse.wikimedia.sse import SSEClient, SSEEvent, StreamUnavailable, parse_sse


def lines(text: str) -> list[str]:
    return text.split("\n")


def test_parses_id_event_and_data():
    events = list(parse_sse(lines('event: message\nid: [{"t":1}]\ndata: {"a": 1}\n\n')))
    assert events == [SSEEvent(data='{"a": 1}', event="message", id='[{"t":1}]')]


def test_ignores_comments_and_joins_multiline_data():
    text = ":ok\n\ndata: line1\ndata: line2\n\n"
    assert [e.data for e in parse_sse(lines(text))] == ["line1\nline2"]


def test_strips_only_one_leading_space_and_handles_crlf():
    events = list(parse_sse(["data:  two spaces\r", "\r"]))
    assert events[0].data == " two spaces"


def test_retry_field_and_incomplete_trailing_event_dropped():
    events = list(parse_sse(lines("retry: 5000\ndata: x\n\ndata: never-dispatched")))
    assert len(events) == 1 and events[0].retry_ms == 5000


def test_event_without_data_is_not_dispatched():
    assert list(parse_sse(lines("id: 7\n\n"))) == []


# ---------------------------------------------------------------- client behaviour


def sse_body(*pairs: tuple[str, str]) -> bytes:
    return "".join(f"id: {i}\ndata: {d}\n\n" for i, d in pairs).encode()


def make_client(handler, **kwargs) -> tuple[SSEClient, list[float]]:
    sleeps: list[float] = []
    client = SSEClient(
        "https://stream.test/v2/stream/x",
        "WikiPulse-test/0 (https://example.org)",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        **kwargs,
    )
    return client, sleeps


def take(it, n):
    out = []
    for ev in it:
        out.append(ev)
        if len(out) == n:
            break
    return out


def test_sends_user_agent_and_resumes_with_last_event_id_after_disconnect():
    seen_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers)
        if len(seen_headers) == 1:
            return httpx.Response(200, content=sse_body(("id-1", "a"), ("id-2", "b")))
        return httpx.Response(200, content=sse_body(("id-3", "c")))

    client, sleeps = make_client(handler)
    got = take(client.events(), 3)

    assert [e.data for e in got] == ["a", "b", "c"]
    assert seen_headers[0]["User-Agent"].startswith("WikiPulse-test/0")
    assert "Last-Event-ID" not in seen_headers[0]
    assert seen_headers[1]["Last-Event-ID"] == "id-2"
    assert client.reconnects == 1 and len(sleeps) == 1


def test_initial_last_event_id_is_sent_and_since_is_dropped_when_resuming():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=sse_body(("id-9", "z")))

    client, _ = make_client(handler, last_event_id="id-8", params={"since": "2026-01-01"})
    take(client.events(), 1)
    assert requests[0].headers["Last-Event-ID"] == "id-8"
    assert "since" not in requests[0].url.params


def test_since_is_sent_on_fresh_start():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=sse_body(("id-1", "z")))

    client, _ = make_client(handler, params={"since": "2026-01-01T00:00:00Z"})
    take(client.events(), 1)
    assert requests[0].url.params["since"] == "2026-01-01T00:00:00Z"


def test_honours_retry_after_on_429_then_recovers():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, content=sse_body(("id-1", "ok")))

    client, sleeps = make_client(handler)
    assert [e.data for e in take(client.events(), 1)] == ["ok"]
    assert sleeps == [7.0]


def test_exponential_backoff_on_connection_errors_is_capped():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    client, sleeps = make_client(handler, initial_backoff_s=1, max_backoff_s=4, max_reconnects=5)
    with pytest.raises(ConnectionError):
        list(client.events())
    # jitter keeps each delay within [backoff/2, backoff]
    caps = [1, 2, 4, 4, 4]
    assert len(sleeps) == 5
    assert all(cap / 2 <= s <= cap for s, cap in zip(sleeps, caps, strict=True))


def test_404_is_not_retried():
    client, sleeps = make_client(lambda r: httpx.Response(404))
    with pytest.raises(StreamUnavailable):
        list(client.events())
    assert sleeps == []
