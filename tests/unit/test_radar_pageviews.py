from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from wikipulse.radar.pageviews import (
    PageviewsClient,
    RateLimitedError,
    judge,
    project_for_wiki,
)

H = timedelta(hours=1)
START = datetime(2026, 11, 9, 12, 25, tzinfo=UTC)  # trend starts 12:25 -> start hour 12:00
SH = START.replace(minute=0)


@pytest.mark.parametrize(
    ("wiki", "project"),
    [
        ("enwiki", "en.wikipedia"),
        ("dewiki", "de.wikipedia"),
        ("zh_yuewiki", "zh-yue.wikipedia"),
        ("commonswiki", None),
        ("wikidatawiki", None),
        ("samplewiki", None),
        ("enwiktionary", None),
    ],
)
def test_project_for_wiki(wiki, project):
    assert project_for_wiki(wiki) == project


def history(level=100):
    return {SH - H * i: level for i in range(1, 72)}


def test_confirmed_when_views_jump():
    views = history(100) | {SH: 120, SH + H: 900}
    v = judge(views, START, now=SH + 3 * H)
    assert v.status == "confirmed" and v.peak == 900 and v.ratio == 9.0


def test_pending_while_data_may_still_arrive():
    v = judge(history(100), START, now=SH + 2 * H)
    assert v.status == "pending"


def test_not_confirmed_once_the_window_is_old_enough():
    views = history(100) | {SH: 130, SH + H: 140}
    v = judge(views, START, now=SH + 10 * H)
    assert v.status == "not_confirmed" and v.ratio == 1.4


def test_small_pages_need_a_minimum_number_of_views():
    views = history(2) | {SH: 60}  # 30x, but only 60 views
    assert judge(views, START, now=SH + 10 * H).status == "not_confirmed"


def test_missing_hours_count_as_zero_views():
    views = {SH: 400}  # never viewed before -> baseline 0 -> ratio 400
    assert judge(views, START, now=SH + 10 * H).status == "confirmed"


def make_client(handler):
    sleeps: list[float] = []
    c = PageviewsClient(
        "WikiPulse-test/0 (https://example.org)",
        httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=sleeps.append,
        clock=lambda: 0.0,
    )
    return c, sleeps


def test_client_builds_the_documented_url_and_parses_items():
    seen = []

    def handler(req: httpx.Request):
        seen.append(req)
        return httpx.Response(
            200,
            json={
                "items": [
                    {"timestamp": "2026110912", "views": 5},
                    {"timestamp": "2026110913", "views": 7},
                ]
            },
        )

    c, _ = make_client(handler)
    out = c.hourly("en.wikipedia", "AC/DC (band)", SH, SH + H)
    assert out == {SH: 5, SH + H: 7}
    # "/" inside a title must be encoded, or the API would read it as a path separator.
    assert (
        seen[0]
        .url.raw_path.decode()
        .endswith(
            "/per-article/en.wikipedia/all-access/user/AC%2FDC_%28band%29/hourly/2026110912/2026110913"
        )
    )
    assert seen[0].headers["User-Agent"].startswith("WikiPulse-test")


def test_client_honours_retry_after_and_treats_404_as_no_views():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return (
            httpx.Response(429, headers={"Retry-After": "7"})
            if calls["n"] == 1
            else httpx.Response(404)
        )

    c, sleeps = make_client(handler)
    assert c.hourly("en.wikipedia", "X", SH, SH) == {}
    assert 7.0 in sleeps


def test_client_gives_up_after_repeated_429():
    c, _ = make_client(lambda req: httpx.Response(429, headers={"Retry-After": "1"}))
    with pytest.raises(RateLimitedError):
        c.hourly("en.wikipedia", "X", SH, SH)
