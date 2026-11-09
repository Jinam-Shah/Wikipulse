"""Confirm trends with Wikimedia's Pageviews API (is the public actually reading it?).

Rule (docs/DECISIONS.md D-030): a trend is *confirmed* when, in the 4 hours from its
start hour, some hour has at least `min_ratio` x the page's median hourly views over the
previous 71 hours, and at least `min_peak` views. Hours missing from the API response
mean 0 views. Hourly data is published with a delay (measured at about 2 h for the
hourly dumps on 2026-11-09), so a trend stays *pending* until either it is confirmed or
the whole window is old enough for its data to exist, then it is *not_confirmed*.

API etiquette: descriptive User-Agent, one request at a time, at most `max_per_minute`
requests (well under the 200/min limit), honour Retry-After on 429, and cache every
hour fetched in `page_views_hourly`.
"""

from __future__ import annotations

import logging
import re
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import httpx

log = logging.getLogger(__name__)

API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
HOUR = timedelta(hours=1)
# Wikis whose ids end in "wiki" but aren't language Wikipedias.
NOT_WIKIPEDIA = {
    "commonswiki",
    "wikidatawiki",
    "metawiki",
    "specieswiki",
    "mediawikiwiki",
    "sourceswiki",
    "incubatorwiki",
    "testwiki",
    "test2wiki",
    "samplewiki",
    "wikifunctionswiki",
}


def project_for_wiki(wiki: str) -> str | None:
    """'enwiki' -> 'en.wikipedia'; None for wikis the Pageviews API can't serve."""
    if wiki in NOT_WIKIPEDIA:
        return None
    m = re.fullmatch(r"([a-z][a-z0-9_]*)wiki", wiki)
    return f"{m.group(1).replace('_', '-')}.wikipedia" if m else None


def floor_hour(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class Verdict:
    status: str  # confirmed | not_confirmed | pending
    peak: int | None
    baseline: float | None
    ratio: float | None


@dataclass(frozen=True)
class Rule:
    baseline_hours: int = 71
    window_hours: int = 4
    min_ratio: float = 3.0
    min_peak: int = 200
    data_delay: timedelta = timedelta(hours=4)


DEFAULT_RULE = Rule()


def judge(
    views: dict[datetime, int], started_at: datetime, now: datetime, rule: Rule = DEFAULT_RULE
) -> Verdict:
    start_hour = floor_hour(started_at)
    base_hours = [start_hour - HOUR * i for i in range(rule.baseline_hours, 0, -1)]
    window = [start_hour + HOUR * i for i in range(rule.window_hours)]
    baseline = float(statistics.median(views.get(h, 0) for h in base_hours))
    available = [h for h in window if now >= h + HOUR + rule.data_delay or h in views]
    peak = max((views.get(h, 0) for h in available), default=0)
    ratio = peak / max(baseline, 1.0)
    if available and peak >= rule.min_peak and ratio >= rule.min_ratio:
        return Verdict("confirmed", peak, baseline, round(ratio, 2))
    if now >= window[-1] + HOUR + rule.data_delay:
        return Verdict("not_confirmed", peak, baseline, round(ratio, 2))
    return Verdict("pending", peak if available else None, baseline, round(ratio, 2))


class RateLimitedError(Exception):
    pass


class PageviewsClient:
    def __init__(
        self,
        user_agent: str,
        client: httpx.Client | None = None,
        *,
        max_per_minute: int = 60,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client or httpx.Client(timeout=20.0)
        self.headers = {"User-Agent": user_agent, "Accept": "application/json"}
        self.min_interval = 60.0 / max_per_minute
        self.sleep = sleep
        self.clock = clock
        self._last = float("-inf")
        self.requests = 0

    def hourly(
        self, project: str, title: str, start: datetime, end: datetime
    ) -> dict[datetime, int]:
        """Views per hour for [start, end] (inclusive hours). Missing hours mean 0."""
        wait = self._last + self.min_interval - self.clock()
        if wait > 0:
            self.sleep(wait)
        article = quote(title.replace(" ", "_"), safe="")
        url = f"{API}/{project}/all-access/user/{article}/hourly/{start:%Y%m%d%H}/{end:%Y%m%d%H}"
        for _attempt in range(3):
            self._last = self.clock()
            self.requests += 1
            resp = self.client.get(url, headers=self.headers)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "30") or 30)
                log.warning("pageviews 429; waiting %.0fs", retry_after)
                self.sleep(retry_after)
                continue
            if resp.status_code == 404:  # no views recorded in the range
                return {}
            resp.raise_for_status()
            out = {}
            for item in resp.json().get("items", []):
                hour = datetime.strptime(item["timestamp"], "%Y%m%d%H").replace(tzinfo=UTC)
                out[hour] = int(item["views"])
            return out
        raise RateLimitedError(f"still rate limited after retries: {url}")
